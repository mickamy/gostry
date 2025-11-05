package gostry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jinzhu/inflection"

	"github.com/mickamy/gostry/internal/buffer"
	"github.com/mickamy/gostry/internal/ident"
	"github.com/mickamy/gostry/internal/query"
)

// RedactFunc defines a function used to sanitize or mask values before logging.
type RedactFunc func(key string, v any) any

// RedactMap maps key names to specific redaction functions.
type RedactMap map[string]RedactFunc

// Config defines the main configuration options for gostry.
type Config struct {
	HistorySuffix   string    // e.g. "_history" (default)
	Redact          RedactMap // optional key-based redaction
	SkipIfNotExists bool      // skip insertion to history table if it does not exists
}

func (c Config) HistoryTableName(base string) string {
	parts := ident.HistoryParts(base, c.HistorySuffix)
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ".")
}

// Handler is the main entry point that manages gostry behavior.
type Handler struct {
	cfg Config
}

// New creates a new Handler instance with sensible defaults.
func New(cfg Config) *Handler {
	if cfg.HistorySuffix == "" {
		cfg.HistorySuffix = "_history"
	}
	if cfg.Redact == nil {
		cfg.Redact = RedactMap{}
	}
	return &Handler{cfg: cfg}
}

// DB wraps a *sql.DB instance to enable history tracking on transactions.
type DB struct {
	*sql.DB
	h *Handler
}

// WrapDB attaches gostry to a *sql.DB connection.
func (h *Handler) WrapDB(db *sql.DB) *DB {
	return &DB{DB: db, h: h}
}

// applyRedact returns a redacted copy of the given map using cfg.Redact.
func (h *Handler) applyRedact(m map[string]any) map[string]any {
	if m == nil || len(h.cfg.Redact) == 0 {
		return m
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if fn, ok := h.cfg.Redact[k]; ok && fn != nil {
			out[k] = fn(k, v)
		} else {
			out[k] = v
		}
	}
	return out
}

// tx wraps a *sql.Tx and buffers historical entries within the transaction.
type tx struct {
	*sql.Tx
	h   *Handler
	buf *buffer.Buffer[entry]
	ctx context.Context
}

// BeginTx starts a wrapped transaction that records DML changes.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*tx, error) {
	t, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &tx{Tx: t, h: db.h, buf: buffer.NewBuffer[entry](), ctx: ctx}, nil
}

// ExecContext intercepts ExecContext to capture and log DML operations.
// MVP behavior:
// - If the statement is INSERT/UPDATE/DELETE with RETURNING, capture row(s) as after/before.
// - Otherwise, pass-through and record only SQL/args metadata for later (future resolvers).
func (t *tx) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	if dml, ok := query.ParseDML(q); ok {
		if !dml.HasReturning {
			// No RETURNING: pass-through; record minimal info.
			res, err := t.Tx.ExecContext(ctx, q, args...)
			if err == nil {
				t.buf.Add(entry{table: dml.Table, op: dml.Op, sql: q, args: args, meta: extractMeta(ctx)})
			}
			return res, err
		}
		// Use QueryContext to fetch returned row(s); we only capture the first row for MVP.
		rows, err := t.Tx.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		ms, n, err := scanAll(rows)
		if err != nil {
			return nil, fmt.Errorf("gostry: failed to scan rows: %w", err)
		}
		me := extractMeta(ctx)
		for _, m := range ms {
			e := entry{table: dml.Table, op: dml.Op, meta: me}
			if dml.Op == "DELETE" {
				e.before = m
			} else {
				e.after = m
			}
			t.buf.Add(e)
		}
		return newAffectedRows(n), nil

	}
	// Not a recognized DML; just pass-through.
	return t.Tx.ExecContext(ctx, q, args...)
}

// Commit flushes buffered history records into history tables before commit.
func (t *tx) Commit() error {
	if err := t.flush(); err != nil {
		return err
	}
	return t.Tx.Commit()
}

// flush writes buffered entries into their corresponding history tables within the same transaction.
func (t *tx) flush() error {
	rows := t.buf.Drain()
	if len(rows) == 0 {
		return nil
	}

	for _, e := range rows {
		before := t.h.applyRedact(e.before)
		after := t.h.applyRedact(e.after)
		id := pickID(e.table, before, after)
		beforeJSON, err := json.Marshal(before)
		if err != nil {
			return fmt.Errorf("gostry: failed to marshal before: %w", err)
		}
		afterJSON, err := json.Marshal(after)
		if err != nil {
			return fmt.Errorf("gostry: failed to marshal after: %w", err)
		}

		// Simple per-row INSERT for MVP; can be batched later.
		historyParts := ident.HistoryParts(e.table, t.h.cfg.HistorySuffix)
		historyIdent := ident.QuoteQualified(historyParts)
		if historyIdent == "" {
			return fmt.Errorf("gostry: invalid history table identifier for %q", e.table)
		}
		stmt := fmt.Sprintf(`
INSERT INTO %s (id, operation, operated_at, operated_by, trace_id, reason, before, after)
VALUES ($1, $2, now(), $3, $4, $5, $6, $7)
`, historyIdent)
		if t.h.cfg.SkipIfNotExists {
			regclass := ident.QualifiedRegclassLiteral(historyParts)
			stmt = fmt.Sprintf(`
DO $$
BEGIN
    IF to_regclass(%s) IS NOT NULL THEN
        INSERT INTO %s (id, operation, operated_at, operated_by, trace_id, reason, before, after)
        VALUES ($1, $2, now(), $3, $4, $5, $6, $7);
    END IF;
END $$;
`, regclass, historyIdent)
		}

		if _, err := t.Tx.ExecContext(
			t.ctx,
			stmt,
			id,
			e.op,
			e.meta.operator,
			e.meta.traceID,
			e.meta.reason,
			beforeJSON,
			afterJSON,
		); err != nil {
			return fmt.Errorf("gostry: failed to insert history table: %w", err)
		}
	}
	return nil
}

// Rollback clears buffered history entries and rolls back the transaction.
func (t *tx) Rollback() error {
	t.buf.Reset()
	return t.Tx.Rollback()
}

// pickID attempts to choose a sensible primary key from before/after maps.
func pickID(table string, before, after map[string]any) any {
	// Heuristics: "id" first; then "<singular>_id", else nil.
	if v, ok := before["id"]; ok {
		return v
	}
	if v, ok := after["id"]; ok {
		return v
	}
	base := ident.BaseTableName(table)
	singular := inflection.Singular(base)
	singularID := fmt.Sprintf("%s_id", singular)
	if v, ok := before[singularID]; ok {
		return v
	}
	if v, ok := after[singularID]; ok {
		return v
	}
	return nil
}
