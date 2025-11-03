package gostry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/jinzhu/inflection"

	"github.com/mickamy/gostry/internal/buffer"
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
	return base + c.HistorySuffix
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

// Tx wraps a *sql.Tx and buffers historical entries within the transaction.
type Tx struct {
	*sql.Tx
	h   *Handler
	buf *buffer.Buffer[entry]
	ctx context.Context
}

// BeginTx starts a wrapped transaction that records DML changes.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, h: db.h, buf: buffer.NewBuffer[entry](), ctx: ctx}, nil
}

// ExecContext intercepts ExecContext to capture and log DML operations.
// MVP behavior:
// - If the statement is INSERT/UPDATE/DELETE with RETURNING, capture row(s) as after/before.
// - Otherwise, pass-through and record only SQL/args metadata for later (future resolvers).
func (t *Tx) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
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
		m, n, err := scanOne(rows)
		if err != nil {
			return nil, fmt.Errorf("gostry: failed to scan rows: %w", err)
		}

		e := entry{table: dml.Table, op: dml.Op, meta: extractMeta(ctx)}
		if dml.Op == "DELETE" {
			e.before = m
		} else {
			e.after = m
		}
		t.buf.Add(e)
		return newAffectedRows(n), nil

	}
	// Not a recognized DML; just pass-through.
	return t.Tx.ExecContext(ctx, q, args...)
}

// Commit flushes buffered history records into history tables before commit.
func (t *Tx) Commit() error {
	if err := t.flush(); err != nil {
		return err
	}
	return t.Tx.Commit()
}

// flush writes buffered entries into their corresponding history tables within the same transaction.
func (t *Tx) flush() error {
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
		table := t.h.cfg.HistoryTableName(e.table)
		stmt := fmt.Sprintf(`
INSERT INTO %s (id, operation, operated_at, operated_by, trace_id, reason, before, after)
VALUES ($1, $2, now(), $3, $4, $5, $6, $7)
`, t.h.cfg.HistoryTableName(e.table))
		if t.h.cfg.SkipIfNotExists {
			stmt = fmt.Sprintf(`
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '%s') THEN
        INSERT INTO %s (id, operation, operated_at, operated_by, trace_id, reason, before, after)
        VALUES ($1, $2, now(), $3, $4, $5, $6, $7);
    END IF;
END $$;
`, table, table)
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
func (t *Tx) Rollback() error {
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
	singular := inflection.Singular(table)
	singularID := fmt.Sprintf("%s_id", singular)
	if v, ok := before[singularID]; ok {
		return v
	}
	if v, ok := after[singularID]; ok {
		return v
	}
	return nil
}
