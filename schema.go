package gostry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/mickamy/gostry/internal/ident"
)

// SchemaConfig controls CreateHistoryTables behaviour.
type SchemaConfig struct {
	HistorySuffix string // suffix appended to base table name (default: _history)
	CreateIDIndex bool   // add CREATE INDEX IF NOT EXISTS ... (id)
}

// Migrate creates history tables for the provided base tables inside PostgreSQL.
func Migrate(ctx context.Context, db *sql.DB, cfg SchemaConfig, tables ...string) error {
	if cfg.HistorySuffix == "" {
		cfg.HistorySuffix = "_history"
	}
	if len(tables) == 0 {
		return nil
	}

	for _, table := range tables {
		parts := ident.SplitQualified(table)
		if len(parts) == 0 {
			return fmt.Errorf("gostry: invalid table identifier %q", table)
		}
		base, err := selectBaseTable(ctx, db, parts)
		if err != nil {
			return err
		}
		if err := createHistoryTable(ctx, db, cfg, base); err != nil {
			return err
		}
	}
	return nil
}

type tableInfo struct {
	schema string
	table  string
	ident  string
	idType string
}

func selectBaseTable(ctx context.Context, db *sql.DB, parts []string) (tableInfo, error) {
	var schemaName, tableName string
	switch len(parts) {
	case 1:
		schemaName = "public"
		tableName = parts[0]
	case 2:
		schemaName = parts[0]
		tableName = parts[1]
	default:
		return tableInfo{}, fmt.Errorf("gostry: unsupported identifier %q", strings.Join(parts, "."))
	}

	row := db.QueryRowContext(ctx, `
		SELECT
			n.nspname,
			r.relname,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS id_type
		FROM pg_class r
		JOIN pg_namespace n ON n.oid = r.relnamespace
		LEFT JOIN (
			SELECT attrelid, atttypid, atttypmod
			FROM pg_attribute
			WHERE attname = 'id'
			  AND attnum > 0
			  AND NOT attisdropped
		) AS a ON a.attrelid = r.oid
		WHERE n.nspname = $1 AND r.relname = $2
	`, schemaName, tableName)

	var info tableInfo
	var idType sql.NullString
	if err := row.Scan(&info.schema, &info.table, &idType); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return tableInfo{}, fmt.Errorf("gostry: table %s.%s not found", schemaName, tableName)
		}
		return tableInfo{}, err
	}
	info.ident = ident.QuoteQualified([]string{info.schema, info.table})
	if idType.Valid {
		info.idType = idType.String
	}
	return info, nil
}

func createHistoryTable(ctx context.Context, db *sql.DB, cfg SchemaConfig, base tableInfo) error {
	historyParts := ident.HistoryParts(base.ident, cfg.HistorySuffix)
	historyIdent := ident.QuoteQualified(historyParts)
	if historyIdent == "" {
		return fmt.Errorf("gostry: invalid history identifier for %s", base.ident)
	}
	columns := []string{
		"history_id BIGSERIAL PRIMARY KEY",
	}
	if base.idType != "" {
		columns = append(columns, fmt.Sprintf("id %s", base.idType))
	} else {
		columns = append(columns, "id UUID")
	}
	columns = append(columns,
		"operation TEXT NOT NULL",
		"operated_at TIMESTAMPTZ NOT NULL",
		"operated_by TEXT",
		"trace_id TEXT",
		"reason TEXT",
		"before JSONB",
		"after JSONB",
	)

	ddl := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		%s
	);
	`, historyIdent, strings.Join(columns, ",\n\t"))
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return err
	}
	if cfg.CreateIDIndex {
		indexName := ident.Quote(historyParts[len(historyParts)-1] + "_id_idx")
		stmt := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (id);`, indexName, historyIdent)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
