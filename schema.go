package gostry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/jinzhu/inflection"

	"github.com/mickamy/gostry/internal/ident"
)

// SchemaConfig controls history table generation behaviour.
type SchemaConfig struct {
	HistorySuffix string // suffix appended to base table name (default: _history)
	CreateIDIndex bool   // create an index on the history table id column
}

// TableNamer provides a custom table name for a model.
type TableNamer interface {
	TableName() string
}

// Migrate resolves table identifiers from the provided targets and creates history tables.
func Migrate(ctx context.Context, db *sql.DB, cfg SchemaConfig, targets ...any) error {
	if cfg.HistorySuffix == "" {
		cfg.HistorySuffix = "_history"
	}
	if len(targets) == 0 {
		return nil
	}
	names := make([]string, 0, len(targets))
	for _, t := range targets {
		name, err := resolveTableName(t)
		if err != nil {
			return err
		}
		names = append(names, name)
	}

	for _, name := range names {
		parts := ident.SplitQualified(name)
		if len(parts) == 0 {
			return fmt.Errorf("gostry: invalid table identifier %q", name)
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
		indexName := fmt.Sprintf("idx_%s_id", historyParts[len(historyParts)-1])
		stmt := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (id);`, ident.Quote(indexName), historyIdent)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

var tableNamerType = reflect.TypeOf((*TableNamer)(nil)).Elem()

func resolveTableName(target any) (string, error) {
	switch v := target.(type) {
	case nil:
		return "", errors.New("gostry: nil table target")
	case string:
		name := strings.TrimSpace(v)
		if name == "" {
			return "", errors.New("gostry: empty table name")
		}
		return name, nil
	}

	val := reflect.ValueOf(target)
	typ := val.Type()

	if typ.Kind() == reflect.Pointer {
		if val.IsNil() {
			return "", fmt.Errorf("gostry: nil pointer target %T", target)
		}
		if namer, ok := val.Interface().(TableNamer); ok {
			name := strings.TrimSpace(namer.TableName())
			if name == "" {
				return "", fmt.Errorf("gostry: TableName returned empty string. %T", target)
			}
			return name, nil
		}
		typ = typ.Elem()
		val = val.Elem()
	}

	if namer, ok := val.Interface().(TableNamer); ok {
		name := strings.TrimSpace(namer.TableName())
		if name == "" {
			return "", fmt.Errorf("gostry: TableName returned empty string. %T", target)
		}
		return name, nil
	}

	if typ.Kind() == reflect.Struct {
		if reflect.PointerTo(typ).Implements(tableNamerType) {
			inst := reflect.New(typ)
			if namer, ok := inst.Interface().(TableNamer); ok {
				name := strings.TrimSpace(namer.TableName())
				if name == "" {
					return "", fmt.Errorf("gostry: TableName returned empty string. %T", target)
				}
				return name, nil
			}
		}
		if typ.Name() == "" {
			return "", fmt.Errorf("gostry: cannot derive table name for anonymous struct of type %v", typ)
		}
		return inflection.Plural(toSnakeCase(typ.Name())), nil
	}

	return "", fmt.Errorf("gostry: unsupported table target %T", target)
}

func toSnakeCase(s string) string {
	runes := []rune(s)
	var b strings.Builder
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 && (unicode.IsLower(runes[i-1]) || (i+1 < len(runes) && unicode.IsLower(runes[i+1]))) {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
