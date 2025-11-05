package gostry

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// affectedResult implements sql.Result for Exec-like semantics.
type affectedResult struct{ n int64 }

func newAffectedRows(n int) sql.Result {
	return affectedResult{n: int64(n)}
}

func (r affectedResult) LastInsertId() (int64, error) {
	return 0, errors.New("not supported")
}

func (r affectedResult) RowsAffected() (int64, error) {
	return r.n, nil
}

// scanAll consumes all rows from *sql.Rows and returns them as slice of maps.
func scanAll(rows *sql.Rows) ([]map[string]any, int, error) {
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	cols, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, 0, err
		}
		out = append(out, rowToMap(cols, vals))
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if len(out) == 0 {
		return nil, 0, sql.ErrNoRows
	}
	return out, len(out), nil
}

// rowToMap converts a single row (columns + values) to a map.
func rowToMap(cols []string, vals []any) map[string]any {
	m := make(map[string]any, len(cols))
	for i, c := range cols {
		v := vals[i]
		if b, ok := v.([]byte); ok {
			// Try JSON decoding; if it fails, keep as string
			var js any
			if json.Unmarshal(b, &js) == nil {
				m[c] = js
				continue
			}
			m[c] = string(b)
			continue
		}
		m[c] = v
	}
	return m
}
