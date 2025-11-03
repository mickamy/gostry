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

// scanOne consumes exactly one row from *sql.Rows into a map.
func scanOne(rows *sql.Rows) (map[string]any, int, error) {
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	cols, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}
	if !rows.Next() {
		return nil, 0, sql.ErrNoRows
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, 0, err
	}
	return rowToMap(cols, vals), 1, nil
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
