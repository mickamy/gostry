package query_test

import (
	"testing"

	"github.com/mickamy/gostry/internal/query"
)

func TestParseDML(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name    string
		sql     string
		wantDML query.DML
		wantOK  bool
	}{
		{
			name:    "insert simple",
			sql:     "INSERT INTO orders (id) VALUES ($1)",
			wantDML: query.DML{Op: "INSERT", Table: "orders", HasReturning: false},
			wantOK:  true,
		},
		{
			name: "insert with returning",
			sql: `insert into public.orders (id)
values ($1)
returning *`,
			wantDML: query.DML{Op: "INSERT", Table: "public.orders", HasReturning: true},
			wantOK:  true,
		},
		{
			name:    "update with alias",
			sql:     `UPDATE orders o SET amount = amount + 1 WHERE id = $1`,
			wantDML: query.DML{Op: "UPDATE", Table: "orders", HasReturning: false},
			wantOK:  true,
		},
		{
			name: "delete with returning and cte",
			sql: `WITH c AS (
	SELECT id FROM orders WHERE status = 'obsolete'
) DELETE FROM public.orders o USING c WHERE o.id = c.id RETURNING o.id`,
			wantDML: query.DML{Op: "DELETE", Table: "public.orders", HasReturning: true},
			wantOK:  true,
		},
		{
			name:   "unknown statement",
			sql:    "SELECT * FROM orders",
			wantOK: false,
		},
		{
			name:    "not top level returning word",
			sql:     "UPDATE orders SET note='returning soon'",
			wantDML: query.DML{Op: "UPDATE", Table: "orders", HasReturning: true},
			wantOK:  true,
		},
		{
			name:    "quoted identifier with alias",
			sql:     `UPDATE "Sales"."Orders" so SET status = $1 WHERE so.id = $2`,
			wantDML: query.DML{Op: "UPDATE", Table: `"Sales"."Orders"`, HasReturning: false},
			wantOK:  true,
		},
		{
			name:    "delete with schema without returning",
			sql:     "DELETE FROM public.orders WHERE created_at < now() - interval '1 day'",
			wantDML: query.DML{Op: "DELETE", Table: "public.orders", HasReturning: false},
			wantOK:  true,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := query.ParseDML(tc.sql)
			if ok != tc.wantOK {
				t.Fatalf("ParseDML ok = %t, want %t", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if got.Op != tc.wantDML.Op || got.Table != tc.wantDML.Table || got.HasReturning != tc.wantDML.HasReturning {
				t.Fatalf("ParseDML(%q) = %#v, want %#v", tc.sql, got, tc.wantDML)
			}
		})
	}
}

func TestAppendReturningAll(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name string
		sql  string
		want string
		ok   bool
	}{
		{
			name: "simple insert",
			sql:  "INSERT INTO orders (id) VALUES ($1)",
			want: "INSERT INTO orders (id) VALUES ($1)\nRETURNING *",
			ok:   true,
		},
		{
			name: "trim whitespace",
			sql:  "  UPDATE orders SET status='x'  ",
			want: "UPDATE orders SET status='x'\nRETURNING *",
			ok:   true,
		},
		{
			name: "keep semicolon",
			sql:  "DELETE FROM orders WHERE id=$1;",
			want: "DELETE FROM orders WHERE id=$1\nRETURNING *;",
			ok:   true,
		},
		{
			name: "empty string",
			sql:  "   ",
			want: "   ",
			ok:   false,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := query.AppendReturningAll(tc.sql)
			if ok != tc.ok {
				t.Fatalf("AppendReturningAll ok = %t, want %t", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("AppendReturningAll(%q) = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}
