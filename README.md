# gostry

`gostry` is a lightweight auditing helper for Go applications that need to persist change history for PostgreSQL tables.
It wraps `database/sql` transactions, captures row images for data-changing statements, and writes them into
configurable history tables inside the same transaction.

## Highlights

- Safe wrapper around `*sql.Tx` that buffers DML metadata and flushes to history tables on commit.
- Row snapshots for `INSERT`, `UPDATE`, and `DELETE` when `RETURNING` clauses are present, with optional auto-attachment
  of `RETURNING *`.
- Pluggable redaction hooks to mask sensitive columns before they are persisted.
- Schema-aware identifier handling, including quoted names and custom schema prefixes.

## Requirements

- Go 1.21 or later
- PostgreSQL (tested with PG-compatible drivers such as `pgx`)

## Installation

```bash
go get github.com/mickamy/gostry
```

## Usage

1. Construct a `gostry.Handler` with your preferred configuration.
2. Wrap your `*sql.DB` connection.
3. Use the returned `*gostry.Tx` instead of the standard `*sql.Tx`.

```go
package main

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/mickamy/gostry"
)

func main() {
	db, _ := sql.Open("pgx", "postgres://user:pass@localhost:5432/db?sslmode=disable")
	defer db.Close()

	handler := gostry.New(gostry.Config{
		HistorySuffix:       "_history",
		SkipIfNotExists:     true,
		AutoAttachReturning: true,
	})

	wrapped := handler.Wrap(db)
	ctx := gostry.WithOperator(context.Background(), "cli-user")

	tx, _ := wrapped.BeginTx(ctx, nil)
	defer tx.Rollback(ctx) // safe fallback

	if _, err := tx.ExecContext(ctx, `UPDATE orders SET status='paid' WHERE id=$1`, "order-id"); err != nil {
		return
	}

	_ = tx.CommitContext(ctx)
}
```

### Configuration options

| Field                 | Default    | Description                                                                                                                                             |
|-----------------------|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `HistorySuffix`       | `_history` | Suffix appended to base table names when deriving history tables (schema is preserved).                                                                 |
| `Redact`              | `nil`      | Optional map of column name → redaction function executed before values are stored.                                                                     |
| `SkipIfNotExists`     | `false`    | Guards history inserts with `to_regclass(...)` so missing history tables do not fail the transaction.                                                   |
| `AutoAttachReturning` | `false`    | When enabled, attempts to append `RETURNING *` to matching DML that lack it, allowing row snapshots without changing application SQL (PostgreSQL only). |

### Metadata helpers

`gostry.WithOperator`, `gostry.WithTraceID`, and `gostry.WithReason` attach contextual metadata to a `context.Context`.
These fields are propagated into history rows for auditing.

### History table schema

`gostry` expects a companion table per audited table. A minimal example:

```sql
CREATE TABLE orders_history
(
    history_id  SERIAL PRIMARY KEY,
    id          UUID,
    operation   TEXT        NOT NULL,
    operated_at TIMESTAMPTZ NOT NULL,
    operated_by TEXT,
    trace_id    TEXT,
    reason      TEXT,
    before      JSONB,
    after       JSONB
);
```

## Example project

`example/cmd/demo` contains a runnable sample that spins through `INSERT`, `UPDATE`, and `DELETE` statements against
PostgreSQL and prints the number of history rows created. It pairs with the SQL in `db/` and a `docker compose`
definition for local experimentation.

## Limitations

- Automatic `RETURNING *` is best-effort. If the rewritten statement fails (e.g., non-PostgreSQL backend), `gostry`
  falls back to the original SQL and only records metadata.
- Multi-row `RETURNING` statements record each row individually but still execute sequential inserts into the history
  table.
- Only top-level DML statements are recognized; stored procedures and complex batch statements are not yet supported.

## License

[MIT](./LICENSE)
