package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/mickamy/gostry"
)

type Order struct{}

type OrderItem struct{}

func (OrderItem) TableName() string { return "order_items" }

func main() {
	dsn := getenv("DATABASE_URL", "postgres://root:password@localhost:5432/gostry?sslmode=disable")

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	// Migrate
	if err := gostry.Migrate(context.Background(), db, gostry.SchemaConfig{CreateIDIndex: true}, Order{}, OrderItem{}, "payments"); err != nil {
		log.Fatalf("gostry.Migrate: %v", err)
	}

	// Wrap DB with gostry
	wdb := gostry.New(gostry.Config{AutoAttachReturning: true}).Wrap(db)

	// Metadata
	ctx := gostry.WithOperator(context.Background(), "demo-user")
	ctx = gostry.WithTraceID(ctx, "trace-demo-001")
	ctx = gostry.WithReason(ctx, "demo run")

	// Start transaction
	tx, err := wdb.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("begin: %v", err)
	}

	// INSERT with RETURNING * (captured as after)
	id := uuid.NewString()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO orders(id, customer_id, amount, status)
VALUES ($1,$2,$3,$4)
`, id, uuid.NewString(), 1200.00, "new"); err != nil {
		_ = tx.Rollback()
		log.Fatalf("insert: %v", err)
	}

	// UPDATE with RETURNING * (captured as after)
	if _, err := tx.ExecContext(ctx, `
UPDATE orders SET status=$1, amount=$2, updated_at=now()
WHERE id=$3
`, "paid", 1500.00, id); err != nil {
		_ = tx.Rollback()
		log.Fatalf("update: %v", err)
	}

	// DELETE with RETURNING * (captured as before)
	if _, err := tx.ExecContext(ctx, `
DELETE FROM orders WHERE id=$1
`, id); err != nil {
		_ = tx.Rollback()
		log.Fatalf("delete: %v", err)
	}

	if err := tx.CommitContext(ctx); err != nil {
		log.Fatalf("commit: %v", err)
	}

	// Show results
	var cnt int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM orders_history`).Scan(&cnt); err != nil {
		log.Fatalf("count history: %v", err)
	}
	fmt.Printf("history rows = %d (expected >= 3)\n", cnt)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
