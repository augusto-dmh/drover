package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover"
)

// openInspector resolves the DSN, opens a pool, and returns an Inspector.
// The caller must Close the pool. Schema migration is never run here
// (operators migrate out of band).
func openInspector(ctx context.Context, dsn string) (*drover.Inspector, *pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return drover.NewInspector(pool), pool, nil
}
