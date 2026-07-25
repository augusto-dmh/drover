package drover

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover/internal/migrate"
)

// Migrate applies drover's embedded schema migrations to the database
// behind pool. It is idempotent: re-running it against an up-to-date
// schema is a no-op.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return migrate.Migrate(ctx, pool)
}
