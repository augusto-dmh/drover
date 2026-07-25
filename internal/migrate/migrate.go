// Package migrate applies drover's embedded schema migrations.
// Each migration runs in its own transaction and is recorded in
// drover_schema_version, making Migrate idempotent (AD-001).
package migrate

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

type migration struct {
	version int
	name    string
	sql     string
}

// Migrate brings the database schema up to the latest embedded
// migration version.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS drover_schema_version (
			version    int PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`)
	if err != nil {
		return fmt.Errorf("create version table: %w", err)
	}

	var current int
	err = pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM drover_schema_version`).Scan(&current)
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	all, err := load()
	if err != nil {
		return err
	}
	for _, m := range all {
		if m.version <= current {
			continue
		}
		if err := apply(ctx, pool, m); err != nil {
			return err
		}
	}
	return nil
}

func apply(ctx context.Context, pool *pgxpool.Pool, m migration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %03d: %w", m.version, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	if _, err := tx.Exec(ctx, m.sql); err != nil {
		return fmt.Errorf("apply migration %03d %s: %w", m.version, m.name, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO drover_schema_version (version) VALUES ($1)`, m.version); err != nil {
		return fmt.Errorf("record migration %03d: %w", m.version, err)
	}
	return tx.Commit(ctx)
}

func load() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	var all []migration
	for _, entry := range entries {
		name := entry.Name()
		prefix, _, ok := strings.Cut(name, "_")
		if !ok {
			return nil, fmt.Errorf("migration %q: name must be NNN_description.sql", name)
		}
		version, err := strconv.Atoi(prefix)
		if err != nil {
			return nil, fmt.Errorf("migration %q: invalid version prefix: %w", name, err)
		}
		sql, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", name, err)
		}
		all = append(all, migration{version: version, name: name, sql: string(sql)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].version < all[j].version })
	return all, nil
}
