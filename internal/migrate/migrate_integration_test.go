//go:build integration

package migrate_test

import (
	"context"
	"os"
	"slices"
	"testing"

	"github.com/augusto-dmh/drover/internal/migrate"
	"github.com/augusto-dmh/drover/internal/testdb"
)

func TestMain(m *testing.M) { os.Exit(testdb.RunMain(m)) }

func TestMigrateFreshDatabaseCreatesSchemaAtVersionOne(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()

	if err := migrate.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var version int
	if err := pool.QueryRow(ctx,
		`SELECT MAX(version) FROM drover_schema_version`).Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != 1 {
		t.Errorf("schema version = %d, want 1", version)
	}

	rows, err := pool.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_name = 'drover_jobs'`)
	if err != nil {
		t.Fatalf("read columns: %v", err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan column: %v", err)
		}
		columns = append(columns, name)
	}
	wantColumns := []string{
		"args", "attempt", "created_at", "errors", "finalized_at", "id",
		"kind", "leased_until", "max_attempts", "queue", "scheduled_at", "state",
	}
	slices.Sort(columns)
	if !slices.Equal(columns, wantColumns) {
		t.Errorf("drover_jobs columns = %v, want %v", columns, wantColumns)
	}

	enumRows, err := pool.Query(ctx, `
		SELECT e.enumlabel
		FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid
		WHERE t.typname = 'drover_job_state'
		ORDER BY e.enumsortorder`)
	if err != nil {
		t.Fatalf("read enum labels: %v", err)
	}
	defer enumRows.Close()
	var states []string
	for enumRows.Next() {
		var label string
		if err := enumRows.Scan(&label); err != nil {
			t.Fatalf("scan enum label: %v", err)
		}
		states = append(states, label)
	}
	wantStates := []string{
		"available", "scheduled", "running", "retryable", "completed", "cancelled", "dead",
	}
	if !slices.Equal(states, wantStates) {
		t.Errorf("drover_job_state labels = %v, want %v", states, wantStates)
	}

	columnMeta := func(column string) (dataType, colDefault string) {
		if err := pool.QueryRow(ctx, `
			SELECT data_type, COALESCE(column_default, '')
			FROM information_schema.columns
			WHERE table_name = 'drover_jobs' AND column_name = $1`, column).
			Scan(&dataType, &colDefault); err != nil {
			t.Fatalf("read %s column metadata: %v", column, err)
		}
		return dataType, colDefault
	}
	if dataType, colDefault := columnMeta("queue"); dataType != "text" || colDefault != "'default'::text" {
		t.Errorf("queue column = %s default %s, want text default 'default'::text", dataType, colDefault)
	}
	if dataType, colDefault := columnMeta("args"); dataType != "jsonb" || colDefault != "'{}'::jsonb" {
		t.Errorf("args column = %s default %s, want jsonb default '{}'::jsonb", dataType, colDefault)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()

	if err := migrate.Migrate(ctx, pool); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := migrate.Migrate(ctx, pool); err != nil {
		t.Fatalf("second Migrate returned %v, want nil", err)
	}

	var applied int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM drover_schema_version`).Scan(&applied); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if applied != 1 {
		t.Errorf("recorded migrations = %d, want 1 (no re-apply)", applied)
	}
}
