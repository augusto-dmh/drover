//go:build integration

package migrate_test

import (
	"context"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover/internal/migrate"
	"github.com/augusto-dmh/drover/internal/testdb"
)

func TestMain(m *testing.M) { os.Exit(testdb.RunMain(m)) }

// latestVersion is the highest embedded migration; the schema is only
// correct once every one of them has been applied.
const latestVersion = 2

// indexDefs returns each index on drover_jobs by name, with the
// definition PostgreSQL reconstructed from the catalog.
func indexDefs(t *testing.T, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'drover_jobs'`)
	if err != nil {
		t.Fatalf("read indexes: %v", err)
	}
	defer rows.Close()
	defs := make(map[string]string)
	for rows.Next() {
		var name, def string
		if err := rows.Scan(&name, &def); err != nil {
			t.Fatalf("scan index: %v", err)
		}
		defs[name] = def
	}
	return defs
}

// assertFetchAndLeaseIndexes checks that the fetch index covers all
// three states a job can wait in, and that the rescuer's sweep has an
// index on the lease of running jobs.
func assertFetchAndLeaseIndexes(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	defs := indexDefs(t, pool)

	fetch, ok := defs["drover_jobs_fetch_idx"]
	if !ok {
		t.Fatalf("drover_jobs_fetch_idx missing; indexes present: %v", slices.Sorted(maps.Keys(defs)))
	}
	for _, state := range []string{"available", "retryable", "scheduled"} {
		if !strings.Contains(fetch, state) {
			t.Errorf("fetch index does not cover waiting state %q: %s", state, fetch)
		}
	}
	if strings.Contains(fetch, "running") {
		t.Errorf("fetch index covers running jobs, which are never claimable: %s", fetch)
	}

	lease, ok := defs["drover_jobs_lease_idx"]
	if !ok {
		t.Fatalf("drover_jobs_lease_idx missing; indexes present: %v", slices.Sorted(maps.Keys(defs)))
	}
	if !strings.Contains(lease, "leased_until") || !strings.Contains(lease, "running") {
		t.Errorf("lease index does not serve a sweep of expired running leases: %s", lease)
	}
}

func TestMigrateFreshDatabaseCreatesSchemaAtLatestVersion(t *testing.T) {
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
	if version != latestVersion {
		t.Errorf("schema version = %d, want %d", version, latestVersion)
	}

	assertFetchAndLeaseIndexes(t, pool)

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
	if applied != latestVersion {
		t.Errorf("recorded migrations = %d, want %d (no re-apply)", applied, latestVersion)
	}

	assertFetchAndLeaseIndexes(t, pool)
}
