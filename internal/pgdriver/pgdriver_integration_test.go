//go:build integration

package pgdriver_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/memdriver"
	"github.com/augusto-dmh/drover/internal/pgdriver"
	"github.com/augusto-dmh/drover/internal/testdb"
)

// held builds the lease a caller holds after claiming a job once. Every
// test here claims at most once, so attempt 1 is what a legitimate
// holder presents.
func held(id int64) driver.Lease { return driver.Lease{ID: id, Attempt: 1} }

func TestMain(m *testing.M) { os.Exit(testdb.RunMain(m)) }

func newDriver(t *testing.T) (*pgdriver.Driver, *pgxpool.Pool) {
	t.Helper()
	pool := testdb.NewDB(t)
	d := pgdriver.New(pool)
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return d, pool
}

func mustInsert(t *testing.T, d *pgdriver.Driver, kind, queue string) *driver.JobRow {
	t.Helper()
	row, err := d.Insert(context.Background(), driver.InsertParams{
		Kind:  kind,
		Queue: queue,
		Args:  []byte(`{"n":1}`),
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	return row
}

// storedJob is the persisted state of a job, read straight from the
// table so assertions never depend on what the driver returned.
type storedJob struct {
	State       string
	Attempt     int
	ScheduledAt time.Time
	LeasedUntil *time.Time
	FinalizedAt *time.Time
	Errors      []byte
}

func readJob(t *testing.T, pool *pgxpool.Pool, id int64) storedJob {
	t.Helper()
	var j storedJob
	if err := pool.QueryRow(context.Background(), `
		SELECT state, attempt, scheduled_at, leased_until, finalized_at, errors
		FROM drover_jobs WHERE id = $1`, id).
		Scan(&j.State, &j.Attempt, &j.ScheduledAt, &j.LeasedUntil, &j.FinalizedAt, &j.Errors); err != nil {
		t.Fatalf("read job %d: %v", id, err)
	}
	return j
}

func decodeErrors(t *testing.T, raw []byte) []driver.AttemptError {
	t.Helper()
	var recorded []driver.AttemptError
	if err := json.Unmarshal(raw, &recorded); err != nil {
		t.Fatalf("decode errors %s: %v", raw, err)
	}
	return recorded
}

func claimOne(t *testing.T, d *pgdriver.Driver) *driver.JobRow {
	t.Helper()
	rows, err := d.FetchAvailable(context.Background(), "default", time.Minute, 1)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("FetchAvailable claimed %d jobs, want 1", len(rows))
	}
	return rows[0]
}

// park drives a fresh job into a waiting state whose scheduled_at is at,
// using the driver's own transitions wherever one exists.
func park(t *testing.T, d *pgdriver.Driver, pool *pgxpool.Pool, state string, at time.Time) int64 {
	t.Helper()
	ctx := context.Background()
	row := mustInsert(t, d, "k", "default")
	switch state {
	case "available":
		if _, err := pool.Exec(ctx,
			`UPDATE drover_jobs SET scheduled_at = $2 WHERE id = $1`, row.ID, at); err != nil {
			t.Fatalf("set scheduled_at: %v", err)
		}
	case "retryable":
		claimOne(t, d)
		if err := d.MarkRetryable(ctx, held(row.ID), at, []byte(`{"error":"boom"}`)); err != nil {
			t.Fatalf("MarkRetryable: %v", err)
		}
	case "scheduled":
		claimOne(t, d)
		if err := d.MarkSnoozed(ctx, held(row.ID), at); err != nil {
			t.Fatalf("MarkSnoozed: %v", err)
		}
	default:
		t.Fatalf("park: unknown waiting state %q", state)
	}
	return row.ID
}

// expire forces a claimed job's lease into the past, standing in for the
// worker that died still holding it.
func expire(t *testing.T, pool *pgxpool.Pool, ids ...int64) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE drover_jobs SET leased_until = now() - interval '1 minute' WHERE id = ANY($1)`,
		ids); err != nil {
		t.Fatalf("expire leases: %v", err)
	}
}

func TestTransitionsRefuseAnAttemptTheCallerNoLongerHolds(t *testing.T) {
	transitions := []struct {
		name string
		call func(*pgdriver.Driver, driver.Lease) error
	}{
		{"completed", func(d *pgdriver.Driver, l driver.Lease) error {
			return d.MarkCompleted(context.Background(), l)
		}},
		{"dead", func(d *pgdriver.Driver, l driver.Lease) error {
			return d.MarkDead(context.Background(), l, []byte(`{}`))
		}},
		{"cancelled", func(d *pgdriver.Driver, l driver.Lease) error {
			return d.MarkCancelled(context.Background(), l, []byte(`{}`))
		}},
		{"retryable", func(d *pgdriver.Driver, l driver.Lease) error {
			return d.MarkRetryable(context.Background(), l, time.Now(), []byte(`{}`))
		}},
		{"snoozed", func(d *pgdriver.Driver, l driver.Lease) error {
			return d.MarkSnoozed(context.Background(), l, time.Now())
		}},
	}

	for _, tt := range transitions {
		t.Run(tt.name, func(t *testing.T) {
			d, pool := newDriver(t)
			ctx := context.Background()
			mustInsert(t, d, "k", "default")

			// A worker claims the job and then stalls past its lease. The
			// sweep returns the row to the queue and a second worker
			// claims it, so the row is running again on a later attempt
			// while the first worker is still executing.
			stale := claimOne(t, d)
			expire(t, pool, stale.ID)
			if _, err := d.FetchExpired(ctx, time.Minute, 1); err != nil {
				t.Fatalf("FetchExpired: %v", err)
			}
			if err := d.MarkRetryable(ctx, held(stale.ID), time.Now(), []byte(`{}`)); err != nil {
				t.Fatalf("rescue to retryable: %v", err)
			}
			current := claimOne(t, d)
			if current.Attempt != 2 {
				t.Fatalf("second claim is attempt %d, want 2", current.Attempt)
			}

			// A state-only guard would accept this, because the row is
			// running — just not on this caller's attempt.
			err := tt.call(d, driver.Lease{ID: stale.ID, Attempt: stale.Attempt})
			if !errors.Is(err, driver.ErrLeaseLost) {
				t.Fatalf("stale write returned %v, want ErrLeaseLost", err)
			}

			var state string
			var attempt int
			if err := pool.QueryRow(ctx,
				`SELECT state, attempt FROM drover_jobs WHERE id = $1`, stale.ID).
				Scan(&state, &attempt); err != nil {
				t.Fatalf("read job: %v", err)
			}
			if state != "running" || attempt != 2 {
				t.Errorf("job is %s on attempt %d, want running on attempt 2 — the live "+
					"attempt was disturbed by a worker that no longer owns it", state, attempt)
			}

			if err := d.MarkCompleted(ctx, driver.Lease{ID: current.ID, Attempt: current.Attempt}); err != nil {
				t.Errorf("current holder could not finalize: %v", err)
			}
		})
	}
}

func TestLeaseDeadlinesComeFromTheDatabaseClock(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	mustInsert(t, d, "k", "default")

	// The deadline must be derived from the database's own clock, not the
	// caller's: across a fleet the two disagree, and a client running
	// behind would otherwise write leases the sweep already considers
	// expired, duplicating jobs that started milliseconds ago.
	claimed := claimOne(t, d)

	var skew time.Duration
	if err := pool.QueryRow(ctx,
		`SELECT leased_until - (now() + interval '1 minute') FROM drover_jobs WHERE id = $1`,
		claimed.ID).Scan(&skew); err != nil {
		t.Fatalf("compare lease against database clock: %v", err)
	}
	if skew < -time.Second || skew > time.Second {
		t.Errorf("lease sits %v from one minute past the database clock, want it measured by that clock", skew)
	}
}

func TestInsertPersistsAvailableJob(t *testing.T) {
	d, _ := newDriver(t)

	row := mustInsert(t, d, "send_email", "default")

	if row.State != "available" {
		t.Errorf("State = %q, want available", row.State)
	}
	if row.Attempt != 0 || row.MaxAttempts != 25 {
		t.Errorf("Attempt/MaxAttempts = %d/%d, want 0/25", row.Attempt, row.MaxAttempts)
	}
	if string(row.Errors) != "[]" {
		t.Errorf("Errors = %s, want []", row.Errors)
	}
	if string(row.Args) != `{"n": 1}` && string(row.Args) != `{"n":1}` {
		t.Errorf("Args = %s, want {\"n\":1}", row.Args)
	}
	if row.LeasedUntil != nil || row.FinalizedAt != nil {
		t.Error("LeasedUntil/FinalizedAt should be unset on insert")
	}
}

func TestInsertTxVisibilityFollowsTransaction(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()

	countJobs := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM drover_jobs`).Scan(&n); err != nil {
			t.Fatalf("count jobs: %v", err)
		}
		return n
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := d.InsertTx(ctx, tx, driver.InsertParams{Kind: "k", Queue: "default", Args: []byte(`{}`)}); err != nil {
		t.Fatalf("InsertTx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if n := countJobs(); n != 0 {
		t.Fatalf("after rollback: %d jobs, want 0", n)
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := d.InsertTx(ctx, tx, driver.InsertParams{Kind: "k", Queue: "default", Args: []byte(`{}`)}); err != nil {
		t.Fatalf("InsertTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := countJobs(); n != 1 {
		t.Fatalf("after commit: %d jobs, want 1", n)
	}
}

func TestInsertTxRejectsNonPgxTransaction(t *testing.T) {
	d, _ := newDriver(t)

	_, err := d.InsertTx(context.Background(), "not a tx", driver.InsertParams{Kind: "k", Queue: "default"})
	if !errors.Is(err, driver.ErrTxUnsupported) {
		t.Fatalf("error = %v, want ErrTxUnsupported", err)
	}
}

func TestFetchAvailableClaimSemantics(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	first := mustInsert(t, d, "a", "default")
	second := mustInsert(t, d, "b", "default")
	mustInsert(t, d, "c", "other")
	future := mustInsert(t, d, "d", "default")
	if _, err := pool.Exec(ctx,
		`UPDATE drover_jobs SET scheduled_at = now() + interval '1 hour' WHERE id = $1`,
		future.ID); err != nil {
		t.Fatalf("push job to the future: %v", err)
	}

	claimed, err := d.FetchAvailable(ctx, "default", time.Minute, 1)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != first.ID {
		t.Fatalf("first claim = %+v, want job %d", claimed, first.ID)
	}
	if claimed[0].State != "running" || claimed[0].Attempt != 1 {
		t.Errorf("claimed State/Attempt = %s/%d, want running/1", claimed[0].State, claimed[0].Attempt)
	}
	if claimed[0].LeasedUntil == nil || !claimed[0].LeasedUntil.After(time.Now()) {
		t.Errorf("LeasedUntil = %v, want a future lease", claimed[0].LeasedUntil)
	}

	claimed, err = d.FetchAvailable(ctx, "default", time.Minute, 10)
	if err != nil {
		t.Fatalf("second FetchAvailable: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != second.ID {
		t.Fatalf("second claim = %d jobs, want only job %d (other queue and future job excluded)",
			len(claimed), second.ID)
	}

	claimed, err = d.FetchAvailable(ctx, "default", time.Minute, 1)
	if err != nil {
		t.Fatalf("third FetchAvailable: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("third claim = %d jobs, want 0", len(claimed))
	}
}

func TestFetchAvailableReturnsBatchInIDOrder(t *testing.T) {
	d, _ := newDriver(t)
	var want []int64
	for range 3 {
		want = append(want, mustInsert(t, d, "k", "default").ID)
	}
	mustInsert(t, d, "k", "default")

	claimed, err := d.FetchAvailable(context.Background(), "default", time.Minute, 3)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d jobs, want 3", len(claimed))
	}
	for i, row := range claimed {
		if row.ID != want[i] {
			t.Fatalf("claimed[%d].ID = %d, want %d (FIFO)", i, row.ID, want[i])
		}
	}
}

func TestConcurrentClaimersNeverDoubleClaim(t *testing.T) {
	d, _ := newDriver(t)
	const jobs = 25
	for range jobs {
		mustInsert(t, d, "k", "default")
	}

	var mu sync.Mutex
	seen := make(map[int64]int)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				rows, err := d.FetchAvailable(context.Background(), "default", time.Minute, 1)
				if err != nil {
					t.Errorf("FetchAvailable: %v", err)
					return
				}
				if len(rows) == 0 {
					return
				}
				mu.Lock()
				seen[rows[0].ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d claimed %d times, want exactly once", id, n)
		}
	}
}

func TestMarkCompletedFinalizesRunningJob(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	mustInsert(t, d, "k", "default")
	claimed, err := d.FetchAvailable(ctx, "default", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v (%d rows)", err, len(claimed))
	}

	if err := d.MarkCompleted(ctx, held(claimed[0].ID)); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}

	var state string
	var finalized bool
	if err := pool.QueryRow(ctx,
		`SELECT state, finalized_at IS NOT NULL FROM drover_jobs WHERE id = $1`,
		claimed[0].ID).Scan(&state, &finalized); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if state != "completed" || !finalized {
		t.Errorf("state/finalized = %s/%t, want completed/true", state, finalized)
	}
}

func TestMarkDeadAppendsErrorDetail(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	mustInsert(t, d, "k", "default")
	claimed, err := d.FetchAvailable(ctx, "default", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v (%d rows)", err, len(claimed))
	}

	detail, _ := json.Marshal(driver.AttemptError{Attempt: 1, At: time.Now().UTC(), Error: "boom"})
	if err := d.MarkDead(ctx, held(claimed[0].ID), detail); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	var state string
	var rawErrors []byte
	var finalized bool
	if err := pool.QueryRow(ctx,
		`SELECT state, errors, finalized_at IS NOT NULL FROM drover_jobs WHERE id = $1`,
		claimed[0].ID).Scan(&state, &rawErrors, &finalized); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if state != "dead" || !finalized {
		t.Errorf("state/finalized = %s/%t, want dead/true", state, finalized)
	}
	var recorded []driver.AttemptError
	if err := json.Unmarshal(rawErrors, &recorded); err != nil {
		t.Fatalf("decode errors: %v", err)
	}
	if len(recorded) != 1 || recorded[0].Error != "boom" || recorded[0].Attempt != 1 {
		t.Errorf("errors = %+v, want one entry {Attempt:1 Error:boom}", recorded)
	}
}

func TestFinalizeTransitionGuards(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	available := mustInsert(t, d, "k", "default")
	now := time.Now()

	if err := d.MarkCompleted(ctx, held(available.ID)); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkCompleted on available job: %v, want ErrInvalidTransition", err)
	}
	if err := d.MarkDead(ctx, held(available.ID), []byte(`{}`)); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkDead on available job: %v, want ErrInvalidTransition", err)
	}
	if err := d.MarkRetryable(ctx, held(available.ID), now, []byte(`{}`)); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkRetryable on available job: %v, want ErrInvalidTransition", err)
	}
	if err := d.MarkCancelled(ctx, held(available.ID), []byte(`{}`)); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkCancelled on available job: %v, want ErrInvalidTransition", err)
	}
	if err := d.MarkSnoozed(ctx, held(available.ID), now); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkSnoozed on available job: %v, want ErrInvalidTransition", err)
	}

	if err := d.MarkCompleted(ctx, held(99999)); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("MarkCompleted on unknown id: %v, want ErrNotFound", err)
	}
	if err := d.MarkDead(ctx, held(99999), []byte(`{}`)); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("MarkDead on unknown id: %v, want ErrNotFound", err)
	}
	if err := d.MarkRetryable(ctx, held(99999), now, []byte(`{}`)); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("MarkRetryable on unknown id: %v, want ErrNotFound", err)
	}
	if err := d.MarkCancelled(ctx, held(99999), []byte(`{}`)); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("MarkCancelled on unknown id: %v, want ErrNotFound", err)
	}
	if err := d.MarkSnoozed(ctx, held(99999), now); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("MarkSnoozed on unknown id: %v, want ErrNotFound", err)
	}
}

// TestWaitingTransitionGuards covers the states reachable only through
// this cycle's transitions: a job waiting to retry or snoozed is not
// running, so no transition may fire from there.
func TestWaitingTransitionGuards(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	retryable := park(t, d, pool, "retryable", time.Now().Add(time.Hour))
	scheduled := park(t, d, pool, "scheduled", time.Now().Add(time.Hour))

	if err := d.MarkRetryable(ctx, held(retryable), time.Now(), []byte(`{}`)); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkRetryable on a retryable job: %v, want ErrInvalidTransition", err)
	}
	if err := d.MarkCompleted(ctx, held(retryable)); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkCompleted on a retryable job: %v, want ErrInvalidTransition", err)
	}
	if err := d.MarkSnoozed(ctx, held(scheduled), time.Now()); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkSnoozed on a scheduled job: %v, want ErrInvalidTransition", err)
	}
	if err := d.MarkCancelled(ctx, held(scheduled), []byte(`{}`)); !errors.Is(err, driver.ErrInvalidTransition) {
		t.Errorf("MarkCancelled on a scheduled job: %v, want ErrInvalidTransition", err)
	}
}

func TestFetchAvailableClaimsEveryDueWaitingState(t *testing.T) {
	tests := []struct {
		name        string
		state       string
		offset      time.Duration
		wantClaimed bool
	}{
		{"available and due", "available", -time.Second, true},
		{"retryable and due", "retryable", -time.Second, true},
		{"scheduled and due", "scheduled", -time.Second, true},
		{"available but not yet due", "available", time.Hour, false},
		{"retryable but not yet due", "retryable", time.Hour, false},
		{"scheduled but not yet due", "scheduled", time.Hour, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, pool := newDriver(t)
			id := park(t, d, pool, tt.state, time.Now().Add(tt.offset))
			parked := readJob(t, pool, id)

			claimed, err := d.FetchAvailable(context.Background(), "default", time.Minute, 1)
			if err != nil {
				t.Fatalf("FetchAvailable: %v", err)
			}

			if !tt.wantClaimed {
				if len(claimed) != 0 {
					t.Fatalf("claimed %d jobs from state %s due in %v, want 0",
						len(claimed), tt.state, tt.offset)
				}
				return
			}
			if len(claimed) != 1 || claimed[0].ID != id {
				t.Fatalf("claimed %d jobs, want job %d in state %s", len(claimed), id, tt.state)
			}
			if claimed[0].State != "running" {
				t.Errorf("State = %q, want running", claimed[0].State)
			}
			if claimed[0].Attempt != parked.Attempt+1 {
				t.Errorf("Attempt = %d, want %d (one more than the parked %d)",
					claimed[0].Attempt, parked.Attempt+1, parked.Attempt)
			}
			if claimed[0].LeasedUntil == nil || !claimed[0].LeasedUntil.After(time.Now()) {
				t.Errorf("LeasedUntil = %v, want a future lease", claimed[0].LeasedUntil)
			}
		})
	}
}

func TestMarkRetryableSchedulesRetryWithoutFinalizing(t *testing.T) {
	d, pool := newDriver(t)
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d)
	retryAt := time.Now().Add(16 * time.Second).Truncate(time.Microsecond)

	detail, _ := json.Marshal(driver.AttemptError{Attempt: 1, At: time.Now().UTC(), Error: "boom"})
	if err := d.MarkRetryable(context.Background(), held(claimed.ID), retryAt, detail); err != nil {
		t.Fatalf("MarkRetryable: %v", err)
	}

	row := readJob(t, pool, claimed.ID)
	if row.State != "retryable" {
		t.Errorf("state = %q, want retryable", row.State)
	}
	if !row.ScheduledAt.Equal(retryAt) {
		t.Errorf("scheduled_at = %v, want the policy's %v", row.ScheduledAt, retryAt)
	}
	if row.FinalizedAt != nil {
		t.Errorf("finalized_at = %v, want unset: a retrying job is not finished", row.FinalizedAt)
	}
	if row.Attempt != claimed.Attempt {
		t.Errorf("attempt = %d, want %d unchanged by the retry transition", row.Attempt, claimed.Attempt)
	}
	recorded := decodeErrors(t, row.Errors)
	if len(recorded) != 1 || recorded[0].Error != "boom" || recorded[0].Attempt != 1 {
		t.Errorf("errors = %+v, want exactly one entry {Attempt:1 Error:boom}", recorded)
	}
}

func TestMarkCancelledFinalizesWithReason(t *testing.T) {
	d, pool := newDriver(t)
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d)

	detail, _ := json.Marshal(driver.AttemptError{Attempt: 1, At: time.Now().UTC(), Error: "bad input"})
	if err := d.MarkCancelled(context.Background(), held(claimed.ID), detail); err != nil {
		t.Fatalf("MarkCancelled: %v", err)
	}

	row := readJob(t, pool, claimed.ID)
	if row.State != "cancelled" {
		t.Errorf("state = %q, want cancelled", row.State)
	}
	if row.FinalizedAt == nil {
		t.Error("finalized_at not set on a cancelled job")
	}
	recorded := decodeErrors(t, row.Errors)
	if len(recorded) != 1 || recorded[0].Error != "bad input" {
		t.Errorf("errors = %+v, want exactly one entry recording the cancellation reason", recorded)
	}
}

func TestMarkSnoozedDefersWithoutConsumingAttempt(t *testing.T) {
	d, pool := newDriver(t)
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d)
	runAt := time.Now().Add(time.Hour).Truncate(time.Microsecond)

	if err := d.MarkSnoozed(context.Background(), held(claimed.ID), runAt); err != nil {
		t.Fatalf("MarkSnoozed: %v", err)
	}

	row := readJob(t, pool, claimed.ID)
	if row.State != "scheduled" {
		t.Errorf("state = %q, want scheduled: a snooze is a deferral, not a failure", row.State)
	}
	if !row.ScheduledAt.Equal(runAt) {
		t.Errorf("scheduled_at = %v, want the requested wake time %v", row.ScheduledAt, runAt)
	}
	if row.FinalizedAt != nil {
		t.Errorf("finalized_at = %v, want unset", row.FinalizedAt)
	}
	if row.Attempt != claimed.Attempt-1 {
		t.Errorf("attempt = %d, want %d: the snooze gives back the attempt the claim consumed",
			row.Attempt, claimed.Attempt-1)
	}
	if recorded := decodeErrors(t, row.Errors); len(recorded) != 0 {
		t.Errorf("errors = %+v, want none: a snooze records no failure", recorded)
	}
}

func TestMarkSnoozedFloorsAttemptAtZero(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d)
	if _, err := pool.Exec(ctx,
		`UPDATE drover_jobs SET attempt = 0 WHERE id = $1`, claimed.ID); err != nil {
		t.Fatalf("zero the attempt: %v", err)
	}

	// The lease names the attempt actually on the row, so this exercises
	// the floor rather than the ownership fence.
	if err := d.MarkSnoozed(ctx, driver.Lease{ID: claimed.ID}, time.Now()); err != nil {
		t.Fatalf("MarkSnoozed: %v", err)
	}

	if row := readJob(t, pool, claimed.ID); row.Attempt != 0 {
		t.Errorf("attempt = %d, want 0: the decrement is floored, never negative", row.Attempt)
	}
}

func TestEveryTransitionClearsTheLease(t *testing.T) {
	tests := []struct {
		name string
		call func(d *pgdriver.Driver, id int64) error
	}{
		{"completed", func(d *pgdriver.Driver, id int64) error {
			return d.MarkCompleted(context.Background(), held(id))
		}},
		{"dead", func(d *pgdriver.Driver, id int64) error {
			return d.MarkDead(context.Background(), held(id), []byte(`{}`))
		}},
		{"cancelled", func(d *pgdriver.Driver, id int64) error {
			return d.MarkCancelled(context.Background(), held(id), []byte(`{}`))
		}},
		{"retryable", func(d *pgdriver.Driver, id int64) error {
			return d.MarkRetryable(context.Background(), held(id), time.Now(), []byte(`{}`))
		}},
		{"snoozed", func(d *pgdriver.Driver, id int64) error {
			return d.MarkSnoozed(context.Background(), held(id), time.Now())
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, pool := newDriver(t)
			mustInsert(t, d, "k", "default")
			claimed := claimOne(t, d)
			if claimed.LeasedUntil == nil {
				t.Fatal("claim did not write a lease")
			}

			if err := tt.call(d, claimed.ID); err != nil {
				t.Fatalf("transition to %s: %v", tt.name, err)
			}

			if row := readJob(t, pool, claimed.ID); row.LeasedUntil != nil {
				t.Errorf("leased_until = %v after moving to %s, want NULL: no stale lease may survive a transition",
					row.LeasedUntil, tt.name)
			}
		})
	}
}

func TestFetchExpiredReclaimsWithoutTouchingAttempt(t *testing.T) {
	d, pool := newDriver(t)
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d)
	expire(t, pool, claimed.ID)
	before := time.Now()

	reclaimed, err := d.FetchExpired(context.Background(), time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchExpired: %v", err)
	}

	if len(reclaimed) != 1 || reclaimed[0].ID != claimed.ID {
		t.Fatalf("reclaimed %d jobs, want the expired job %d", len(reclaimed), claimed.ID)
	}
	if reclaimed[0].Attempt != claimed.Attempt {
		t.Errorf("attempt = %d, want %d unchanged: a rescue never spends an attempt",
			reclaimed[0].Attempt, claimed.Attempt)
	}
	if reclaimed[0].State != "running" {
		t.Errorf("state = %q, want running: the sweeper now owns the row", reclaimed[0].State)
	}
	row := readJob(t, pool, claimed.ID)
	if row.LeasedUntil == nil || row.LeasedUntil.Before(before.Add(time.Minute)) {
		t.Errorf("leased_until = %v, want a fresh lease at least a minute past %v", row.LeasedUntil, before)
	}
	if row.Attempt != claimed.Attempt {
		t.Errorf("stored attempt = %d, want %d unchanged", row.Attempt, claimed.Attempt)
	}
}

func TestFetchExpiredIgnoresLiveAndUnclaimedRows(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, d *pgdriver.Driver)
	}{
		{"running with a live lease", func(t *testing.T, d *pgdriver.Driver) {
			mustInsert(t, d, "k", "default")
			claimOne(t, d)
		}},
		{"available, never claimed", func(t *testing.T, d *pgdriver.Driver) {
			mustInsert(t, d, "k", "default")
		}},
		{"already completed", func(t *testing.T, d *pgdriver.Driver) {
			mustInsert(t, d, "k", "default")
			id := claimOne(t, d).ID
			if err := d.MarkCompleted(context.Background(), held(id)); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _ := newDriver(t)
			tt.setup(t, d)

			reclaimed, err := d.FetchExpired(context.Background(), time.Minute, 10)
			if err != nil {
				t.Fatalf("FetchExpired: %v", err)
			}
			if len(reclaimed) != 0 {
				t.Fatalf("reclaimed %d jobs, want 0", len(reclaimed))
			}
		})
	}
}

func TestExtendLeasesMovesOnlyRunningRows(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	mustInsert(t, d, "k", "default")
	running := claimOne(t, d)
	mustInsert(t, d, "k", "default")
	finished := claimOne(t, d)
	if err := d.MarkCompleted(ctx, held(finished.ID)); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}
	before := time.Now()

	leases := []driver.Lease{held(running.ID), held(finished.ID), held(99999)}
	if err := d.ExtendLeases(ctx, leases, 30*time.Minute); err != nil {
		t.Fatalf("ExtendLeases: %v, want nil: finalized and unknown ids are not failures", err)
	}

	row := readJob(t, pool, running.ID)
	if row.LeasedUntil == nil || row.LeasedUntil.Before(before.Add(30*time.Minute)) {
		t.Errorf("running job leased_until = %v, want at least 30 minutes past %v", row.LeasedUntil, before)
	}
	done := readJob(t, pool, finished.ID)
	if done.State != "completed" {
		t.Errorf("finished job state = %q, want completed: an extension must not resurrect it", done.State)
	}
	if done.LeasedUntil != nil {
		t.Errorf("finished job leased_until = %v, want NULL: an extension must not re-lease it", done.LeasedUntil)
	}
}

func TestConcurrentFetchExpiredReclaimsEachJobExactlyOnce(t *testing.T) {
	d, pool := newDriver(t)
	const jobs = 25
	for range jobs {
		mustInsert(t, d, "k", "default")
	}
	claimed, err := d.FetchAvailable(context.Background(), "default", time.Minute, jobs)
	if err != nil || len(claimed) != jobs {
		t.Fatalf("claim all: %v (%d rows)", err, len(claimed))
	}
	ids := make([]int64, len(claimed))
	for i, row := range claimed {
		ids[i] = row.ID
	}
	expire(t, pool, ids...)

	var mu sync.Mutex
	seen := make(map[int64]int)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				rows, err := d.FetchExpired(context.Background(), time.Minute, 1)
				if err != nil {
					t.Errorf("FetchExpired: %v", err)
					return
				}
				if len(rows) == 0 {
					return
				}
				mu.Lock()
				seen[rows[0].ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != jobs {
		t.Fatalf("reclaimed %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d reclaimed %d times, want exactly once", id, n)
		}
	}
}

func TestInsertHonoursScheduledAt(t *testing.T) {
	d, _ := newDriver(t)

	now := time.Now()
	tests := []struct {
		name      string
		at        time.Time
		wantState string
		claimable bool
	}{
		{"zero means now", time.Time{}, "available", true},
		{"a past time is due already", now.Add(-time.Hour), "available", true},
		{"a future time waits", now.Add(time.Hour), "scheduled", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue := "sched_" + strings.ReplaceAll(tt.name, " ", "_")
			row, err := d.Insert(context.Background(), driver.InsertParams{
				Kind:        "send_email",
				Queue:       queue,
				Args:        []byte(`{"n":1}`),
				ScheduledAt: tt.at,
			})
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if row.State != tt.wantState {
				t.Errorf("State = %q, want %q", row.State, tt.wantState)
			}
			// timestamptz keeps microseconds, so the stored instant is the
			// requested one truncated — not a different time.
			if want := tt.at.Truncate(time.Microsecond); !tt.at.IsZero() && !row.ScheduledAt.Equal(want) {
				t.Errorf("ScheduledAt = %v, want %v", row.ScheduledAt.UTC(), want.UTC())
			}

			claimed, err := d.FetchAvailable(context.Background(), queue, time.Minute, 10)
			if err != nil {
				t.Fatalf("FetchAvailable: %v", err)
			}
			if got := len(claimed) == 1; got != tt.claimable {
				t.Errorf("claimed %d job(s), want claimable=%v", len(claimed), tt.claimable)
			}
		})
	}
}

// The stored state has to agree with the fetch predicate, and the
// predicate is evaluated by the database. A job inserted just far enough
// ahead that only one clock could call it due must come back scheduled
// and unclaimable, then claimable once that time passes — without
// anything promoting it out of the scheduled state.
func TestScheduledJobBecomesClaimableWhenItsTimeArrives(t *testing.T) {
	d, _ := newDriver(t)

	due := time.Now().Add(400 * time.Millisecond)
	row, err := d.Insert(context.Background(), driver.InsertParams{
		Kind: "send_email", Queue: "sched_arrival", Args: []byte(`{"n":1}`), ScheduledAt: due,
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if row.State != "scheduled" {
		t.Fatalf("State = %q, want scheduled", row.State)
	}

	claimed, err := d.FetchAvailable(context.Background(), "sched_arrival", time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed %d job(s) before the scheduled time", len(claimed))
	}

	time.Sleep(time.Until(due) + 200*time.Millisecond)
	claimed, err = d.FetchAvailable(context.Background(), "sched_arrival", time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != row.ID {
		t.Fatalf("claimed %d job(s) after the scheduled time, want job %d", len(claimed), row.ID)
	}
}

// statsDriver is the slice of the storage contract the queue-statistics
// scenario below needs. Naming it here lets the same scenario be driven
// against the in-memory driver, which is the only way to check that the
// two agree rather than that each is self-consistent.
type statsDriver interface {
	Insert(ctx context.Context, params driver.InsertParams) (*driver.JobRow, error)
	FetchAvailable(ctx context.Context, queue string, leaseFor time.Duration, limit int) ([]*driver.JobRow, error)
	MarkCompleted(ctx context.Context, lease driver.Lease) error
	MarkCancelled(ctx context.Context, lease driver.Lease, errDetail []byte) error
	MarkDead(ctx context.Context, lease driver.Lease, errDetail []byte) error
	MarkRetryable(ctx context.Context, lease driver.Lease, retryAt time.Time, errDetail []byte) error
	Stats(ctx context.Context) (*driver.Stats, error)
}

const (
	// overdue is how long the scenario's oldest claimable job has been due.
	overdue = 90 * time.Second
	// backoffEnded is how long ago a retryable job's backoff ran out.
	backoffEnded = 30 * time.Second
)

func dueOn(t *testing.T, d statsDriver, queue string, at time.Time) *driver.JobRow {
	t.Helper()
	row, err := d.Insert(context.Background(), driver.InsertParams{
		Kind:        "k",
		Queue:       queue,
		Args:        []byte(`{"n":1}`),
		ScheduledAt: at,
	})
	if err != nil {
		t.Fatalf("Insert on queue %q: %v", queue, err)
	}
	return row
}

func claimFrom(t *testing.T, d statsDriver, queue string) {
	t.Helper()
	rows, err := d.FetchAvailable(context.Background(), queue, time.Minute, 1)
	if err != nil {
		t.Fatalf("FetchAvailable from queue %q: %v", queue, err)
	}
	if len(rows) != 1 {
		t.Fatalf("FetchAvailable from queue %q claimed %d jobs, want 1", queue, len(rows))
	}
}

// buildStatsScenario drives d through one fixed set of jobs using only
// the driver's own transitions, so any implementation can be asked for a
// reading of the same situation. Every job that has to be claimed is
// claimed while it is the only claimable job on its queue.
func buildStatsScenario(t *testing.T, d statsDriver) {
	t.Helper()
	ctx := context.Background()

	// alpha is behind: one job came due overdue ago, and one is
	// deliberately deferred and therefore not late at all.
	dueOn(t, d, "alpha", time.Now().Add(-overdue))
	dueOn(t, d, "alpha", time.Now().Add(time.Hour))

	// beta has one job in a worker's hands and one that died in them.
	dueOn(t, d, "beta", time.Now())
	claimFrom(t, d, "beta")
	died := dueOn(t, d, "beta", time.Now())
	claimFrom(t, d, "beta")
	if err := d.MarkDead(ctx, held(died.ID), []byte(`{"error":"boom"}`)); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	// gamma holds the two states that are never counted, plus one job
	// whose backoff has run out and is waiting to be picked up again.
	finished := dueOn(t, d, "gamma", time.Now())
	claimFrom(t, d, "gamma")
	if err := d.MarkCompleted(ctx, held(finished.ID)); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}
	abandoned := dueOn(t, d, "gamma", time.Now())
	claimFrom(t, d, "gamma")
	if err := d.MarkCancelled(ctx, held(abandoned.ID), []byte(`{"error":"nope"}`)); err != nil {
		t.Fatalf("MarkCancelled: %v", err)
	}
	retried := dueOn(t, d, "gamma", time.Now())
	claimFrom(t, d, "gamma")
	if err := d.MarkRetryable(ctx, held(retried.ID),
		time.Now().Add(-backoffEnded), []byte(`{"error":"boom"}`)); err != nil {
		t.Fatalf("MarkRetryable: %v", err)
	}

	// delta holds nothing but a job scheduled for later. It is the queue
	// that separates lateness from delay: anywhere a deferred job sits
	// beside an overdue one the overdue one is the older of the two, so a
	// reading that forgot to check whether a job is due yet would still
	// look right. Here there is nothing to hide behind.
	dueOn(t, d, "delta", time.Now().Add(time.Hour))
}

// assertAges compares a reading's ages queue by queue. The tolerance
// covers the time the scenario itself takes to build; it is orders of
// magnitude below the hour a job counted from the wrong side of now would
// be out by.
func assertAges(t *testing.T, got, want []driver.QueueAge, tolerance float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("Oldest = %+v, want %+v", got, want)
	}
	for i, w := range want {
		if got[i].Queue != w.Queue || math.Abs(got[i].AgeSeconds-w.AgeSeconds) > tolerance {
			t.Errorf("Oldest[%d] = %+v, want queue %q aged about %vs", i, got[i], w.Queue, w.AgeSeconds)
		}
	}
}

func TestStatsReportsDepthPerQueueAndAgeOfWhatIsClaimable(t *testing.T) {
	d, _ := newDriver(t)

	buildStatsScenario(t, d)

	stats, err := d.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	// gamma's completed and cancelled jobs are absent by design: those are
	// the rows nothing ever removes, and counting them would tie the cost
	// of this query to the whole history of the queue.
	wantDepths := []driver.QueueDepth{
		{Queue: "alpha", State: "available", Count: 1},
		{Queue: "alpha", State: "scheduled", Count: 1},
		{Queue: "beta", State: "dead", Count: 1},
		{Queue: "beta", State: "running", Count: 1},
		{Queue: "delta", State: "scheduled", Count: 1},
		{Queue: "gamma", State: "retryable", Count: 1},
	}
	if !slices.Equal(stats.Depths, wantDepths) {
		t.Errorf("Depths = %+v, want %+v", stats.Depths, wantDepths)
	}

	// Only alpha and gamma are late. beta has nothing claimable at all —
	// one job is running, the other is dead — and delta's only job is not
	// due yet, so neither is reported as waiting rather than being
	// reported as waiting for zero.
	assertAges(t, stats.Oldest, []driver.QueueAge{
		{Queue: "alpha", AgeSeconds: overdue.Seconds()},
		{Queue: "gamma", AgeSeconds: backoffEnded.Seconds()},
	}, 5)
}

func TestStatsAgreesWithTheInMemoryDriver(t *testing.T) {
	pg, _ := newDriver(t)
	mem := memdriver.New()

	buildStatsScenario(t, pg)
	buildStatsScenario(t, mem)

	ctx := context.Background()
	fromPostgres, err := pg.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats from Postgres: %v", err)
	}
	fromMemory, err := mem.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats from memory: %v", err)
	}

	// The in-memory driver is the executable specification the Postgres
	// one is checked against; a depth the two disagree on means one of the
	// predicates was restated rather than reused.
	if !slices.Equal(fromPostgres.Depths, fromMemory.Depths) {
		t.Errorf("Postgres depths = %+v, in-memory depths = %+v", fromPostgres.Depths, fromMemory.Depths)
	}
	assertAges(t, fromPostgres.Oldest, fromMemory.Oldest, 5)
}

func TestStatsAgesJobsByTheDatabaseClock(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	row := mustInsert(t, d, "k", "default")

	// The scheduled time is written from the database's own clock, so the
	// age reported for it can only be right if it was subtracted there
	// too. A caller subtracting its own now() would be out by whatever the
	// two machines disagree by, which across a fleet is not zero.
	if _, err := pool.Exec(ctx,
		`UPDATE drover_jobs SET scheduled_at = now() - interval '90 seconds' WHERE id = $1`,
		row.ID); err != nil {
		t.Fatalf("age the job by the database clock: %v", err)
	}

	stats, err := d.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	var byDatabase float64
	if err := pool.QueryRow(ctx,
		`SELECT EXTRACT(EPOCH FROM (now() - scheduled_at))::float8 FROM drover_jobs WHERE id = $1`,
		row.ID).Scan(&byDatabase); err != nil {
		t.Fatalf("read the age the database computes: %v", err)
	}
	if len(stats.Oldest) != 1 || stats.Oldest[0].Queue != "default" {
		t.Fatalf("Oldest = %+v, want one entry for queue default", stats.Oldest)
	}
	if skew := math.Abs(stats.Oldest[0].AgeSeconds - byDatabase); skew > 1 {
		t.Errorf("reported age is %vs from the age the database computes, want it measured by that clock", skew)
	}
}

func TestStatsOnAnEmptyDatabaseReportsNothing(t *testing.T) {
	d, _ := newDriver(t)

	stats, err := d.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if len(stats.Depths) != 0 || len(stats.Oldest) != 0 {
		t.Errorf("Stats = %+v, want no depths and no ages", stats)
	}
}

func TestListJobsFiltersOrdersAndLimits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d, _ := newDriver(t)

	mustInsert(t, d, "a", "alpha")
	mustInsert(t, d, "b", "beta")
	deadSeed := mustInsert(t, d, "c", "alpha")
	mustInsert(t, d, "d", "beta")

	claimed, err := d.FetchAvailable(ctx, "alpha", time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	var deadID int64
	for _, row := range claimed {
		if row.ID == deadSeed.ID {
			if err := d.MarkDead(ctx, driver.Lease{ID: row.ID, Attempt: row.Attempt}, []byte(`{"error":"x"}`)); err != nil {
				t.Fatalf("MarkDead: %v", err)
			}
			deadID = row.ID
			continue
		}
		if err := d.MarkCompleted(ctx, driver.Lease{ID: row.ID, Attempt: row.Attempt}); err != nil {
			t.Fatalf("MarkCompleted: %v", err)
		}
	}
	if deadID == 0 {
		t.Fatal("expected dead seed to be claimed")
	}
	mustInsert(t, d, "e", "alpha")

	all, err := d.ListJobs(ctx, driver.ListJobsParams{Limit: 100})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(all) < 2 {
		t.Fatalf("ListJobs returned %d rows, want at least 2", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].ID < all[i].ID {
			t.Fatalf("ids not descending: %d then %d", all[i-1].ID, all[i].ID)
		}
	}

	beta, err := d.ListJobs(ctx, driver.ListJobsParams{Queue: "beta", Limit: 100})
	if err != nil {
		t.Fatalf("ListJobs queue: %v", err)
	}
	if len(beta) == 0 {
		t.Fatal("expected at least one beta job")
	}
	for _, row := range beta {
		if row.Queue != "beta" {
			t.Errorf("queue = %q, want beta", row.Queue)
		}
	}

	dead, err := d.ListJobs(ctx, driver.ListJobsParams{State: "dead", Limit: 100})
	if err != nil {
		t.Fatalf("ListJobs state: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != deadID || dead[0].State != "dead" {
		t.Fatalf("ListJobs dead = %+v, want only id %d", dead, deadID)
	}

	limited, err := d.ListJobs(ctx, driver.ListJobsParams{Limit: 1})
	if err != nil {
		t.Fatalf("ListJobs limit: %v", err)
	}
	if len(limited) != 1 || limited[0].ID != all[0].ID {
		t.Fatalf("ListJobs limit 1 = %+v, want newest id %d", limited, all[0].ID)
	}

	for _, limit := range []int{0, -1} {
		if _, err := d.ListJobs(ctx, driver.ListJobsParams{Limit: limit}); err == nil {
			t.Fatalf("ListJobs limit %d succeeded", limit)
		}
	}
}

func TestGetJobReturnsRowOrNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d, _ := newDriver(t)
	inserted := mustInsert(t, d, "k", "default")

	got, err := d.GetJob(ctx, inserted.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.ID != inserted.ID || got.Kind != "k" || got.State != "available" {
		t.Errorf("GetJob = %+v, want inserted available job", got)
	}

	_, err = d.GetJob(ctx, 99999)
	if !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("GetJob unknown: %v, want ErrNotFound", err)
	}
}

func TestOperatorCancelAllowedAndRefusedStates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name      string
		setup     func(t *testing.T, d *pgdriver.Driver, pool *pgxpool.Pool) int64
		wantState string
		wantErr   error
	}{
		{
			name: "available",
			setup: func(t *testing.T, d *pgdriver.Driver, _ *pgxpool.Pool) int64 {
				return mustInsert(t, d, "k", "default").ID
			},
			wantState: "cancelled",
		},
		{
			name: "scheduled",
			setup: func(t *testing.T, d *pgdriver.Driver, pool *pgxpool.Pool) int64 {
				return park(t, d, pool, "scheduled", time.Now().Add(time.Hour))
			},
			wantState: "cancelled",
		},
		{
			name: "retryable",
			setup: func(t *testing.T, d *pgdriver.Driver, pool *pgxpool.Pool) int64 {
				return park(t, d, pool, "retryable", time.Now().Add(time.Hour))
			},
			wantState: "cancelled",
		},
		{
			name: "dead",
			setup: func(t *testing.T, d *pgdriver.Driver, _ *pgxpool.Pool) int64 {
				mustInsert(t, d, "k", "default")
				row := claimOne(t, d)
				if err := d.MarkDead(ctx, held(row.ID), []byte(`{"error":"boom"}`)); err != nil {
					t.Fatal(err)
				}
				return row.ID
			},
			wantState: "cancelled",
		},
		{
			name: "running refused",
			setup: func(t *testing.T, d *pgdriver.Driver, _ *pgxpool.Pool) int64 {
				mustInsert(t, d, "k", "default")
				return claimOne(t, d).ID
			},
			wantErr: driver.ErrInvalidTransition,
		},
		{
			name: "completed refused",
			setup: func(t *testing.T, d *pgdriver.Driver, _ *pgxpool.Pool) int64 {
				mustInsert(t, d, "k", "default")
				row := claimOne(t, d)
				if err := d.MarkCompleted(ctx, held(row.ID)); err != nil {
					t.Fatal(err)
				}
				return row.ID
			},
			wantErr: driver.ErrInvalidTransition,
		},
		{
			name: "cancelled refused",
			setup: func(t *testing.T, d *pgdriver.Driver, _ *pgxpool.Pool) int64 {
				id := mustInsert(t, d, "k", "default").ID
				if _, err := d.OperatorCancel(ctx, id); err != nil {
					t.Fatal(err)
				}
				return id
			},
			wantErr: driver.ErrInvalidTransition,
		},
		{
			name:    "unknown id",
			setup:   func(*testing.T, *pgdriver.Driver, *pgxpool.Pool) int64 { return 99999 },
			wantErr: driver.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d, pool := newDriver(t)
			id := tt.setup(t, d, pool)
			var before storedJob
			if tt.wantErr != nil && id != 99999 {
				before = readJob(t, pool, id)
			}

			got, err := d.OperatorCancel(ctx, id)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("OperatorCancel: %v, want %v", err, tt.wantErr)
				}
				if id != 99999 {
					after := readJob(t, pool, id)
					if after.State != before.State {
						t.Errorf("state changed to %s on refusal, was %s", after.State, before.State)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("OperatorCancel: %v", err)
			}
			if got.State != tt.wantState || got.FinalizedAt == nil {
				t.Errorf("OperatorCancel = state=%s finalized=%v, want %s with finalized_at",
					got.State, got.FinalizedAt, tt.wantState)
			}
			stored := readJob(t, pool, id)
			if stored.State != "cancelled" || stored.FinalizedAt == nil {
				t.Errorf("stored = %+v, want cancelled with finalized_at", stored)
			}
		})
	}
}

func TestRedriveDeadFromDeadOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("dead redriven", func(t *testing.T) {
		t.Parallel()
		d, pool := newDriver(t)
		mustInsert(t, d, "k", "default")
		row := claimOne(t, d)
		detail := []byte(`{"attempt":1,"error":"boom"}`)
		if err := d.MarkDead(ctx, held(row.ID), detail); err != nil {
			t.Fatalf("MarkDead: %v", err)
		}
		before := readJob(t, pool, row.ID)

		got, err := d.RedriveDead(ctx, row.ID)
		if err != nil {
			t.Fatalf("RedriveDead: %v", err)
		}
		if got.State != "available" || got.Attempt != 0 || got.LeasedUntil != nil || got.FinalizedAt != nil {
			t.Errorf("RedriveDead = %+v, want available attempt=0 cleared lease/finalized", got)
		}
		if !slices.Equal(got.Errors, before.Errors) {
			t.Errorf("errors = %s, want retained %s", got.Errors, before.Errors)
		}
		stored := readJob(t, pool, row.ID)
		if stored.State != "available" || stored.Attempt != 0 || stored.LeasedUntil != nil || stored.FinalizedAt != nil {
			t.Errorf("stored = %+v, want available attempt=0 cleared lease/finalized", stored)
		}
		if !slices.Equal(stored.Errors, before.Errors) {
			t.Errorf("stored errors = %s, want retained %s", stored.Errors, before.Errors)
		}
		// scheduled_at must be the database clock's now — at or after the
		// prior finalization, and not far in the future.
		if stored.ScheduledAt.Before(*before.FinalizedAt) {
			t.Errorf("scheduled_at %v before prior finalized_at %v", stored.ScheduledAt, before.FinalizedAt)
		}
	})

	t.Run("non-dead refused", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name  string
			setup func(t *testing.T, d *pgdriver.Driver) int64
		}{
			{"available", func(t *testing.T, d *pgdriver.Driver) int64 {
				return mustInsert(t, d, "k", "default").ID
			}},
			{"running", func(t *testing.T, d *pgdriver.Driver) int64 {
				mustInsert(t, d, "k", "default")
				return claimOne(t, d).ID
			}},
			{"completed", func(t *testing.T, d *pgdriver.Driver) int64 {
				mustInsert(t, d, "k", "default")
				id := claimOne(t, d).ID
				if err := d.MarkCompleted(ctx, held(id)); err != nil {
					t.Fatal(err)
				}
				return id
			}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				d, pool := newDriver(t)
				id := tt.setup(t, d)
				before := readJob(t, pool, id)
				_, err := d.RedriveDead(ctx, id)
				if !errors.Is(err, driver.ErrInvalidTransition) {
					t.Fatalf("RedriveDead: %v, want ErrInvalidTransition", err)
				}
				after := readJob(t, pool, id)
				if after.State != before.State || after.Attempt != before.Attempt {
					t.Errorf("row mutated on refusal: before=%+v after=%+v", before, after)
				}
			})
		}
	})

	t.Run("unknown id", func(t *testing.T) {
		t.Parallel()
		d, _ := newDriver(t)
		_, err := d.RedriveDead(ctx, 99999)
		if !errors.Is(err, driver.ErrNotFound) {
			t.Errorf("RedriveDead: %v, want ErrNotFound", err)
		}
	})
}

func TestOperatorCancelRaceReportsInvalidTransition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d, pool := newDriver(t)
	id := mustInsert(t, d, "k", "default").ID

	if _, err := d.OperatorCancel(ctx, id); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	_, err := d.OperatorCancel(ctx, id)
	if !errors.Is(err, driver.ErrInvalidTransition) {
		t.Fatalf("second cancel: %v, want ErrInvalidTransition", err)
	}
	if got := readJob(t, pool, id); got.State != "cancelled" {
		t.Errorf("state = %s, want cancelled", got.State)
	}
}

func TestInsertManyReturnsRowsInInputOrder(t *testing.T) {
	d, _ := newDriver(t)

	batch := []driver.InsertParams{
		{Kind: "first", Queue: "default", Args: []byte(`{"n":1}`)},
		{Kind: "second", Queue: "default", Args: []byte(`{"n":2}`)},
		{Kind: "third", Queue: "default", Args: []byte(`{"n":3}`)},
	}
	rows, err := d.InsertMany(context.Background(), batch)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(rows) != len(batch) {
		t.Fatalf("InsertMany returned %d rows, want %d", len(rows), len(batch))
	}

	seen := make(map[int64]struct{}, len(rows))
	for i, row := range rows {
		if row.Kind != batch[i].Kind {
			t.Errorf("row %d Kind = %q, want %q", i, row.Kind, batch[i].Kind)
		}
		if row.ID <= 0 {
			t.Errorf("row %d ID = %d, want a positive id", i, row.ID)
		}
		if _, dup := seen[row.ID]; dup {
			t.Errorf("row %d ID %d is not distinct", i, row.ID)
		}
		seen[row.ID] = struct{}{}
	}
}

func TestInsertManyEmptyBatchWritesNothing(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	existing := mustInsert(t, d, "keep", "default")

	for _, batch := range [][]driver.InsertParams{nil, {}} {
		rows, err := d.InsertMany(ctx, batch)
		if err != nil {
			t.Fatalf("InsertMany(%v): %v", batch, err)
		}
		if len(rows) != 0 {
			t.Fatalf("InsertMany(%v) returned %d rows, want 0", batch, len(rows))
		}
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM drover_jobs`).Scan(&n); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if n != 1 {
		t.Fatalf("store has %d jobs, want 1", n)
	}
	if got := readJob(t, pool, existing.ID); got.State != "available" {
		t.Errorf("existing job state = %s, want available", got.State)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	empty, err := d.InsertManyTx(ctx, tx, nil)
	if err != nil {
		t.Fatalf("InsertManyTx(nil): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("InsertManyTx(nil) returned %d rows, want 0", len(empty))
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM drover_jobs`).Scan(&n); err != nil {
		t.Fatalf("count jobs after empty InsertManyTx: %v", err)
	}
	if n != 1 {
		t.Fatalf("after empty InsertManyTx: %d jobs, want 1", n)
	}
}

func TestInsertManyHonoursScheduledAtAndQueues(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	now := time.Now()
	future := now.Add(time.Hour)

	rows, err := d.InsertMany(ctx, []driver.InsertParams{
		{Kind: "due-alpha", Queue: "alpha", Args: []byte(`{"n":1}`)},
		{Kind: "wait-beta", Queue: "beta", Args: []byte(`{"n":2}`), ScheduledAt: future},
		{Kind: "due-beta", Queue: "beta", Args: []byte(`{"n":3}`), ScheduledAt: now.Add(-time.Minute)},
	})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("InsertMany returned %d rows, want 3", len(rows))
	}

	if rows[0].Queue != "alpha" || rows[0].State != "available" {
		t.Errorf("row 0 queue/state = %q/%q, want alpha/available", rows[0].Queue, rows[0].State)
	}
	if rows[0].ScheduledAt.IsZero() {
		t.Error("row 0 ScheduledAt is zero; zero input must become now")
	}
	if rows[1].Queue != "beta" || rows[1].State != "scheduled" {
		t.Errorf("row 1 queue/state = %q/%q, want beta/scheduled", rows[1].Queue, rows[1].State)
	}
	if want := future.Truncate(time.Microsecond); !rows[1].ScheduledAt.Equal(want) {
		t.Errorf("row 1 ScheduledAt = %v, want %v", rows[1].ScheduledAt.UTC(), want.UTC())
	}
	if rows[2].Queue != "beta" || rows[2].State != "available" {
		t.Errorf("row 2 queue/state = %q/%q, want beta/available", rows[2].Queue, rows[2].State)
	}

	alpha, err := d.FetchAvailable(ctx, "alpha", time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchAvailable alpha: %v", err)
	}
	if len(alpha) != 1 || alpha[0].ID != rows[0].ID {
		t.Fatalf("alpha claimed %d job(s), want job %d", len(alpha), rows[0].ID)
	}

	beta, err := d.FetchAvailable(ctx, "beta", time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchAvailable beta: %v", err)
	}
	if len(beta) != 1 || beta[0].ID != rows[2].ID {
		t.Fatalf("beta claimed %d job(s), want due job %d", len(beta), rows[2].ID)
	}
}

func TestInsertManyTxVisibilityFollowsTransaction(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()
	batch := []driver.InsertParams{
		{Kind: "k", Queue: "default", Args: []byte(`{}`)},
		{Kind: "k2", Queue: "default", Args: []byte(`{}`)},
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := d.InsertManyTx(ctx, tx, batch); err != nil {
		t.Fatalf("InsertManyTx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	claimed, err := d.FetchAvailable(ctx, "default", time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchAvailable after rollback: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("after rollback FetchAvailable claimed %d jobs, want 0", len(claimed))
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	inserted, err := d.InsertManyTx(ctx, tx, batch)
	if err != nil {
		t.Fatalf("InsertManyTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	claimed, err = d.FetchAvailable(ctx, "default", time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchAvailable after commit: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("after commit FetchAvailable claimed %d jobs, want 2", len(claimed))
	}
	if claimed[0].ID != inserted[0].ID || claimed[1].ID != inserted[1].ID {
		t.Errorf("claimed ids %d,%d, want %d,%d", claimed[0].ID, claimed[1].ID, inserted[0].ID, inserted[1].ID)
	}
}

func TestInsertManyTxOnRolledBackTransactionPersistsNothing(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	_, err = d.InsertManyTx(ctx, tx, []driver.InsertParams{
		{Kind: "k", Queue: "default", Args: []byte(`{}`)},
	})
	if err == nil {
		t.Fatal("InsertManyTx error = nil, want a failure after the transaction was rolled back")
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM drover_jobs`).Scan(&n); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if n != 0 {
		t.Errorf("drover_jobs has %d rows after a failed InsertManyTx, want 0", n)
	}
}

func TestInsertManyTxTwiceInOneTransaction(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	first, err := d.InsertManyTx(ctx, tx, []driver.InsertParams{
		{Kind: "a", Queue: "default", Args: []byte(`{}`)},
		{Kind: "b", Queue: "default", Args: []byte(`{}`)},
	})
	if err != nil {
		t.Fatalf("first InsertManyTx: %v", err)
	}
	second, err := d.InsertManyTx(ctx, tx, []driver.InsertParams{
		{Kind: "c", Queue: "default", Args: []byte(`{}`)},
	})
	if err != nil {
		t.Fatalf("second InsertManyTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	claimed, err := d.FetchAvailable(ctx, "default", time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d jobs, want 3", len(claimed))
	}
	want := []string{"a", "b", "c"}
	got := []string{claimed[0].Kind, claimed[1].Kind, claimed[2].Kind}
	if !slices.Equal(got, want) {
		t.Errorf("claimed kinds %v, want %v (ids %d,%d then %d)", got, want, first[0].ID, first[1].ID, second[0].ID)
	}
}

func TestInsertManyTxRejectsNonPgxTransaction(t *testing.T) {
	d, _ := newDriver(t)

	_, err := d.InsertManyTx(context.Background(), "not a tx", []driver.InsertParams{
		{Kind: "k", Queue: "default"},
	})
	if !errors.Is(err, driver.ErrTxUnsupported) {
		t.Fatalf("error = %v, want ErrTxUnsupported", err)
	}
}

func TestInsertManyUsesCopyFrom(t *testing.T) {
	d, _, trace := newTracedDriver(t)

	const n = 5
	batch := make([]driver.InsertParams, n)
	for i := range batch {
		batch[i] = driver.InsertParams{Kind: "k", Queue: "default", Args: []byte(`{}`)}
	}
	rows, err := d.InsertMany(context.Background(), batch)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(rows) != n {
		t.Fatalf("InsertMany returned %d rows, want %d", len(rows), n)
	}

	trace.mu.Lock()
	defer trace.mu.Unlock()
	if trace.copyFrom != 1 {
		t.Errorf("CopyFrom called %d times, want 1", trace.copyFrom)
	}
	if trace.insertJob != 0 {
		t.Errorf("InsertJob VALUES ran %d times, want 0 — the batch must not loop single-row inserts", trace.insertJob)
	}
}

type insertTrace struct {
	mu        sync.Mutex
	copyFrom  int
	insertJob int
}

func (t *insertTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "INSERT INTO drover_jobs") && strings.Contains(data.SQL, "VALUES") {
		t.mu.Lock()
		t.insertJob++
		t.mu.Unlock()
	}
	return ctx
}

func (t *insertTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *insertTrace) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	t.mu.Lock()
	t.copyFrom++
	t.mu.Unlock()
	return ctx
}

func (t *insertTrace) TraceCopyFromEnd(context.Context, *pgx.Conn, pgx.TraceCopyFromEndData) {}

func newTracedDriver(t *testing.T) (*pgdriver.Driver, *pgxpool.Pool, *insertTrace) {
	t.Helper()
	base := testdb.NewDB(t)
	trace := &insertTrace{}
	cfg := base.Config()
	cfg.ConnConfig.Tracer = trace
	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("traced pool: %v", err)
	}
	t.Cleanup(pool.Close)
	d := pgdriver.New(pool)
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return d, pool, trace
}

func TestNotifyTxDeliversOnlyOnCommit(t *testing.T) {
	t.Parallel()
	d, pool := newDriver(t)
	ctx := context.Background()

	listener, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, "LISTEN drover"); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // commit path succeeds; rollback after commit is a no-op

	if err := d.NotifyTx(ctx, tx); err != nil {
		t.Fatalf("NotifyTx: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	n, err := listener.Conn().WaitForNotification(waitCtx)
	cancel()
	if err == nil {
		t.Fatalf("received NOTIFY on %q before commit", n.Channel)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	waitCtx, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	n, err = listener.Conn().WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("expected NOTIFY after commit: %v", err)
	}
	if n.Channel != "drover" {
		t.Errorf("channel = %q, want drover", n.Channel)
	}
}

// notifyFailTx lets SAVEPOINT/ROLLBACK reach Postgres but replaces
// pg_notify with a statement error so the savepoint must absorb it.
type notifyFailTx struct {
	pgx.Tx
}

func (t notifyFailTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "pg_notify") {
		return t.Tx.Exec(ctx, "SELECT 1/0")
	}
	return t.Tx.Exec(ctx, sql, arguments...)
}

func TestNotifyTxFailureDoesNotAbortCallerTransaction(t *testing.T) {
	t.Parallel()
	d, pool := newDriver(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // commit path succeeds; rollback after commit is a no-op

	row, err := d.InsertTx(ctx, tx, driver.InsertParams{
		Kind:  "k",
		Queue: "default",
		Args:  []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("InsertTx: %v", err)
	}

	if err := d.NotifyTx(ctx, notifyFailTx{Tx: tx}); err == nil {
		t.Fatal("NotifyTx error = nil, want the injected statement failure")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit after notify failure: %v — notify must not abort the caller transaction", err)
	}

	got, err := d.GetJob(ctx, row.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got == nil || got.ID != row.ID {
		t.Fatalf("job %d missing after commit; notify failure rolled back the insert", row.ID)
	}
}

func TestListenWakeupsExitsWhenContextCancelled(t *testing.T) {
	t.Parallel()
	d, _ := newDriver(t)
	ctx, cancel := context.WithCancel(context.Background())
	wake := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- d.ListenWakeups(ctx, wake) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("ListenWakeups = %v, want nil after cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ListenWakeups did not return after cancel")
	}
}

func TestListenWakeupsNudgesOnNotify(t *testing.T) {
	t.Parallel()
	d, _ := newDriver(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- d.ListenWakeups(ctx, wake) }()

	time.Sleep(100 * time.Millisecond)
	deadline := time.Now().Add(time.Second)
	for {
		if err := d.Notify(ctx); err != nil {
			t.Fatalf("Notify: %v", err)
		}
		select {
		case <-wake:
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("ListenWakeups = %v, want nil after cancel", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("ListenWakeups did not return after cancel")
			}
			return
		case err := <-done:
			t.Fatalf("ListenWakeups returned early: %v", err)
		case <-time.After(20 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatal("ListenWakeups did not signal wake after Notify")
			}
		}
	}
}

func countJobs(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM drover_jobs`).Scan(&n); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	return n
}

func storedUniqueKey(t *testing.T, pool *pgxpool.Pool, id int64) *string {
	t.Helper()
	var key *string
	if err := pool.QueryRow(context.Background(),
		`SELECT unique_key FROM drover_jobs WHERE id = $1`, id).Scan(&key); err != nil {
		t.Fatalf("read unique_key: %v", err)
	}
	return key
}

func TestInsertPersistsUniqueKey(t *testing.T) {
	t.Parallel()
	d, pool := newDriver(t)

	row, err := d.Insert(context.Background(), driver.InsertParams{
		Kind: "send", Queue: "default", Args: []byte(`{}`), UniqueKey: "invoice-1",
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if row.UniqueKey != "invoice-1" {
		t.Errorf("UniqueKey = %q, want invoice-1", row.UniqueKey)
	}
	got := storedUniqueKey(t, pool, row.ID)
	if got == nil || *got != "invoice-1" {
		t.Errorf("stored unique_key = %v, want invoice-1", got)
	}
}

func TestInsertEmptyUniqueKeyStoresNull(t *testing.T) {
	t.Parallel()
	d, pool := newDriver(t)
	ctx := context.Background()

	first, err := d.Insert(ctx, driver.InsertParams{
		Kind: "k", Queue: "default", Args: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("first Insert: %v", err)
	}
	second, err := d.Insert(ctx, driver.InsertParams{
		Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "",
	})
	if err != nil {
		t.Fatalf("second Insert: %v", err)
	}
	if first.UniqueKey != "" || second.UniqueKey != "" {
		t.Errorf("UniqueKey = %q and %q, want both empty", first.UniqueKey, second.UniqueKey)
	}
	if storedUniqueKey(t, pool, first.ID) != nil || storedUniqueKey(t, pool, second.ID) != nil {
		t.Error("empty UniqueKey must be stored as NULL")
	}
	if countJobs(t, pool) != 2 {
		t.Errorf("store has %d jobs, want 2 — empty keys must not collide", countJobs(t, pool))
	}
}

func TestInsertDuplicateUniqueKeyFailsWhileNonTerminal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	future := time.Now().Add(time.Hour)

	tests := []struct {
		name  string
		setup func(*testing.T, *pgdriver.Driver)
	}{
		{
			name: "available",
			setup: func(t *testing.T, d *pgdriver.Driver) {
				if _, err := d.Insert(ctx, driver.InsertParams{
					Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u",
				}); err != nil {
					t.Fatalf("seed: %v", err)
				}
			},
		},
		{
			name: "scheduled",
			setup: func(t *testing.T, d *pgdriver.Driver) {
				if _, err := d.Insert(ctx, driver.InsertParams{
					Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u", ScheduledAt: future,
				}); err != nil {
					t.Fatalf("seed: %v", err)
				}
			},
		},
		{
			name: "retryable",
			setup: func(t *testing.T, d *pgdriver.Driver) {
				row, err := d.Insert(ctx, driver.InsertParams{
					Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u",
				})
				if err != nil {
					t.Fatalf("seed: %v", err)
				}
				claimOne(t, d)
				if err := d.MarkRetryable(ctx, held(row.ID), future, []byte(`{"error":"boom"}`)); err != nil {
					t.Fatalf("MarkRetryable: %v", err)
				}
			},
		},
		{
			name: "running",
			setup: func(t *testing.T, d *pgdriver.Driver) {
				if _, err := d.Insert(ctx, driver.InsertParams{
					Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u",
				}); err != nil {
					t.Fatalf("seed: %v", err)
				}
				claimOne(t, d)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d, pool := newDriver(t)
			tt.setup(t, d)
			before := countJobs(t, pool)

			_, err := d.Insert(ctx, driver.InsertParams{
				Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u",
			})
			if !errors.Is(err, driver.ErrDuplicateJob) {
				t.Fatalf("Insert error = %v, want ErrDuplicateJob", err)
			}
			if got := countJobs(t, pool); got != before {
				t.Errorf("store has %d jobs after duplicate, want %d", got, before)
			}
		})
	}
}

func TestInsertReusesUniqueKeyAfterTerminal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name   string
		finish func(*testing.T, *pgdriver.Driver, int64)
	}{
		{
			name: "completed",
			finish: func(t *testing.T, d *pgdriver.Driver, id int64) {
				claimOne(t, d)
				if err := d.MarkCompleted(ctx, held(id)); err != nil {
					t.Fatalf("MarkCompleted: %v", err)
				}
			},
		},
		{
			name: "cancelled",
			finish: func(t *testing.T, d *pgdriver.Driver, id int64) {
				claimOne(t, d)
				if err := d.MarkCancelled(ctx, held(id), []byte(`{"error":"nope"}`)); err != nil {
					t.Fatalf("MarkCancelled: %v", err)
				}
			},
		},
		{
			name: "dead",
			finish: func(t *testing.T, d *pgdriver.Driver, id int64) {
				claimOne(t, d)
				if err := d.MarkDead(ctx, held(id), []byte(`{"error":"boom"}`)); err != nil {
					t.Fatalf("MarkDead: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d, pool := newDriver(t)
			first, err := d.Insert(ctx, driver.InsertParams{
				Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u",
			})
			if err != nil {
				t.Fatalf("first Insert: %v", err)
			}
			tt.finish(t, d, first.ID)

			second, err := d.Insert(ctx, driver.InsertParams{
				Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u",
			})
			if err != nil {
				t.Fatalf("Insert after %s: %v", tt.name, err)
			}
			if second.ID == first.ID {
				t.Errorf("second Insert reused id %d; want a new row", second.ID)
			}
			if second.UniqueKey != "u" {
				t.Errorf("UniqueKey = %q, want u", second.UniqueKey)
			}
			if countJobs(t, pool) != 2 {
				t.Errorf("store has %d jobs, want 2", countJobs(t, pool))
			}
		})
	}
}

func TestInsertUniqueKeyStillUsesDatabaseClockForState(t *testing.T) {
	t.Parallel()
	d, _ := newDriver(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour)

	due, err := d.Insert(ctx, driver.InsertParams{
		Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "due",
	})
	if err != nil {
		t.Fatalf("Insert due: %v", err)
	}
	if due.State != "available" {
		t.Errorf("due State = %q, want available", due.State)
	}

	wait, err := d.Insert(ctx, driver.InsertParams{
		Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "later", ScheduledAt: future,
	})
	if err != nil {
		t.Fatalf("Insert later: %v", err)
	}
	if wait.State != "scheduled" {
		t.Errorf("later State = %q, want scheduled", wait.State)
	}
}

func TestInsertConcurrentUniqueKeyCreatesAtMostOneRow(t *testing.T) {
	t.Parallel()
	d, pool := newDriver(t)
	ctx := context.Background()

	const writers = 2
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := d.Insert(ctx, driver.InsertParams{
				Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u",
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	var ok, dup int
	for err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, driver.ErrDuplicateJob):
			dup++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if ok != 1 || dup != 1 {
		t.Errorf("success=%d duplicate=%d, want 1 and 1", ok, dup)
	}
	if got := countJobs(t, pool); got != 1 {
		t.Errorf("store has %d rows, want 1", got)
	}
}

func TestInsertManyInBatchDuplicateInsertsNothing(t *testing.T) {
	t.Parallel()
	d, pool := newDriver(t)
	ctx := context.Background()
	existing := mustInsert(t, d, "keep", "default")

	_, err := d.InsertMany(ctx, []driver.InsertParams{
		{Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u"},
		{Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u"},
	})
	if !errors.Is(err, driver.ErrDuplicateJob) {
		t.Fatalf("InsertMany error = %v, want ErrDuplicateJob", err)
	}
	if got := countJobs(t, pool); got != 1 {
		t.Fatalf("store has %d jobs, want 1 — the batch must insert zero rows", got)
	}
	if readJob(t, pool, existing.ID).State != "available" {
		t.Error("existing job was disturbed")
	}
}

func TestInsertManyConflictWithExistingInsertsNothing(t *testing.T) {
	t.Parallel()
	d, pool := newDriver(t)
	ctx := context.Background()
	first, err := d.Insert(ctx, driver.InsertParams{
		Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u",
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err = d.InsertMany(ctx, []driver.InsertParams{
		{Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "other"},
		{Kind: "k", Queue: "default", Args: []byte(`{}`), UniqueKey: "u"},
	})
	if !errors.Is(err, driver.ErrDuplicateJob) {
		t.Fatalf("InsertMany error = %v, want ErrDuplicateJob", err)
	}
	if got := countJobs(t, pool); got != 1 {
		t.Fatalf("store has %d jobs, want 1 — the colliding batch must insert zero rows", got)
	}
	if storedUniqueKey(t, pool, first.ID) == nil {
		t.Fatal("existing unique job was removed")
	}
}

func TestInsertManyUniqueKeysUsesCopyFrom(t *testing.T) {
	d, _, trace := newTracedDriver(t)

	rows, err := d.InsertMany(context.Background(), []driver.InsertParams{
		{Kind: "a", Queue: "default", Args: []byte(`{}`), UniqueKey: "one"},
		{Kind: "b", Queue: "default", Args: []byte(`{}`), UniqueKey: "two"},
		{Kind: "c", Queue: "default", Args: []byte(`{}`), UniqueKey: "three"},
	})
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("InsertMany returned %d rows, want 3", len(rows))
	}
	if rows[0].UniqueKey != "one" || rows[1].UniqueKey != "two" || rows[2].UniqueKey != "three" {
		t.Errorf("UniqueKeys = %q,%q,%q, want one,two,three", rows[0].UniqueKey, rows[1].UniqueKey, rows[2].UniqueKey)
	}

	trace.mu.Lock()
	defer trace.mu.Unlock()
	if trace.copyFrom != 1 {
		t.Errorf("CopyFrom called %d times, want 1", trace.copyFrom)
	}
	if trace.insertJob != 0 {
		t.Errorf("InsertJob VALUES ran %d times, want 0 — the batch must not loop single-row inserts", trace.insertJob)
	}
}

func TestPeriodicLeaderHoldsDedicatedConnection(t *testing.T) {
	d, pool := newDriver(t)
	ctx := context.Background()

	baseline := pool.Stat().AcquiredConns()
	ok, err := d.TryBecomeLeader(ctx)
	if err != nil {
		t.Fatalf("TryBecomeLeader: %v", err)
	}
	if !ok {
		t.Fatal("TryBecomeLeader = false, want the first caller to hold the lock")
	}
	if got := pool.Stat().AcquiredConns(); got <= baseline {
		t.Fatalf("acquired conns = %d, want > %d (lock conn must stay checked out)", got, baseline)
	}
	if holders := advisoryLockHolders(t, pool); holders != 1 {
		t.Fatalf("lock holders = %d, want 1", holders)
	}

	other := pgdriver.New(pool)
	ok, err = other.TryBecomeLeader(ctx)
	if err != nil {
		t.Fatalf("second TryBecomeLeader: %v", err)
	}
	if ok {
		t.Fatal("second client took the lock while the first still holds it")
	}

	d.ReleaseLeader()
	if got := pool.Stat().AcquiredConns(); got != baseline {
		t.Fatalf("acquired conns after ReleaseLeader = %d, want %d", got, baseline)
	}
	if holders := advisoryLockHolders(t, pool); holders != 0 {
		t.Fatalf("lock holders after ReleaseLeader = %d, want 0", holders)
	}

	ok, err = other.TryBecomeLeader(ctx)
	if err != nil {
		t.Fatalf("failover TryBecomeLeader: %v", err)
	}
	if !ok {
		t.Fatal("second client could not take the lock after the first released it")
	}
	other.ReleaseLeader()
}

func advisoryLockHolders(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	key := uint64(pgdriver.AdvisoryLockPeriodic)
	var n int
	err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM pg_locks
		WHERE locktype = 'advisory' AND granted AND objsubid = 1
		  AND classid = $1 AND objid = $2
	`, uint32(key>>32), uint32(key)).Scan(&n)
	if err != nil {
		t.Fatalf("count advisory locks: %v", err)
	}
	return n
}
