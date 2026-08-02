//go:build integration

package pgdriver_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/augusto-dmh/drover/internal/driver"
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
