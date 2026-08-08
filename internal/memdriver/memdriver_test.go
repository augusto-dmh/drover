package memdriver

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/augusto-dmh/drover/internal/driver"
)

func mustInsert(t *testing.T, d *Driver, kind, queue string) *driver.JobRow {
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

func claimOne(t *testing.T, d *Driver, queue string) *driver.JobRow {
	t.Helper()
	rows, err := d.FetchAvailable(context.Background(), queue, time.Minute, 1)
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
func park(t *testing.T, d *Driver, state string, at time.Time) *driver.JobRow {
	t.Helper()
	row := mustInsert(t, d, "k", "default")
	switch state {
	case "available":
		d.mu.Lock()
		d.jobs[row.ID].ScheduledAt = at
		d.mu.Unlock()
	case "retryable":
		claimOne(t, d, "default")
		if err := d.MarkRetryable(context.Background(), held(row.ID), at, []byte(`{"error":"boom"}`)); err != nil {
			t.Fatalf("MarkRetryable: %v", err)
		}
	case "scheduled":
		claimOne(t, d, "default")
		if err := d.MarkSnoozed(context.Background(), held(row.ID), at); err != nil {
			t.Fatalf("MarkSnoozed: %v", err)
		}
	default:
		t.Fatalf("park: unknown waiting state %q", state)
	}
	parked, ok := d.Row(row.ID)
	if !ok {
		t.Fatal("park: job disappeared")
	}
	return parked
}

// expire forces a claimed job's lease into the past, standing in for the
// worker that died still holding it.
func expire(t *testing.T, d *Driver, id int64) {
	t.Helper()
	past := time.Now().Add(-time.Minute)
	d.mu.Lock()
	defer d.mu.Unlock()
	row, ok := d.jobs[id]
	if !ok {
		t.Fatalf("expire: job %d not found", id)
	}
	row.LeasedUntil = &past
}

func decodeErrors(t *testing.T, row *driver.JobRow) []driver.AttemptError {
	t.Helper()
	var recorded []driver.AttemptError
	if err := json.Unmarshal(row.Errors, &recorded); err != nil {
		t.Fatalf("decode errors %s: %v", row.Errors, err)
	}
	return recorded
}

// held builds the lease a caller holds after claiming a job once. Every
// test here claims at most once, so attempt 1 is what a legitimate
// holder presents.
func held(id int64) driver.Lease { return driver.Lease{ID: id, Attempt: 1} }

// due inserts a job on queue whose scheduled time is at: in the past it
// has been waiting for a worker since then, in the future it is not
// waiting at all. The state is left to Insert's own due-time rule rather
// than asserted by the test.
func due(t *testing.T, d *Driver, queue string, at time.Time) *driver.JobRow {
	t.Helper()
	row, err := d.Insert(context.Background(), driver.InsertParams{
		Kind:        "k",
		Queue:       queue,
		Args:        []byte(`{"n":1}`),
		ScheduledAt: at,
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	return row
}

func TestInsertSetsNewJobDefaults(t *testing.T) {
	t.Parallel()
	d := New()

	row := mustInsert(t, d, "send_email", "default")

	if row.State != "available" {
		t.Errorf("State = %q, want %q", row.State, "available")
	}
	if row.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0", row.Attempt)
	}
	if row.MaxAttempts != 25 {
		t.Errorf("MaxAttempts = %d, want 25", row.MaxAttempts)
	}
	if string(row.Errors) != "[]" {
		t.Errorf("Errors = %s, want []", row.Errors)
	}
	if string(row.Args) != `{"n":1}` {
		t.Errorf("Args = %s, want {\"n\":1}", row.Args)
	}
	if row.Kind != "send_email" || row.Queue != "default" {
		t.Errorf("Kind/Queue = %q/%q, want send_email/default", row.Kind, row.Queue)
	}
	if row.LeasedUntil != nil || row.FinalizedAt != nil {
		t.Errorf("LeasedUntil/FinalizedAt should be unset on insert")
	}
}

func TestInsertTxIsUnsupported(t *testing.T) {
	t.Parallel()
	d := New()

	_, err := d.InsertTx(context.Background(), nil, driver.InsertParams{Kind: "k", Queue: "default"})
	if !errors.Is(err, driver.ErrTxUnsupported) {
		t.Fatalf("InsertTx error = %v, want ErrTxUnsupported", err)
	}
}

func TestFetchAvailableClaimsInFIFOOrderWithLease(t *testing.T) {
	t.Parallel()
	d := New()
	first := mustInsert(t, d, "a", "default")
	second := mustInsert(t, d, "b", "default")

	claimed := claimOne(t, d, "default")

	if claimed.ID != first.ID {
		t.Errorf("claimed job %d, want FIFO first %d", claimed.ID, first.ID)
	}
	if claimed.State != "running" {
		t.Errorf("State = %q, want running", claimed.State)
	}
	if claimed.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1", claimed.Attempt)
	}
	if claimed.LeasedUntil == nil || !claimed.LeasedUntil.After(time.Now()) {
		t.Errorf("LeasedUntil = %v, want a future lease", claimed.LeasedUntil)
	}

	next := claimOne(t, d, "default")
	if next.ID != second.ID {
		t.Errorf("second claim = job %d, want %d", next.ID, second.ID)
	}
}

func TestFetchAvailableFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, d *Driver)
	}{
		{"empty store", func(*testing.T, *Driver) {}},
		{"other queue only", func(t *testing.T, d *Driver) { mustInsert(t, d, "k", "other") }},
		{"already running", func(t *testing.T, d *Driver) {
			mustInsert(t, d, "k", "default")
			claimOne(t, d, "default")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := New()
			tt.setup(t, d)

			rows, err := d.FetchAvailable(context.Background(), "default", time.Minute, 1)
			if err != nil {
				t.Fatalf("FetchAvailable: %v", err)
			}
			if len(rows) != 0 {
				t.Fatalf("claimed %d jobs, want 0", len(rows))
			}
		})
	}
}

func TestMarkCompletedFinalizesRunningJob(t *testing.T) {
	t.Parallel()
	d := New()
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d, "default")

	if err := d.MarkCompleted(context.Background(), held(claimed.ID)); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}

	row, ok := d.Row(claimed.ID)
	if !ok {
		t.Fatal("job disappeared")
	}
	if row.State != "completed" {
		t.Errorf("State = %q, want completed", row.State)
	}
	if row.FinalizedAt == nil {
		t.Error("FinalizedAt not set")
	}
}

func TestMarkDeadAppendsErrorDetail(t *testing.T) {
	t.Parallel()
	d := New()
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d, "default")

	detail, _ := json.Marshal(driver.AttemptError{Attempt: 1, At: time.Now(), Error: "boom"})
	if err := d.MarkDead(context.Background(), held(claimed.ID), detail); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	row, _ := d.Row(claimed.ID)
	if row.State != "dead" {
		t.Errorf("State = %q, want dead", row.State)
	}
	if row.FinalizedAt == nil {
		t.Error("FinalizedAt not set")
	}
	var recorded []driver.AttemptError
	if err := json.Unmarshal(row.Errors, &recorded); err != nil {
		t.Fatalf("decode errors: %v", err)
	}
	if len(recorded) != 1 || recorded[0].Error != "boom" || recorded[0].Attempt != 1 {
		t.Errorf("Errors = %+v, want one entry {Attempt:1 Error:boom}", recorded)
	}
}

func TestFinalizeTransitionGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      func(d *Driver, t *testing.T) int64
		call    func(d *Driver, id int64) error
		wantErr error
	}{
		{
			"complete an available job",
			func(d *Driver, t *testing.T) int64 { return mustInsert(t, d, "k", "default").ID },
			func(d *Driver, id int64) error { return d.MarkCompleted(context.Background(), held(id)) },
			driver.ErrInvalidTransition,
		},
		{
			"kill an available job",
			func(d *Driver, t *testing.T) int64 { return mustInsert(t, d, "k", "default").ID },
			func(d *Driver, id int64) error { return d.MarkDead(context.Background(), held(id), []byte(`{}`)) },
			driver.ErrInvalidTransition,
		},
		{
			"complete a completed job",
			func(d *Driver, t *testing.T) int64 {
				mustInsert(t, d, "k", "default")
				id := claimOne(t, d, "default").ID
				if err := d.MarkCompleted(context.Background(), held(id)); err != nil {
					t.Fatal(err)
				}
				return id
			},
			func(d *Driver, id int64) error { return d.MarkCompleted(context.Background(), held(id)) },
			driver.ErrInvalidTransition,
		},
		{
			"complete an unknown id",
			func(*Driver, *testing.T) int64 { return 999 },
			func(d *Driver, id int64) error { return d.MarkCompleted(context.Background(), held(id)) },
			driver.ErrNotFound,
		},
		{
			"kill an unknown id",
			func(*Driver, *testing.T) int64 { return 999 },
			func(d *Driver, id int64) error { return d.MarkDead(context.Background(), held(id), []byte(`{}`)) },
			driver.ErrNotFound,
		},
		{
			"retry an available job",
			func(d *Driver, t *testing.T) int64 { return mustInsert(t, d, "k", "default").ID },
			func(d *Driver, id int64) error {
				return d.MarkRetryable(context.Background(), held(id), time.Now(), []byte(`{}`))
			},
			driver.ErrInvalidTransition,
		},
		{
			"retry a job already waiting to retry",
			func(d *Driver, t *testing.T) int64 {
				return park(t, d, "retryable", time.Now().Add(time.Hour)).ID
			},
			func(d *Driver, id int64) error {
				return d.MarkRetryable(context.Background(), held(id), time.Now(), []byte(`{}`))
			},
			driver.ErrInvalidTransition,
		},
		{
			"cancel an available job",
			func(d *Driver, t *testing.T) int64 { return mustInsert(t, d, "k", "default").ID },
			func(d *Driver, id int64) error { return d.MarkCancelled(context.Background(), held(id), []byte(`{}`)) },
			driver.ErrInvalidTransition,
		},
		{
			"cancel a completed job",
			func(d *Driver, t *testing.T) int64 {
				mustInsert(t, d, "k", "default")
				id := claimOne(t, d, "default").ID
				if err := d.MarkCompleted(context.Background(), held(id)); err != nil {
					t.Fatal(err)
				}
				return id
			},
			func(d *Driver, id int64) error { return d.MarkCancelled(context.Background(), held(id), []byte(`{}`)) },
			driver.ErrInvalidTransition,
		},
		{
			"snooze an available job",
			func(d *Driver, t *testing.T) int64 { return mustInsert(t, d, "k", "default").ID },
			func(d *Driver, id int64) error { return d.MarkSnoozed(context.Background(), held(id), time.Now()) },
			driver.ErrInvalidTransition,
		},
		{
			"snooze an already snoozed job",
			func(d *Driver, t *testing.T) int64 {
				return park(t, d, "scheduled", time.Now().Add(time.Hour)).ID
			},
			func(d *Driver, id int64) error { return d.MarkSnoozed(context.Background(), held(id), time.Now()) },
			driver.ErrInvalidTransition,
		},
		{
			"retry an unknown id",
			func(*Driver, *testing.T) int64 { return 999 },
			func(d *Driver, id int64) error {
				return d.MarkRetryable(context.Background(), held(id), time.Now(), []byte(`{}`))
			},
			driver.ErrNotFound,
		},
		{
			"cancel an unknown id",
			func(*Driver, *testing.T) int64 { return 999 },
			func(d *Driver, id int64) error { return d.MarkCancelled(context.Background(), held(id), []byte(`{}`)) },
			driver.ErrNotFound,
		},
		{
			"snooze an unknown id",
			func(*Driver, *testing.T) int64 { return 999 },
			func(d *Driver, id int64) error { return d.MarkSnoozed(context.Background(), held(id), time.Now()) },
			driver.ErrNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := New()
			id := tt.id(d, t)

			if err := tt.call(d, id); !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestFetchAvailableClaimsEveryDueWaitingState(t *testing.T) {
	t.Parallel()

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
			t.Parallel()
			d := New()
			parked := park(t, d, tt.state, time.Now().Add(tt.offset))

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
			if len(claimed) != 1 || claimed[0].ID != parked.ID {
				t.Fatalf("claimed %d jobs, want job %d in state %s", len(claimed), parked.ID, tt.state)
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
	t.Parallel()
	d := New()
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d, "default")
	retryAt := time.Now().Add(16 * time.Second).Round(0)

	detail, _ := json.Marshal(driver.AttemptError{Attempt: 1, At: time.Now(), Error: "boom"})
	if err := d.MarkRetryable(context.Background(), held(claimed.ID), retryAt, detail); err != nil {
		t.Fatalf("MarkRetryable: %v", err)
	}

	row, _ := d.Row(claimed.ID)
	if row.State != "retryable" {
		t.Errorf("State = %q, want retryable", row.State)
	}
	if !row.ScheduledAt.Equal(retryAt) {
		t.Errorf("ScheduledAt = %v, want the policy's %v", row.ScheduledAt, retryAt)
	}
	if row.FinalizedAt != nil {
		t.Errorf("FinalizedAt = %v, want unset: a retrying job is not finished", row.FinalizedAt)
	}
	if row.Attempt != claimed.Attempt {
		t.Errorf("Attempt = %d, want %d unchanged by the retry transition", row.Attempt, claimed.Attempt)
	}
	recorded := decodeErrors(t, row)
	if len(recorded) != 1 || recorded[0].Error != "boom" || recorded[0].Attempt != 1 {
		t.Errorf("Errors = %+v, want exactly one entry {Attempt:1 Error:boom}", recorded)
	}
}

func TestMarkCancelledFinalizesWithReason(t *testing.T) {
	t.Parallel()
	d := New()
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d, "default")

	detail, _ := json.Marshal(driver.AttemptError{Attempt: 1, At: time.Now(), Error: "bad input"})
	if err := d.MarkCancelled(context.Background(), held(claimed.ID), detail); err != nil {
		t.Fatalf("MarkCancelled: %v", err)
	}

	row, _ := d.Row(claimed.ID)
	if row.State != "cancelled" {
		t.Errorf("State = %q, want cancelled", row.State)
	}
	if row.FinalizedAt == nil {
		t.Error("FinalizedAt not set on a cancelled job")
	}
	recorded := decodeErrors(t, row)
	if len(recorded) != 1 || recorded[0].Error != "bad input" {
		t.Errorf("Errors = %+v, want exactly one entry recording the cancellation reason", recorded)
	}
}

func TestMarkSnoozedDefersWithoutConsumingAttempt(t *testing.T) {
	t.Parallel()
	d := New()
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d, "default")
	runAt := time.Now().Add(time.Hour).Round(0)

	if err := d.MarkSnoozed(context.Background(), held(claimed.ID), runAt); err != nil {
		t.Fatalf("MarkSnoozed: %v", err)
	}

	row, _ := d.Row(claimed.ID)
	if row.State != "scheduled" {
		t.Errorf("State = %q, want scheduled: a snooze is a deferral, not a failure", row.State)
	}
	if !row.ScheduledAt.Equal(runAt) {
		t.Errorf("ScheduledAt = %v, want the requested wake time %v", row.ScheduledAt, runAt)
	}
	if row.FinalizedAt != nil {
		t.Errorf("FinalizedAt = %v, want unset", row.FinalizedAt)
	}
	if row.Attempt != claimed.Attempt-1 {
		t.Errorf("Attempt = %d, want %d: the snooze gives back the attempt the claim consumed",
			row.Attempt, claimed.Attempt-1)
	}
	if recorded := decodeErrors(t, row); len(recorded) != 0 {
		t.Errorf("Errors = %+v, want none: a snooze records no failure", recorded)
	}
}

func TestMarkSnoozedFloorsAttemptAtZero(t *testing.T) {
	t.Parallel()
	d := New()
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d, "default")
	d.mu.Lock()
	d.jobs[claimed.ID].Attempt = 0
	d.mu.Unlock()

	// The lease names the attempt actually on the row, so this exercises
	// the floor rather than the ownership fence.
	if err := d.MarkSnoozed(context.Background(), driver.Lease{ID: claimed.ID}, time.Now()); err != nil {
		t.Fatalf("MarkSnoozed: %v", err)
	}

	row, _ := d.Row(claimed.ID)
	if row.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0: the decrement is floored, never negative", row.Attempt)
	}
}

func TestTransitionsRefuseAnAttemptTheCallerNoLongerHolds(t *testing.T) {
	t.Parallel()

	transitions := []struct {
		name string
		call func(*Driver, driver.Lease) error
	}{
		{"completed", func(d *Driver, l driver.Lease) error { return d.MarkCompleted(context.Background(), l) }},
		{"dead", func(d *Driver, l driver.Lease) error { return d.MarkDead(context.Background(), l, []byte(`{}`)) }},
		{"cancelled", func(d *Driver, l driver.Lease) error { return d.MarkCancelled(context.Background(), l, []byte(`{}`)) }},
		{"retryable", func(d *Driver, l driver.Lease) error {
			return d.MarkRetryable(context.Background(), l, time.Now(), []byte(`{}`))
		}},
		{"snoozed", func(d *Driver, l driver.Lease) error { return d.MarkSnoozed(context.Background(), l, time.Now()) }},
	}

	for _, tt := range transitions {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := New()
			mustInsert(t, d, "k", "default")

			// A worker claims the job, then stalls long enough for its
			// lease to lapse. The sweep hands the row back and a second
			// worker claims it — the row is running again, on a later
			// attempt, and the first worker is still executing.
			stale := claimOne(t, d, "default")
			expire(t, d, stale.ID)
			if _, err := d.FetchExpired(context.Background(), time.Minute, 1); err != nil {
				t.Fatalf("FetchExpired: %v", err)
			}
			if err := d.MarkRetryable(context.Background(), held(stale.ID), time.Now(), []byte(`{}`)); err != nil {
				t.Fatalf("rescue to retryable: %v", err)
			}
			current := claimOne(t, d, "default")
			if current.Attempt != 2 {
				t.Fatalf("second claim is attempt %d, want 2", current.Attempt)
			}

			// The stale worker finishes and tries to record its outcome.
			// The row is running, so a state-only guard would let this
			// through and overwrite the attempt now in flight.
			err := tt.call(d, driver.Lease{ID: stale.ID, Attempt: stale.Attempt})
			if !errors.Is(err, driver.ErrLeaseLost) {
				t.Fatalf("stale write returned %v, want ErrLeaseLost", err)
			}

			row, _ := d.Row(stale.ID)
			if row.State != "running" {
				t.Errorf("State = %q, want running: the live attempt was disturbed", row.State)
			}
			if row.Attempt != 2 {
				t.Errorf("Attempt = %d, want 2 unchanged", row.Attempt)
			}
			if recorded := decodeErrors(t, row); len(recorded) != 1 {
				t.Errorf("Errors = %+v, want only the rescue entry", recorded)
			}

			// The worker that does hold the job is unaffected.
			if err := d.MarkCompleted(context.Background(), driver.Lease{ID: current.ID, Attempt: current.Attempt}); err != nil {
				t.Errorf("current holder could not finalize: %v", err)
			}
		})
	}
}

func TestExtendLeasesSkipsAnAttemptTheCallerNoLongerHolds(t *testing.T) {
	t.Parallel()
	d := New()
	mustInsert(t, d, "k", "default")
	stale := claimOne(t, d, "default")
	expire(t, d, stale.ID)
	if _, err := d.FetchExpired(context.Background(), time.Millisecond, 1); err != nil {
		t.Fatalf("FetchExpired: %v", err)
	}
	if err := d.MarkRetryable(context.Background(), held(stale.ID), time.Now(), []byte(`{}`)); err != nil {
		t.Fatalf("rescue to retryable: %v", err)
	}
	current := claimOne(t, d, "default")
	beforeLease := *mustRow(t, d, current.ID).LeasedUntil

	// The stale worker's heartbeat is still beating. Renewing here would
	// hand the row's lease back to a worker that no longer owns it,
	// keeping the rescuer away from a job nobody is really running.
	if err := d.ExtendLeases(context.Background(),
		[]driver.Lease{{ID: stale.ID, Attempt: stale.Attempt}}, time.Hour); err != nil {
		t.Fatalf("ExtendLeases: %v, want nil: a lost lease is not a failure", err)
	}

	if got := *mustRow(t, d, current.ID).LeasedUntil; !got.Equal(beforeLease) {
		t.Errorf("LeasedUntil = %v, want %v unchanged by a stale holder's heartbeat", got, beforeLease)
	}
}

func mustRow(t *testing.T, d *Driver, id int64) *driver.JobRow {
	t.Helper()
	row, ok := d.Row(id)
	if !ok {
		t.Fatalf("job %d not found", id)
	}
	return row
}

func TestEveryTransitionClearsTheLease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(d *Driver, id int64) error
	}{
		{"completed", func(d *Driver, id int64) error { return d.MarkCompleted(context.Background(), held(id)) }},
		{"dead", func(d *Driver, id int64) error { return d.MarkDead(context.Background(), held(id), []byte(`{}`)) }},
		{"cancelled", func(d *Driver, id int64) error { return d.MarkCancelled(context.Background(), held(id), []byte(`{}`)) }},
		{"retryable", func(d *Driver, id int64) error {
			return d.MarkRetryable(context.Background(), held(id), time.Now(), []byte(`{}`))
		}},
		{"snoozed", func(d *Driver, id int64) error { return d.MarkSnoozed(context.Background(), held(id), time.Now()) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := New()
			mustInsert(t, d, "k", "default")
			claimed := claimOne(t, d, "default")
			if claimed.LeasedUntil == nil {
				t.Fatal("claim did not write a lease")
			}

			if err := tt.call(d, claimed.ID); err != nil {
				t.Fatalf("transition to %s: %v", tt.name, err)
			}

			row, _ := d.Row(claimed.ID)
			if row.LeasedUntil != nil {
				t.Errorf("LeasedUntil = %v after moving to %s, want nil: no stale lease may survive a transition",
					row.LeasedUntil, tt.name)
			}
		})
	}
}

func TestFetchExpiredReclaimsWithoutTouchingAttempt(t *testing.T) {
	t.Parallel()
	d := New()
	mustInsert(t, d, "k", "default")
	claimed := claimOne(t, d, "default")
	expire(t, d, claimed.ID)
	before := time.Now()

	reclaimed, err := d.FetchExpired(context.Background(), time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchExpired: %v", err)
	}

	if len(reclaimed) != 1 || reclaimed[0].ID != claimed.ID {
		t.Fatalf("reclaimed %d jobs, want the expired job %d", len(reclaimed), claimed.ID)
	}
	if reclaimed[0].Attempt != claimed.Attempt {
		t.Errorf("Attempt = %d, want %d unchanged: a rescue never spends an attempt",
			reclaimed[0].Attempt, claimed.Attempt)
	}
	if reclaimed[0].State != "running" {
		t.Errorf("State = %q, want running: the sweeper now owns the row", reclaimed[0].State)
	}
	row, _ := d.Row(claimed.ID)
	if row.LeasedUntil == nil || row.LeasedUntil.Before(before.Add(time.Minute)) {
		t.Errorf("LeasedUntil = %v, want a fresh lease at least a minute past %v", row.LeasedUntil, before)
	}
	if row.Attempt != claimed.Attempt {
		t.Errorf("stored Attempt = %d, want %d unchanged", row.Attempt, claimed.Attempt)
	}
}

func TestFetchExpiredIgnoresLiveAndUnclaimedRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, d *Driver)
	}{
		{"running with a live lease", func(t *testing.T, d *Driver) {
			mustInsert(t, d, "k", "default")
			claimOne(t, d, "default")
		}},
		{"available, never claimed", func(t *testing.T, d *Driver) {
			mustInsert(t, d, "k", "default")
		}},
		{"already completed", func(t *testing.T, d *Driver) {
			mustInsert(t, d, "k", "default")
			id := claimOne(t, d, "default").ID
			if err := d.MarkCompleted(context.Background(), held(id)); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := New()
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
	t.Parallel()
	d := New()
	mustInsert(t, d, "k", "default")
	running := claimOne(t, d, "default")
	mustInsert(t, d, "k", "default")
	finished := claimOne(t, d, "default")
	if err := d.MarkCompleted(context.Background(), held(finished.ID)); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}
	before := time.Now()

	leases := []driver.Lease{held(running.ID), held(finished.ID), held(999)}
	if err := d.ExtendLeases(context.Background(), leases, 30*time.Minute); err != nil {
		t.Fatalf("ExtendLeases: %v, want nil: finalized and unknown ids are not failures", err)
	}

	row, _ := d.Row(running.ID)
	if row.LeasedUntil == nil || row.LeasedUntil.Before(before.Add(30*time.Minute)) {
		t.Errorf("running job LeasedUntil = %v, want at least 30 minutes past %v", row.LeasedUntil, before)
	}
	done, _ := d.Row(finished.ID)
	if done.State != "completed" {
		t.Errorf("finished job State = %q, want completed: an extension must not resurrect it", done.State)
	}
	if done.LeasedUntil != nil {
		t.Errorf("finished job LeasedUntil = %v, want nil: an extension must not re-lease it", done.LeasedUntil)
	}
}

func TestConcurrentFetchExpiredReclaimsEachJobExactlyOnce(t *testing.T) {
	t.Parallel()
	d := New()
	const jobs = 100
	for range jobs {
		mustInsert(t, d, "k", "default")
	}
	claimed, err := d.FetchAvailable(context.Background(), "default", time.Minute, jobs)
	if err != nil || len(claimed) != jobs {
		t.Fatalf("claim all: %v (%d rows)", err, len(claimed))
	}
	for _, row := range claimed {
		expire(t, d, row.ID)
	}

	var mu sync.Mutex
	seen := make(map[int64]int)
	var wg sync.WaitGroup
	for range 10 {
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

func TestConcurrentFetchClaimsEachJobExactlyOnce(t *testing.T) {
	t.Parallel()
	d := New()
	const jobs = 100
	for range jobs {
		mustInsert(t, d, "k", "default")
	}

	var mu sync.Mutex
	seen := make(map[int64]int)
	var wg sync.WaitGroup
	for range 10 {
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

func TestInsertHonoursScheduledAt(t *testing.T) {
	t.Parallel()

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
			t.Parallel()
			d := New()
			row, err := d.Insert(context.Background(), driver.InsertParams{
				Kind:        "greet",
				Queue:       "default",
				Args:        []byte(`{}`),
				ScheduledAt: tt.at,
			})
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if row.State != tt.wantState {
				t.Errorf("state = %q, want %q", row.State, tt.wantState)
			}
			if !tt.at.IsZero() && !row.ScheduledAt.Equal(tt.at) {
				t.Errorf("ScheduledAt = %v, want %v", row.ScheduledAt, tt.at)
			}

			claimed, err := d.FetchAvailable(context.Background(), "default", time.Minute, 10)
			if err != nil {
				t.Fatalf("FetchAvailable: %v", err)
			}
			if got := len(claimed) == 1; got != tt.claimable {
				t.Errorf("claimed %d job(s), want claimable=%v", len(claimed), tt.claimable)
			}
		})
	}
}

// A scheduled job is claimed once its time arrives, without anything
// having to promote it out of the scheduled state first.
func TestScheduledJobBecomesClaimableWhenItsTimeArrives(t *testing.T) {
	t.Parallel()

	d := New()
	due := time.Now().Add(60 * time.Millisecond)
	row, err := d.Insert(context.Background(), driver.InsertParams{
		Kind: "greet", Queue: "default", Args: []byte(`{}`), ScheduledAt: due,
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if row.State != "scheduled" {
		t.Fatalf("state = %q, want scheduled", row.State)
	}

	claimed, err := d.FetchAvailable(context.Background(), "default", time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed %d job(s) before the scheduled time", len(claimed))
	}

	time.Sleep(time.Until(due) + 20*time.Millisecond)
	claimed, err = d.FetchAvailable(context.Background(), "default", time.Minute, 10)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != row.ID {
		t.Fatalf("claimed %d job(s) after the scheduled time, want job %d", len(claimed), row.ID)
	}
}

func TestStatsCountsOnlyStatesWorthActingOn(t *testing.T) {
	t.Parallel()
	d := New()
	ctx := context.Background()

	// Every job below is claimed while it is the only claimable job on the
	// queue, so each claim takes the row the next line transitions. The
	// unclaimed available job is created last for the same reason.
	dead := due(t, d, "default", time.Now())
	claimOne(t, d, "default")
	if err := d.MarkDead(ctx, held(dead.ID), []byte(`{"error":"boom"}`)); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	completed := due(t, d, "default", time.Now())
	claimOne(t, d, "default")
	if err := d.MarkCompleted(ctx, held(completed.ID)); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}

	cancelled := due(t, d, "default", time.Now())
	claimOne(t, d, "default")
	if err := d.MarkCancelled(ctx, held(cancelled.ID), []byte(`{"error":"nope"}`)); err != nil {
		t.Fatalf("MarkCancelled: %v", err)
	}

	due(t, d, "default", time.Now())
	claimOne(t, d, "default")

	snoozed := due(t, d, "default", time.Now())
	claimOne(t, d, "default")
	if err := d.MarkSnoozed(ctx, held(snoozed.ID), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("MarkSnoozed: %v", err)
	}

	retried := due(t, d, "default", time.Now())
	claimOne(t, d, "default")
	if err := d.MarkRetryable(ctx, held(retried.ID), time.Now().Add(time.Minute),
		[]byte(`{"error":"boom"}`)); err != nil {
		t.Fatalf("MarkRetryable: %v", err)
	}

	due(t, d, "default", time.Now())

	stats, err := d.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	// completed and cancelled are absent by design: they are the states
	// that accumulate forever, and counting them would make the reading
	// cost grow with history instead of with backlog.
	want := []driver.QueueDepth{
		{Queue: "default", State: "available", Count: 1},
		{Queue: "default", State: "dead", Count: 1},
		{Queue: "default", State: "retryable", Count: 1},
		{Queue: "default", State: "running", Count: 1},
		{Queue: "default", State: "scheduled", Count: 1},
	}
	if !slices.Equal(stats.Depths, want) {
		t.Errorf("Depths = %v, want %v", stats.Depths, want)
	}
}

func TestStatsCountsEachQueueSeparately(t *testing.T) {
	t.Parallel()
	d := New()

	due(t, d, "alpha", time.Now())
	due(t, d, "alpha", time.Now())
	due(t, d, "beta", time.Now())

	stats, err := d.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	want := []driver.QueueDepth{
		{Queue: "alpha", State: "available", Count: 2},
		{Queue: "beta", State: "available", Count: 1},
	}
	if !slices.Equal(stats.Depths, want) {
		t.Errorf("Depths = %v, want %v", stats.Depths, want)
	}
}

func TestStatsAgesOnlyJobsClaimableNow(t *testing.T) {
	t.Parallel()

	const waited = 90 * time.Second
	// noAge marks a queue that must not be reported as waiting at all.
	const noAge = -1.0

	tests := []struct {
		name    string
		build   func(t *testing.T, d *Driver)
		wantAge float64
	}{
		{
			name: "a job due in the past has waited since it came due",
			build: func(t *testing.T, d *Driver) {
				due(t, d, "q", time.Now().Add(-waited))
			},
			wantAge: waited.Seconds(),
		},
		{
			name: "a job scheduled for later is not waiting",
			build: func(t *testing.T, d *Driver) {
				due(t, d, "q", time.Now().Add(time.Hour))
			},
			wantAge: noAge,
		},
		{
			name: "an overdue job is reported past a job scheduled for later",
			build: func(t *testing.T, d *Driver) {
				due(t, d, "q", time.Now().Add(time.Hour))
				due(t, d, "q", time.Now().Add(-waited))
			},
			wantAge: waited.Seconds(),
		},
		{
			name: "a retryable job waits from the end of its backoff",
			build: func(t *testing.T, d *Driver) {
				row := due(t, d, "q", time.Now())
				claimOne(t, d, "q")
				if err := d.MarkRetryable(context.Background(), held(row.ID),
					time.Now().Add(-waited), []byte(`{"error":"boom"}`)); err != nil {
					t.Fatalf("MarkRetryable: %v", err)
				}
			},
			wantAge: waited.Seconds(),
		},
		{
			name: "a running job is being worked on, not waiting",
			build: func(t *testing.T, d *Driver) {
				due(t, d, "q", time.Now().Add(-waited))
				claimOne(t, d, "q")
			},
			wantAge: noAge,
		},
		{
			name: "a dead job is never claimable again",
			build: func(t *testing.T, d *Driver) {
				row := due(t, d, "q", time.Now().Add(-waited))
				claimOne(t, d, "q")
				if err := d.MarkDead(context.Background(), held(row.ID),
					[]byte(`{"error":"boom"}`)); err != nil {
					t.Fatalf("MarkDead: %v", err)
				}
			},
			wantAge: noAge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := New()
			tt.build(t, d)

			stats, err := d.Stats(context.Background())
			if err != nil {
				t.Fatalf("Stats: %v", err)
			}
			if tt.wantAge == noAge {
				if len(stats.Oldest) != 0 {
					t.Fatalf("Oldest = %v, want no queue reported as waiting", stats.Oldest)
				}
				return
			}
			if len(stats.Oldest) != 1 || stats.Oldest[0].Queue != "q" {
				t.Fatalf("Oldest = %v, want one entry for queue q", stats.Oldest)
			}
			// The tolerance covers the time the test itself takes, and is
			// far below the hour a job counted from the wrong side of now
			// would be out by.
			if got := stats.Oldest[0].AgeSeconds; math.Abs(got-tt.wantAge) > 2 {
				t.Errorf("AgeSeconds = %v, want %v", got, tt.wantAge)
			}
		})
	}
}

func TestStatsOnAnEmptyStoreReportsNothing(t *testing.T) {
	t.Parallel()

	stats, err := New().Stats(context.Background())
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

	seed := func(t *testing.T) (*Driver, int64) {
		t.Helper()
		d := New()
		mustInsert(t, d, "a", "alpha")
		mustInsert(t, d, "b", "beta")
		deadSeed := mustInsert(t, d, "c", "alpha")
		mustInsert(t, d, "d", "beta")
		rows, err := d.FetchAvailable(ctx, "alpha", time.Minute, 10)
		if err != nil {
			t.Fatalf("FetchAvailable: %v", err)
		}
		var deadID int64
		for _, row := range rows {
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
		return d, deadID
	}

	t.Run("all newest first", func(t *testing.T) {
		t.Parallel()
		d, _ := seed(t)
		rows, err := d.ListJobs(ctx, driver.ListJobsParams{Limit: 100})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(rows) < 2 {
			t.Fatalf("ListJobs returned %d rows, want at least 2", len(rows))
		}
		for i := 1; i < len(rows); i++ {
			if rows[i-1].ID < rows[i].ID {
				t.Fatalf("ids not descending: %d then %d", rows[i-1].ID, rows[i].ID)
			}
		}
	})

	t.Run("queue filter", func(t *testing.T) {
		t.Parallel()
		d, _ := seed(t)
		rows, err := d.ListJobs(ctx, driver.ListJobsParams{Queue: "beta", Limit: 100})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(rows) == 0 {
			t.Fatal("expected at least one beta job")
		}
		for _, row := range rows {
			if row.Queue != "beta" {
				t.Errorf("queue = %q, want beta", row.Queue)
			}
		}
	})

	t.Run("state filter", func(t *testing.T) {
		t.Parallel()
		d, deadID := seed(t)
		rows, err := d.ListJobs(ctx, driver.ListJobsParams{State: "dead", Limit: 100})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(rows) != 1 || rows[0].ID != deadID || rows[0].State != "dead" {
			t.Fatalf("ListJobs dead = %+v, want only id %d", rows, deadID)
		}
	})

	t.Run("limit", func(t *testing.T) {
		t.Parallel()
		d, _ := seed(t)
		rows, err := d.ListJobs(ctx, driver.ListJobsParams{Limit: 1})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("ListJobs limit 1 returned %d rows", len(rows))
		}
		all, err := d.ListJobs(ctx, driver.ListJobsParams{Limit: 100})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if rows[0].ID != all[0].ID {
			t.Errorf("limited row id = %d, want newest %d", rows[0].ID, all[0].ID)
		}
	})
}

func TestGetJobReturnsRowOrNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := New()
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
		setup     func(t *testing.T, d *Driver) int64
		wantState string
		wantErr   error
	}{
		{
			name: "available",
			setup: func(t *testing.T, d *Driver) int64 {
				return mustInsert(t, d, "k", "default").ID
			},
			wantState: "cancelled",
		},
		{
			name: "scheduled",
			setup: func(t *testing.T, d *Driver) int64 {
				return park(t, d, "scheduled", time.Now().Add(time.Hour)).ID
			},
			wantState: "cancelled",
		},
		{
			name: "retryable",
			setup: func(t *testing.T, d *Driver) int64 {
				return park(t, d, "retryable", time.Now().Add(time.Hour)).ID
			},
			wantState: "cancelled",
		},
		{
			name: "dead",
			setup: func(t *testing.T, d *Driver) int64 {
				mustInsert(t, d, "k", "default")
				row := claimOne(t, d, "default")
				if err := d.MarkDead(ctx, held(row.ID), []byte(`{"error":"boom"}`)); err != nil {
					t.Fatal(err)
				}
				return row.ID
			},
			wantState: "cancelled",
		},
		{
			name: "running refused",
			setup: func(t *testing.T, d *Driver) int64 {
				mustInsert(t, d, "k", "default")
				return claimOne(t, d, "default").ID
			},
			wantErr: driver.ErrInvalidTransition,
		},
		{
			name: "completed refused",
			setup: func(t *testing.T, d *Driver) int64 {
				mustInsert(t, d, "k", "default")
				row := claimOne(t, d, "default")
				if err := d.MarkCompleted(ctx, held(row.ID)); err != nil {
					t.Fatal(err)
				}
				return row.ID
			},
			wantErr: driver.ErrInvalidTransition,
		},
		{
			name: "cancelled refused",
			setup: func(t *testing.T, d *Driver) int64 {
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
			setup:   func(*testing.T, *Driver) int64 { return 99999 },
			wantErr: driver.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := New()
			id := tt.setup(t, d)
			before, _ := d.Row(id)

			got, err := d.OperatorCancel(ctx, id)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("OperatorCancel: %v, want %v", err, tt.wantErr)
				}
				if before != nil {
					after, ok := d.Row(id)
					if !ok {
						t.Fatal("job disappeared after refused cancel")
					}
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
			stored, ok := d.Row(id)
			if !ok || stored.State != "cancelled" || stored.FinalizedAt == nil {
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
		d := New()
		mustInsert(t, d, "k", "default")
		row := claimOne(t, d, "default")
		detail := []byte(`{"attempt":1,"error":"boom"}`)
		if err := d.MarkDead(ctx, held(row.ID), detail); err != nil {
			t.Fatalf("MarkDead: %v", err)
		}
		before, ok := d.Row(row.ID)
		if !ok {
			t.Fatal("dead job missing")
		}

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
		if got.ScheduledAt.Before(before.FinalizedAt.Add(-time.Second)) {
			t.Errorf("scheduled_at %v looks stale vs prior finalized %v", got.ScheduledAt, before.FinalizedAt)
		}
	})

	t.Run("non-dead refused", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name  string
			setup func(t *testing.T, d *Driver) int64
		}{
			{"available", func(t *testing.T, d *Driver) int64 {
				return mustInsert(t, d, "k", "default").ID
			}},
			{"running", func(t *testing.T, d *Driver) int64 {
				mustInsert(t, d, "k", "default")
				return claimOne(t, d, "default").ID
			}},
			{"completed", func(t *testing.T, d *Driver) int64 {
				mustInsert(t, d, "k", "default")
				id := claimOne(t, d, "default").ID
				if err := d.MarkCompleted(ctx, held(id)); err != nil {
					t.Fatal(err)
				}
				return id
			}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				d := New()
				id := tt.setup(t, d)
				before, _ := d.Row(id)
				_, err := d.RedriveDead(ctx, id)
				if !errors.Is(err, driver.ErrInvalidTransition) {
					t.Fatalf("RedriveDead: %v, want ErrInvalidTransition", err)
				}
				after, ok := d.Row(id)
				if !ok || after.State != before.State || after.Attempt != before.Attempt {
					t.Errorf("row mutated on refusal: before=%+v after=%+v", before, after)
				}
			})
		}
	})

	t.Run("unknown id", func(t *testing.T) {
		t.Parallel()
		_, err := New().RedriveDead(ctx, 99999)
		if !errors.Is(err, driver.ErrNotFound) {
			t.Errorf("RedriveDead: %v, want ErrNotFound", err)
		}
	})
}
