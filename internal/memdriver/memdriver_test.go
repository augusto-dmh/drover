package memdriver

import (
	"context"
	"encoding/json"
	"errors"
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
	rows, err := d.FetchAvailable(context.Background(), queue, 1)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("FetchAvailable claimed %d jobs, want 1", len(rows))
	}
	return rows[0]
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

			rows, err := d.FetchAvailable(context.Background(), "default", 1)
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

	if err := d.MarkCompleted(context.Background(), claimed.ID); err != nil {
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
	if err := d.MarkDead(context.Background(), claimed.ID, detail); err != nil {
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
			func(d *Driver, id int64) error { return d.MarkCompleted(context.Background(), id) },
			driver.ErrInvalidTransition,
		},
		{
			"kill an available job",
			func(d *Driver, t *testing.T) int64 { return mustInsert(t, d, "k", "default").ID },
			func(d *Driver, id int64) error { return d.MarkDead(context.Background(), id, []byte(`{}`)) },
			driver.ErrInvalidTransition,
		},
		{
			"complete a completed job",
			func(d *Driver, t *testing.T) int64 {
				mustInsert(t, d, "k", "default")
				id := claimOne(t, d, "default").ID
				if err := d.MarkCompleted(context.Background(), id); err != nil {
					t.Fatal(err)
				}
				return id
			},
			func(d *Driver, id int64) error { return d.MarkCompleted(context.Background(), id) },
			driver.ErrInvalidTransition,
		},
		{
			"complete an unknown id",
			func(*Driver, *testing.T) int64 { return 999 },
			func(d *Driver, id int64) error { return d.MarkCompleted(context.Background(), id) },
			driver.ErrNotFound,
		},
		{
			"kill an unknown id",
			func(*Driver, *testing.T) int64 { return 999 },
			func(d *Driver, id int64) error { return d.MarkDead(context.Background(), id, []byte(`{}`)) },
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
				rows, err := d.FetchAvailable(context.Background(), "default", 1)
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
