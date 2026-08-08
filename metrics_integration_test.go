//go:build integration

package drover

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/pgdriver"
	"github.com/augusto-dmh/drover/internal/testdb"
)

// The gauges must match what Postgres holds once the refresher has run:
// depths per state, age of claimable work from the database clock, zero
// age for a queue that is only deferred, and a visible dead count.
func TestQueueGaugesMatchPostgres(t *testing.T) {
	pool := testdb.NewDB(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	const (
		overdue = 90 * time.Second
		delay   = time.Hour
	)

	// Seed states the fetch loop will not claim: a dead job, and a queue
	// whose only work is deliberately scheduled for later.
	pg := pgdriver.New(pool)
	due := func(queue string, at time.Time) *driver.JobRow {
		t.Helper()
		row, err := pg.Insert(ctx, driver.InsertParams{
			Kind: "gauge", Queue: queue, Args: []byte(`{"n":1}`), ScheduledAt: at,
		})
		if err != nil {
			t.Fatalf("Insert on %q: %v", queue, err)
		}
		return row
	}
	died := due("work", time.Now())
	claimed, err := pg.FetchAvailable(ctx, "work", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim dead candidate: %v (n=%d)", err, len(claimed))
	}
	if err := pg.MarkDead(ctx, driver.Lease{ID: died.ID, Attempt: claimed[0].Attempt},
		[]byte(`{"error":"boom"}`)); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}
	due("later", time.Now().Add(delay))
	due("later", time.Now().Add(2*delay))

	reg := prometheus.NewRegistry()
	c, err := NewClient(pool, Config{
		Workers:         NewWorkers(),
		PollInterval:    time.Hour,
		StatsInterval:   40 * time.Millisecond,
		MetricsRegistry: reg,
		Concurrency:     1,
		// "work" is deliberately absent: its jobs are inserted through
		// the driver so depths and ages are real, but the fetch loop
		// never visits it and cannot race the assertion by claiming them.
		Queues: map[string]int{
			"later": 1,
			"idle":  1,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(runCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	due("work", time.Now().Add(-overdue))
	due("work", time.Now().Add(-overdue/2))

	waitFor(t, func() bool {
		s, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{
			"queue": "work", "state": "available",
		})
		return ok && s.value == 2
	}, "refresh to see the overdue available jobs")

	dead, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{
		"queue": "work", "state": "dead",
	})
	if !ok || dead.value != 1 {
		t.Errorf("depth{work,dead} = %v present=%v, want 1", dead.value, ok)
	}

	available, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{
		"queue": "work", "state": "available",
	})
	if !ok || available.value != 2 {
		t.Errorf("depth{work,available} = %v present=%v, want 2", available.value, ok)
	}

	age, ok := seriesFor(t, reg, "drover_oldest_job_age_seconds", map[string]string{"queue": "work"})
	if !ok {
		t.Fatal("oldest_job_age{work} missing")
	}
	// Age is the database clock's reading of the oldest claimable job.
	// Allow a few seconds of skew around the overdue we wrote.
	if age.value < overdue.Seconds()-5 || age.value > overdue.Seconds()+5 {
		t.Errorf("oldest_job_age{work} = %v, want about %v", age.value, overdue.Seconds())
	}

	laterAge, ok := seriesFor(t, reg, "drover_oldest_job_age_seconds", map[string]string{"queue": "later"})
	if !ok {
		t.Fatal("oldest_job_age{later} missing")
	}
	if laterAge.value != 0 {
		t.Errorf("oldest_job_age{later} = %v, want 0 — future-scheduled jobs are not late", laterAge.value)
	}

	laterScheduled, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{
		"queue": "later", "state": "scheduled",
	})
	if !ok || laterScheduled.value != 2 {
		t.Errorf("depth{later,scheduled} = %v present=%v, want 2", laterScheduled.value, ok)
	}

	// Configured but idle: every published state and age read zero, not
	// missing — the property that keeps alert expressions defined.
	for _, state := range publishedDepthStates {
		s, ok := seriesFor(t, reg, "drover_queue_depth", map[string]string{
			"queue": "idle", "state": state,
		})
		if !ok || s.value != 0 {
			t.Errorf("depth{idle,%s} = %v present=%v, want 0", state, s.value, ok)
		}
	}
	idleAge, ok := seriesFor(t, reg, "drover_oldest_job_age_seconds", map[string]string{"queue": "idle"})
	if !ok || idleAge.value != 0 {
		t.Errorf("oldest_job_age{idle} = %v present=%v, want 0", idleAge.value, ok)
	}
}
