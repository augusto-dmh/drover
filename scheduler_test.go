package drover

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.uber.org/goleak"

	"github.com/augusto-dmh/drover/internal/cron"
	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/memdriver"
)

type recordingLocker struct {
	driver.Driver
	tries atomic.Int64
}

func (l *recordingLocker) TryBecomeLeader(context.Context) (bool, error) {
	l.tries.Add(1)
	return true, nil
}

func (l *recordingLocker) ReleaseLeader() {}

type failingLocker struct {
	driver.Driver
}

func (failingLocker) TryBecomeLeader(context.Context) (bool, error) {
	return false, errors.New("lock unavailable")
}

func (failingLocker) ReleaseLeader() {}

func quietPeriodicConfig(jobs []PeriodicJob) Config {
	return Config{
		Logger:         discardLogger(),
		PollInterval:   time.Hour,
		StatsInterval:  time.Hour,
		RescueInterval: time.Hour,
		LeaseDuration:  time.Hour,
		PeriodicJobs:   jobs,
	}
}

func listMemJobs(t *testing.T, mem *memdriver.Driver) []*driver.JobRow {
	t.Helper()
	rows, err := mem.ListJobs(context.Background(), driver.ListJobsParams{Limit: 1000})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	return rows
}

func TestEmptyPeriodicJobsTakesNoLockAndLeaksNothing(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	locker := &recordingLocker{Driver: memdriver.New()}
	c := newClient(locker, quietPeriodicConfig(nil))
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := c.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if n := locker.tries.Load(); n != 0 {
		t.Fatalf("TryBecomeLeader called %d times, want 0", n)
	}
}

func TestPeriodicLeaderEnqueuesOneJobPerTick(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	synctest.Test(t, func(t *testing.T) {
		mem := memdriver.New()
		c := newClient(mem, quietPeriodicConfig([]PeriodicJob{{
			ID:   "tick",
			Cron: "@every 1s",
			Args: pingArgs{},
			Opts: &InsertOpts{UniqueKey: "caller-must-not-win", Queue: "periodic"},
		}}))

		started := time.Now()
		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(time.Second)
		synctest.Wait()

		rows := listMemJobs(t, mem)
		if len(rows) != 1 {
			t.Fatalf("after 1s: %d jobs, want 1", len(rows))
		}
		fire := rows[0].ScheduledAt
		if !fire.After(started) {
			t.Errorf("first fire %v is not strictly after Start %v", fire, started)
		}
		wantKey := periodicUniqueKey("tick", fire)
		if rows[0].UniqueKey != wantKey {
			t.Errorf("UniqueKey = %q, want %q", rows[0].UniqueKey, wantKey)
		}
		if rows[0].UniqueKey == "caller-must-not-win" {
			t.Error("caller UniqueKey was used as-is")
		}
		if rows[0].Queue != "periodic" {
			t.Errorf("Queue = %q, want periodic", rows[0].Queue)
		}
		if rows[0].Kind != "ping" {
			t.Errorf("Kind = %q, want ping", rows[0].Kind)
		}

		time.Sleep(time.Second)
		synctest.Wait()

		rows = listMemJobs(t, mem)
		if len(rows) != 2 {
			t.Fatalf("after 2s: %d jobs, want 2", len(rows))
		}

		if err := c.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})
}

func TestPeriodicDuplicateTickIsNotAHandlerFailure(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	synctest.Test(t, func(t *testing.T) {
		logs := &syncWriter{}
		mem := memdriver.New()
		sched, err := cron.Parse("@every 1s")
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		start := time.Now()
		fire := sched.Next(start)
		key := periodicUniqueKey("tick", fire)

		cfg := quietPeriodicConfig([]PeriodicJob{{
			ID:   "tick",
			Cron: "@every 1s",
			Args: pingArgs{},
		}})
		cfg.Logger = newTestLogger(logs)
		c := newClient(mem, cfg)

		if _, err := c.Insert(context.Background(), pingArgs{}, &InsertOpts{
			UniqueKey:   key,
			ScheduledAt: fire,
		}); err != nil {
			t.Fatalf("pre-insert: %v", err)
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		time.Sleep(time.Second)
		synctest.Wait()

		if n := countMemJobs(t, mem); n != 1 {
			t.Fatalf("jobs = %d, want 1 (duplicate tick must not insert)", n)
		}
		out := logs.String()
		if strings.Contains(out, "job failed") || strings.Contains(out, "job execution failed") {
			t.Errorf("duplicate tick logged as a handler failure:\n%s", out)
		}

		if err := c.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})
}

func TestPeriodicLockFailureDoesNotFailStart(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	logs := &syncWriter{}
	cfg := quietPeriodicConfig([]PeriodicJob{{
		ID:   "tick",
		Cron: "@every 1h",
		Args: pingArgs{},
	}})
	cfg.Logger = newTestLogger(logs)
	c := newClient(&failingLocker{Driver: memdriver.New()}, cfg)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v, want nil when the lock cannot be acquired", err)
	}
	waitFor(t, func() bool {
		return strings.Contains(logs.String(), "acquire periodic scheduler lock")
	}, "the lock acquire failure to be logged")
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPeriodicStopDoesNotWaitForNextFire(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	c := newClient(memdriver.New(), quietPeriodicConfig([]PeriodicJob{{
		ID:   "yearly",
		Cron: "0 0 1 1 *",
		Args: pingArgs{},
	}}))
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	begin := time.Now()
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	elapsed := time.Since(begin)
	const max = 2 * time.Second
	if elapsed >= max {
		t.Fatalf("Stop took %v, want well under the next-fire wait (<%v)", elapsed, max)
	}
}
