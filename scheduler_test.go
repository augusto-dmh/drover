package drover

import (
	"context"
	"errors"
	"log/slog"
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

type delayedLocker struct {
	driver.Driver
	ready time.Time
}

func (l *delayedLocker) TryBecomeLeader(context.Context) (bool, error) {
	return !time.Now().Before(l.ready), nil
}

func (l *delayedLocker) ReleaseLeader() {}

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

		sched, err := cron.Parse("@every 1s")
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
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
		wantFire := sched.Next(started)
		fire := rows[0].ScheduledAt
		if !fire.Equal(wantFire) {
			t.Errorf("ScheduledAt = %v, want %v (Next independently of the stored row)", fire, wantFire)
		}
		if !fire.After(started) {
			t.Errorf("first fire %v is not strictly after Start %v", fire, started)
		}
		wantKey := periodicUniqueKey("tick", wantFire)
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

func TestPeriodicLeaderEnqueuesEachRegisteredJob(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	synctest.Test(t, func(t *testing.T) {
		mem := memdriver.New()
		c := newClient(mem, quietPeriodicConfig([]PeriodicJob{
			{ID: "fast", Cron: "@every 1s", Args: pingArgs{}},
			{ID: "slow", Cron: "@every 2s", Args: pingArgs{}},
		}))
		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(2 * time.Second)
		synctest.Wait()

		rows := listMemJobs(t, mem)
		fast, slow := 0, 0
		for _, row := range rows {
			switch {
			case strings.HasPrefix(row.UniqueKey, "fast/"):
				fast++
			case strings.HasPrefix(row.UniqueKey, "slow/"):
				slow++
			default:
				t.Errorf("unexpected UniqueKey %q", row.UniqueKey)
			}
		}
		if fast != 2 || slow != 1 {
			t.Fatalf("fast=%d slow=%d (%d jobs), want 2 and 1", fast, slow, len(rows))
		}

		if err := c.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})
}

func TestPeriodicLeadershipDoesNotReplayTicksFromBeforeTheLock(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	synctest.Test(t, func(t *testing.T) {
		mem := memdriver.New()
		cfg := quietPeriodicConfig([]PeriodicJob{{
			ID:   "tick",
			Cron: "@every 1s",
			Args: pingArgs{},
		}})
		cfg.PollInterval = 100 * time.Millisecond
		locker := &delayedLocker{Driver: mem, ready: time.Now().Add(3 * time.Second)}
		c := newClient(locker, cfg)

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(3 * time.Second)
		synctest.Wait()
		if n := countMemJobs(t, mem); n != 0 {
			t.Fatalf("jobs while waiting for the lock = %d, want 0", n)
		}

		time.Sleep(time.Second)
		synctest.Wait()
		if n := countMemJobs(t, mem); n != 1 {
			t.Fatalf("jobs after one period of leadership = %d, want 1 (not a replay from process start)", n)
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
		cfg.Logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
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
		if !strings.Contains(out, "skipped duplicate periodic tick") {
			t.Errorf("duplicate tick did not log a skip:\n%s", out)
		}
		if strings.Contains(out, "enqueue periodic job") {
			t.Errorf("duplicate tick logged as an enqueue error:\n%s", out)
		}
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

func TestPeriodicUnsatisfiableScheduleDoesNotHangStop(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	c := newClient(memdriver.New(), quietPeriodicConfig([]PeriodicJob{{
		ID:   "never",
		Cron: "0 0 31 2 *",
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
		t.Fatalf("Stop took %v, want well under %v for a schedule with no next fire", elapsed, max)
	}
}
