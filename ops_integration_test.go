//go:build integration

package drover

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/augusto-dmh/drover/internal/testdb"
)

// /healthz and /readyz exist as two endpoints because they diverge under
// database loss: the process is still alive, but it must leave rotation.
func TestHealthEndpointsDivergeOnDatabaseLoss(t *testing.T) {
	pool := testdb.NewDB(t)
	// After NewDB: ignore the admin/test pools' background goroutines.
	// The sensor here is the client's own Start/Stop lifecycle.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	const statsInterval = 40 * time.Millisecond
	c, err := NewClient(pool, Config{
		Workers:       NewWorkers(),
		PollInterval:  time.Hour,
		StatsInterval: statsInterval,
		Concurrency:   1,
		OpsAddr:       "127.0.0.1:0",
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

	base := opsBaseURL(t, c)

	waitFor(t, func() bool {
		resp, err := http.Get(base + "/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, "readyz to become ready against a healthy database")

	code, _ := getStatus(t, base+"/healthz")
	if code != http.StatusOK {
		t.Fatalf("/healthz = %d against a healthy database, want 200", code)
	}
	code, _ = getStatus(t, base+"/readyz")
	if code != http.StatusOK {
		t.Fatalf("/readyz = %d against a healthy database, want 200", code)
	}

	// Close the pool the client holds: the process lives, but Stats can
	// no longer reach Postgres. That is the outage /readyz exists to
	// report without /healthz asking the orchestrator to restart us.
	pool.Close()

	waitFor(t, func() bool {
		resp, err := http.Get(base + "/readyz")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusServiceUnavailable {
			return false
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		return strings.Contains(string(body), "stale")
	}, "readyz to report stale after the database became unreachable")

	code, _ = getStatus(t, base+"/healthz")
	if code != http.StatusOK {
		t.Fatalf("/healthz = %d after database loss, want 200 — liveness must not consult the store", code)
	}
	code, body := getStatus(t, base+"/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d after database loss, want 503", code)
	}
	if !strings.Contains(body, "stale") {
		t.Fatalf("/readyz body = %q, want it to name staleness", body)
	}
}

func getStatus(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest %s: %v", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body)
}
