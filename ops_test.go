package drover

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"
)

// startTestOps binds 127.0.0.1:0, builds an ops server, and serves it.
// The returned base URL uses the assigned port; the caller must shut
// the server down.
func startTestOps(t *testing.T, reg *prometheus.Registry, ready func() error) (base string, shutdown func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ops := newOpsServer(ln, reg, ready, newTestLogger(&syncWriter{}))
	go ops.serve()
	base = "http://" + ln.Addr().String()
	return base, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ops.shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body)
}

func TestOpsMetricsServesRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "drover_test_ops_metric",
		Help: "Proves /metrics serves the supplied registry.",
	})
	if err := reg.Register(c); err != nil {
		t.Fatalf("Register: %v", err)
	}
	c.Inc()

	base, shutdown := startTestOps(t, reg, nil)
	defer shutdown()

	code, body := get(t, base+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", code)
	}
	if !strings.Contains(body, "drover_test_ops_metric") {
		t.Fatalf("/metrics body missing registered metric\n%s", body)
	}
	if !strings.Contains(body, "# TYPE drover_test_ops_metric counter") {
		t.Fatalf("/metrics body is not Prometheus text format\n%s", body)
	}
}

func TestOpsHealthzAlwaysOK(t *testing.T) {
	t.Parallel()

	base, shutdown := startTestOps(t, prometheus.NewRegistry(), func() error {
		return errors.New("not ready")
	})
	defer shutdown()

	code, _ := get(t, base+"/healthz")
	if code != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200 even when readiness fails", code)
	}
}

func TestOpsReadyzNilFunctionIsReady(t *testing.T) {
	t.Parallel()

	base, shutdown := startTestOps(t, prometheus.NewRegistry(), nil)
	defer shutdown()

	code, _ := get(t, base+"/readyz")
	if code != http.StatusOK {
		t.Fatalf("/readyz status = %d, want 200 when ready fn is nil", code)
	}
}

func TestOpsReadyzReflectsReadyFunction(t *testing.T) {
	t.Parallel()

	readyErr := errors.New("database unreachable")
	var fail atomic.Bool

	base, shutdown := startTestOps(t, prometheus.NewRegistry(), func() error {
		if fail.Load() {
			return readyErr
		}
		return nil
	})
	defer shutdown()

	code, _ := get(t, base+"/readyz")
	if code != http.StatusOK {
		t.Fatalf("/readyz status = %d when ready returns nil, want 200", code)
	}

	fail.Store(true)
	code, body := get(t, base+"/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz status = %d when ready returns error, want 503", code)
	}
	if !strings.Contains(body, readyErr.Error()) {
		t.Fatalf("/readyz body = %q, want it to contain %q", body, readyErr.Error())
	}
}

func TestOpsUnknownPathIs404(t *testing.T) {
	t.Parallel()

	base, shutdown := startTestOps(t, prometheus.NewRegistry(), nil)
	defer shutdown()

	code, _ := get(t, base+"/nope")
	if code != http.StatusNotFound {
		t.Fatalf("unknown path status = %d, want 404", code)
	}
}

// Shutdown must join the Serve goroutine. http.Server.Shutdown alone
// does not; without waiting on done, goleak would catch the leak.
func TestOpsShutdownJoinsServe(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ops := newOpsServer(ln, prometheus.NewRegistry(), nil, newTestLogger(&syncWriter{}))
	go ops.serve()

	// Prove the server is up before shutting it down.
	base := "http://" + ln.Addr().String()
	waitFor(t, func() bool {
		resp, err := http.Get(base + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, "ops server to accept")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ops.shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
