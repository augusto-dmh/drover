package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/augusto-dmh/drover"
	"go.uber.org/goleak"
)

func TestWebJSONExit2(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run([]string{"web", "--json"}, &stdout, &stderr, func(string) string { return "postgres://x" }, nil)
	if code != 2 {
		t.Fatalf("exit %d want 2 stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--json") || !strings.Contains(stderr.String(), "web") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestWebMissingDSNExit2(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run([]string{"web"}, &stdout, &stderr, func(string) string { return "" }, nil)
	if code != 2 {
		t.Fatalf("exit %d want 2 stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "database URL") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestParseWebRefresh(t *testing.T) {
	t.Parallel()
	t.Run("too small", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		_, code := parseWebConfig([]string{"--refresh", "500ms"}, &stderr)
		if code != 2 {
			t.Fatalf("code %d", code)
		}
		if !strings.Contains(stderr.String(), "--refresh") {
			t.Fatalf("stderr=%q", stderr.String())
		}
	})
	t.Run("zero ok", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		cfg, code := parseWebConfig([]string{"--refresh", "0"}, &stderr)
		if code != 0 {
			t.Fatalf("code %d stderr=%q", code, stderr.String())
		}
		if cfg.Refresh != 0 {
			t.Fatalf("refresh=%s", cfg.Refresh)
		}
	})
	t.Run("two seconds ok", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		cfg, code := parseWebConfig([]string{"--refresh", "2s"}, &stderr)
		if code != 0 {
			t.Fatalf("code %d", code)
		}
		if cfg.Refresh != 2*time.Second {
			t.Fatalf("refresh=%s", cfg.Refresh)
		}
	})
	t.Run("default listen", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		cfg, code := parseWebConfig(nil, &stderr)
		if code != 0 {
			t.Fatalf("code %d", code)
		}
		if cfg.Listen != defaultWebListen {
			t.Fatalf("listen=%q", cfg.Listen)
		}
		if cfg.Refresh != defaultWebRefresh {
			t.Fatalf("refresh=%s", cfg.Refresh)
		}
	})
}

func TestRunWebListenGetAndShutdown(t *testing.T) {
	t.Parallel()
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &fakeInspector{stats: &drover.QueueStats{}}
	urlCh := make(chan string, 1)
	errCh := make(chan int, 1)
	go func() {
		errCh <- runWeb(ctx, fake, []string{"--listen", "127.0.0.1:0", "--refresh", "0"}, urlOnceWriter{ch: urlCh}, io.Discard)
	}()

	var base string
	select {
	case base = <-urlCh:
	case code := <-errCh:
		t.Fatalf("runWeb exited %d before printing URL", code)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for listen URL")
	}
	base = strings.TrimSpace(base)
	req, err := http.NewRequest(http.MethodGet, base, nil)
	if err != nil {
		t.Fatalf("NewRequest %s: %v", base, err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", base, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET status %d", res.StatusCode)
	}
	cancel()
	select {
	case code := <-errCh:
		if code != 0 {
			t.Fatalf("runWeb exit %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runWeb did not return after cancel")
	}
}

func TestRunWebBindFailure(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()
	var stderr bytes.Buffer
	code := runWeb(context.Background(), &fakeInspector{}, []string{"--listen", addr}, io.Discard, &stderr)
	if code != 1 {
		t.Fatalf("exit %d want 1 stderr=%q", code, stderr.String())
	}
	if stderr.Len() == 0 {
		t.Fatal("expected bind error on stderr")
	}
}

func TestUsageListsWeb(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	printUsage(&stdout)
	if !strings.Contains(stdout.String(), "web") {
		t.Fatalf("usage missing web: %s", stdout.String())
	}
}

// urlOnceWriter sends the first write to ch so tests can learn the bound URL
// without racing a bytes.Buffer.
type urlOnceWriter struct {
	ch chan string
}

func (w urlOnceWriter) Write(p []byte) (int, error) {
	select {
	case w.ch <- string(p):
	default:
	}
	return len(p), nil
}
