package main

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUsageErrorsExit2AndDoNotInsert(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		env  func(string) string
		want string
	}{
		{
			name: "missing database and empty DATABASE_URL",
			args: []string{"--mode", "enqueue"},
			env:  func(string) string { return "" },
			want: "database URL required",
		},
		{
			name: "jobs zero",
			args: []string{"--database", "postgres://bench", "--jobs", "0"},
			want: "--jobs",
		},
		{
			name: "jobs negative",
			args: []string{"--database", "postgres://bench", "--jobs", "-1"},
			want: "--jobs",
		},
		{
			name: "batch zero",
			args: []string{"--database", "postgres://bench", "--batch", "0"},
			want: "--batch",
		},
		{
			name: "batch negative",
			args: []string{"--database", "postgres://bench", "--batch", "-3"},
			want: "--batch",
		},
		{
			name: "mode neither enqueue nor drain",
			args: []string{"--database", "postgres://bench", "--mode", "throughput"},
			want: "--mode",
		},
		{
			name: "empty mode",
			args: []string{"--database", "postgres://bench", "--mode", ""},
			want: "--mode",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var ran atomic.Bool
			exec := func(context.Context, benchConfig) (benchOutcome, error) {
				ran.Store(true)
				return benchOutcome{}, errors.New("must not open postgres")
			}
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr, tt.env, exec)
			if code != 2 {
				t.Fatalf("exit %d, want 2; stderr=%q", code, stderr.String())
			}
			if ran.Load() {
				t.Fatal("execute ran; usage errors must insert nothing")
			}
			if !strings.Contains(stderr.String(), tt.want) {
				t.Fatalf("stderr=%q, want substring %q", stderr.String(), tt.want)
			}
			if !strings.Contains(stderr.String(), "Usage:") {
				t.Fatalf("stderr missing usage: %q", stderr.String())
			}
		})
	}
}

func TestEnqueueMethodologyKeys(t *testing.T) {
	t.Parallel()
	const pgVersion = "PostgreSQL 16.4 on x86_64-pc-linux-gnu (fake)"
	exec := func(_ context.Context, cfg benchConfig) (benchOutcome, error) {
		if cfg.dsn != "postgres://bench" {
			t.Errorf("dsn=%q, want postgres://bench", cfg.dsn)
		}
		if cfg.mode != modeEnqueue {
			t.Errorf("mode=%q, want %s", cfg.mode, modeEnqueue)
		}
		return benchOutcome{
			postgresVersion: pgVersion,
			elapsed:         2 * time.Second,
		}, nil
	}
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{
			"--database", "postgres://bench",
			"--mode", "enqueue",
			"--jobs", "1234",
			"--batch", "56",
			"--concurrency", "7",
		},
		&stdout, &stderr, func(string) string { return "" }, exec,
	)
	if code != 0 {
		t.Fatalf("exit %d, stderr=%q", code, stderr.String())
	}
	out := stdout.String()
	keys := []string{
		"GOOS=" + runtime.GOOS,
		"GOARCH=" + runtime.GOARCH,
		"NumCPU=" + strconv.Itoa(runtime.NumCPU()),
		"postgres_version=" + pgVersion,
		"jobs=1234",
		"batch=56",
		"concurrency=7",
		"handlers=no-op",
		"jobs/sec=617.00",
	}
	for _, key := range keys {
		if !strings.Contains(out, key) {
			t.Errorf("stdout missing %q:\n%s", key, out)
		}
	}
}

func TestDatabaseURLFallbackRuns(t *testing.T) {
	t.Parallel()
	var gotDSN string
	exec := func(_ context.Context, cfg benchConfig) (benchOutcome, error) {
		gotDSN = cfg.dsn
		return benchOutcome{postgresVersion: "PostgreSQL 16.4 (fake)", elapsed: time.Second}, nil
	}
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"--mode", "enqueue", "--jobs", "10"},
		&stdout, &stderr,
		func(k string) string {
			if k == "DATABASE_URL" {
				return "postgres://from-env"
			}
			return ""
		},
		exec,
	)
	if code != 0 {
		t.Fatalf("exit %d, stderr=%q", code, stderr.String())
	}
	if gotDSN != "postgres://from-env" {
		t.Fatalf("dsn=%q, want postgres://from-env", gotDSN)
	}
}

func TestDrainPrintsJobsPerSecAndPercentiles(t *testing.T) {
	t.Parallel()
	lats := make([]time.Duration, 100)
	for i := range lats {
		lats[i] = time.Duration(i+1) * time.Millisecond
	}
	exec := func(context.Context, benchConfig) (benchOutcome, error) {
		return benchOutcome{
			postgresVersion: "PostgreSQL 16.4 (fake)",
			elapsed:         time.Second,
			latencies:       lats,
		}, nil
	}
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"--database", "postgres://bench", "--jobs", "10"},
		&stdout, &stderr, nil, exec,
	)
	if code != 0 {
		t.Fatalf("exit %d, stderr=%q", code, stderr.String())
	}
	out := stdout.String()
	for _, key := range []string{"jobs/sec=10.00", "p50=50ms", "p95=95ms", "p99=99ms", "handlers=no-op"} {
		if !strings.Contains(out, key) {
			t.Errorf("stdout missing %q:\n%s", key, out)
		}
	}
}
