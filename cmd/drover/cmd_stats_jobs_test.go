package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/augusto-dmh/drover"
)

func TestRunStatsHuman(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		stats: &drover.QueueStats{
			Depths: []drover.QueueDepth{{Queue: "default", State: "available", Count: 3}},
			Oldest: []drover.QueueAge{{Queue: "default", AgeSeconds: 1.5}},
		},
	}
	var stdout, stderr bytes.Buffer
	code := runStats(context.Background(), fake, false, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "default") || !strings.Contains(out, "available") || !strings.Contains(out, "3") {
		t.Fatalf("human stats missing depth row: %q", out)
	}
	if !strings.Contains(out, "1.500") {
		t.Fatalf("human stats missing age: %q", out)
	}
}

func TestRunStatsJSON(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		stats: &drover.QueueStats{
			Depths: []drover.QueueDepth{{Queue: "mail", State: "dead", Count: 2}},
			Oldest: []drover.QueueAge{{Queue: "mail", AgeSeconds: 9}},
		},
	}
	var stdout, stderr bytes.Buffer
	code := runStats(context.Background(), fake, true, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr.String())
	}
	var got drover.QueueStats
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("json: %v body=%q", err, stdout.String())
	}
	if len(got.Depths) != 1 || got.Depths[0].Count != 2 || got.Depths[0].Queue != "mail" {
		t.Fatalf("depths=%v", got.Depths)
	}
	if len(got.Oldest) != 1 || got.Oldest[0].AgeSeconds != 9 {
		t.Fatalf("oldest=%v", got.Oldest)
	}
}

func TestRunStatsErrorExit1(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{statsErr: errors.New("db down")}
	var stdout, stderr bytes.Buffer
	code := runStats(context.Background(), fake, false, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit %d want 1", code)
	}
	if !strings.Contains(stderr.String(), "db down") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestStatsCommandMissingDSNExit2(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run([]string{"stats"}, &stdout, &stderr, func(string) string { return "" }, nil)
	if code != 2 {
		t.Fatalf("exit %d want 2 stderr=%q", code, stderr.String())
	}
}

func TestStatsCommandViaRun(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		stats: &drover.QueueStats{
			Depths: []drover.QueueDepth{{Queue: "default", State: "available", Count: 1}},
		},
	}
	open := func(context.Context, string) (inspector, func(), error) {
		return fake, nil, nil
	}
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"--database", "postgres://test", "--json", "stats"},
		&stdout, &stderr,
		nil, open,
	)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr.String())
	}
	var got drover.QueueStats
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Depths) != 1 {
		t.Fatalf("depths=%v", got.Depths)
	}
}

func TestRunJobsListFiltersAndLimit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	fake := &fakeInspector{
		jobs: []*drover.JobRow{
			{ID: 2, Kind: "email", Queue: "mail", State: drover.StateDead, Attempt: 1, CreatedAt: now, ScheduledAt: now},
			{ID: 1, Kind: "email", Queue: "mail", State: drover.StateDead, Attempt: 0, CreatedAt: now, ScheduledAt: now},
		},
	}
	var stdout, stderr bytes.Buffer
	code := runJobsList(context.Background(), fake, []string{"--queue", "mail", "--state", "dead", "--limit", "5"}, false, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr.String())
	}
	if fake.listOpts == nil {
		t.Fatal("ListJobs not called")
	}
	if fake.listOpts.Queue != "mail" || fake.listOpts.State != drover.StateDead || fake.listOpts.Limit != 5 {
		t.Fatalf("opts=%+v", fake.listOpts)
	}
	out := stdout.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 3 {
		t.Fatalf("want header + 2 rows, got %q", out)
	}
	if !strings.HasPrefix(lines[1], "2\t") {
		t.Fatalf("expected id 2 first data row, got %q", lines[1])
	}
	if !strings.HasPrefix(lines[2], "1\t") {
		t.Fatalf("expected id 1 second data row, got %q", lines[2])
	}
}

func TestRunJobsListJSONEmpty(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{jobs: nil}
	var stdout, stderr bytes.Buffer
	code := runJobsList(context.Background(), fake, nil, true, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr.String())
	}
	body := strings.TrimSpace(stdout.String())
	if body != "[]" {
		t.Fatalf("want [], got %q", body)
	}
}

func TestRunJobsListEmptyHuman(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{jobs: []*drover.JobRow{}}
	var stdout, stderr bytes.Buffer
	code := runJobsList(context.Background(), fake, nil, false, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	lines := strings.Split(strings.TrimRight(stdout.String(), "\n"), "\n")
	if len(lines) != 1 || !strings.Contains(lines[0], "ID") {
		t.Fatalf("want header-only table, got %q", stdout.String())
	}
}

func TestRunJobsListLimitNonPositivePassedThrough(t *testing.T) {
	t.Parallel()
	// CLI passes Limit as given; Inspector treats ≤0 as default 100.
	fake := &fakeInspector{jobs: []*drover.JobRow{}}
	var stdout, stderr bytes.Buffer
	code := runJobsList(context.Background(), fake, []string{"--limit", "0"}, false, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	if fake.listOpts.Limit != 0 {
		t.Fatalf("limit=%d want 0 (Inspector default applies)", fake.listOpts.Limit)
	}
}

func TestJobsListUnknownNestedExit2(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run([]string{"jobs", "explode"}, &stdout, &stderr, nil, nil)
	if code != 2 {
		t.Fatalf("exit %d want 2", code)
	}
}
