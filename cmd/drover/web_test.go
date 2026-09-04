package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/augusto-dmh/drover"
)

func TestGETStatusPageRendersStatsAndJobs(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		stats: &drover.QueueStats{
			Depths: []drover.QueueDepth{{Queue: "default", State: "available", Count: 3}},
			Oldest: []drover.QueueAge{{Queue: "default", AgeSeconds: 1.5}},
		},
		jobs: []*drover.JobRow{
			{
				ID:          9,
				Kind:        "email",
				Queue:       "default",
				State:       drover.StateDead,
				Attempt:     3,
				MaxAttempts: 25,
				Args:        json.RawMessage(`{"to":"<script>alert(1)</script>"}`),
				Errors:      json.RawMessage(`[{"error":"<b>boom</b>"}]`),
			},
		},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	newStatusHandler(fake, 5*time.Second).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body=%s", rec.Code, rec.Body.String())
	}
	ct := rec.Header().Get("Content-Type")
	if ct != htmlContentType {
		t.Fatalf("Content-Type %q, want %q", ct, htmlContentType)
	}
	csp := rec.Header().Get("Content-Security-Policy")
	if !strings.Contains(csp, "default-src 'none'") {
		t.Fatalf("CSP %q missing default-src 'none'", csp)
	}
	if fake.listOpts == nil || fake.listOpts.State != drover.StateDead || fake.listOpts.Limit != 100 {
		t.Fatalf("ListJobs opts=%+v, want state=dead limit=100", fake.listOpts)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "default") || !strings.Contains(body, "available") || !strings.Contains(body, "3") {
		t.Fatalf("missing depth row: %s", body)
	}
	if !strings.Contains(body, "1.500") {
		t.Fatalf("missing oldest age: %s", body)
	}
	if !strings.Contains(body, "email") || !strings.Contains(body, `action="/jobs/9/retry"`) || !strings.Contains(body, `action="/jobs/9/cancel"`) {
		t.Fatalf("dead job missing retry/cancel: %s", body)
	}
	if strings.Contains(body, `<script>alert(1)</script>`) {
		t.Fatalf("unescaped script in body")
	}
	if !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Fatalf("expected escaped script in args, body=%s", body)
	}
	if strings.Contains(body, `<b>boom</b>`) {
		t.Fatalf("unescaped errors HTML")
	}
	if !strings.Contains(body, `&lt;b&gt;boom&lt;/b&gt;`) {
		t.Fatalf("expected escaped errors, body=%s", body)
	}
}

func TestGETStatusPageButtonsByState(t *testing.T) {
	t.Parallel()
	jobs := []*drover.JobRow{
		{ID: 1, Kind: "a", Queue: "q", State: drover.StateAvailable, Args: json.RawMessage(`{}`)},
		{ID: 2, Kind: "a", Queue: "q", State: drover.StateScheduled, Args: json.RawMessage(`{}`)},
		{ID: 3, Kind: "a", Queue: "q", State: drover.StateRetryable, Args: json.RawMessage(`{}`)},
		{ID: 4, Kind: "a", Queue: "q", State: drover.StateDead, Args: json.RawMessage(`{}`)},
		{ID: 5, Kind: "a", Queue: "q", State: drover.StateRunning, Args: json.RawMessage(`{}`)},
		{ID: 6, Kind: "a", Queue: "q", State: drover.StateCompleted, Args: json.RawMessage(`{}`)},
		{ID: 7, Kind: "a", Queue: "q", State: drover.StateCancelled, Args: json.RawMessage(`{}`)},
	}
	fake := &fakeInspector{stats: &drover.QueueStats{}, jobs: jobs}
	rec := httptest.NewRecorder()
	newStatusHandler(fake, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	for _, id := range []int{1, 2, 3, 4} {
		if !strings.Contains(body, `/jobs/`+strconv.Itoa(id)+`/cancel`) {
			t.Errorf("id %d: want cancel form", id)
		}
	}
	if !strings.Contains(body, `/jobs/4/retry`) {
		t.Error("dead job: want retry form")
	}
	for _, id := range []int{1, 2, 3, 5, 6, 7} {
		if strings.Contains(body, `/jobs/`+strconv.Itoa(id)+`/retry`) {
			t.Errorf("id %d: unexpected retry form", id)
		}
	}
	for _, id := range []int{5, 6, 7} {
		if strings.Contains(body, `/jobs/`+strconv.Itoa(id)+`/cancel`) {
			t.Errorf("id %d: unexpected cancel form", id)
		}
	}
}

func TestGETStatusPageEmpty(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{stats: &drover.QueueStats{}}
	rec := httptest.NewRecorder()
	newStatusHandler(fake, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "No queue depths.") || !strings.Contains(body, "No oldest-claimable ages.") || !strings.Contains(body, "No jobs match.") {
		t.Fatalf("empty captions missing: %s", body)
	}
}

func TestGETStatusPageStatsError500(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{statsErr: errors.New("stats boom <x>")}
	rec := httptest.NewRecorder()
	newStatusHandler(fake, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "<x>") {
		t.Fatalf("unescaped error HTML: %s", body)
	}
	if !strings.Contains(body, "stats boom") || !strings.Contains(body, "&lt;x&gt;") {
		t.Fatalf("escaped error missing: %s", body)
	}
}

func TestGETStatusPageListError500(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{stats: &drover.QueueStats{}, listErr: errors.New("list boom")}
	rec := httptest.NewRecorder()
	newStatusHandler(fake, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "list boom") {
		t.Fatalf("body=%s", rec.Body.String())
	}
}

