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

func TestGETStatusPageFilters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		rawQuery  string
		wantQueue string
		wantState drover.JobState
		wantLimit int
	}{
		{name: "defaults", rawQuery: "", wantState: drover.StateDead, wantLimit: 100},
		{name: "all states", rawQuery: "state=all", wantState: "", wantLimit: 100},
		{name: "named state", rawQuery: "state=running", wantState: drover.StateRunning, wantLimit: 100},
		{name: "queue and limit", rawQuery: "queue=bulk&state=available&limit=10", wantQueue: "bulk", wantState: drover.StateAvailable, wantLimit: 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeInspector{stats: &drover.QueueStats{}}
			u := "/"
			if tt.rawQuery != "" {
				u = "/?" + tt.rawQuery
			}
			rec := httptest.NewRecorder()
			newStatusHandler(fake, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, u, nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d body=%s", rec.Code, rec.Body.String())
			}
			if fake.listOpts == nil {
				t.Fatal("ListJobs not called")
			}
			if fake.listOpts.Queue != tt.wantQueue || fake.listOpts.State != tt.wantState || fake.listOpts.Limit != tt.wantLimit {
				t.Fatalf("opts=%+v", fake.listOpts)
			}
		})
	}
}

func TestGETStatusPageFilterValidation400(t *testing.T) {
	t.Parallel()
	tests := []string{
		"/?state=nope",
		"/?limit=0",
		"/?limit=1001",
		"/?limit=-1",
		"/?limit=foo",
	}
	for _, u := range tests {
		t.Run(u, func(t *testing.T) {
			t.Parallel()
			fake := &fakeInspector{stats: &drover.QueueStats{}}
			rec := httptest.NewRecorder()
			newStatusHandler(fake, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, u, nil))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d want 400 body=%s", rec.Code, rec.Body.String())
			}
			if fake.listOpts != nil {
				t.Fatalf("ListJobs called on invalid query: %+v", fake.listOpts)
			}
		})
	}
}

func TestGETStatusPageMetaRefresh(t *testing.T) {
	t.Parallel()

	t.Run("default 5s without flash", func(t *testing.T) {
		t.Parallel()
		fake := &fakeInspector{stats: &drover.QueueStats{}}
		rec := httptest.NewRecorder()
		newStatusHandler(fake, 5*time.Second).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/?notice=retried&id=9&queue=mail", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		body := rec.Body.String()
		if !strings.Contains(body, `http-equiv="refresh"`) {
			t.Fatalf("missing meta refresh: %s", body)
		}
		if !strings.Contains(body, `content="5;url=`) {
			t.Fatalf("missing 5s refresh: %s", body)
		}
		if strings.Contains(body, "notice=") || strings.Contains(body, "error=") {
			t.Fatalf("flash leaked into refresh URL: %s", body)
		}
		if !strings.Contains(body, "queue=mail") {
			t.Fatalf("refresh URL dropped queue filter: %s", body)
		}
	})

	t.Run("refresh disabled", func(t *testing.T) {
		t.Parallel()
		fake := &fakeInspector{stats: &drover.QueueStats{}}
		rec := httptest.NewRecorder()
		newStatusHandler(fake, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if strings.Contains(rec.Body.String(), `http-equiv="refresh"`) {
			t.Fatalf("unexpected meta refresh: %s", rec.Body.String())
		}
	})
}

func TestGETStatusPageFlash(t *testing.T) {
	t.Parallel()
	tests := []struct {
		query  string
		want   string
		unwant string
	}{
		{query: "notice=retried&id=9", want: "redrove job 9"},
		{query: "notice=cancelled&id=4", want: "cancelled job 4"},
		{query: "error=not_found&id=8", want: "job 8 not found"},
		{query: "error=invalid_transition&id=2", want: "job 2 refused the transition"},
		{query: "error=%3Cscript%3E&id=1", unwant: "<script>"},
		{query: "notice=retried", unwant: "redrove job"},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()
			fake := &fakeInspector{stats: &drover.QueueStats{}}
			rec := httptest.NewRecorder()
			newStatusHandler(fake, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/?"+tt.query, nil))
			body := rec.Body.String()
			if tt.want != "" && !strings.Contains(body, tt.want) {
				t.Fatalf("want %q in %s", tt.want, body)
			}
			if tt.unwant != "" && strings.Contains(body, tt.unwant) {
				t.Fatalf("did not want %q in %s", tt.unwant, body)
			}
		})
	}
}
