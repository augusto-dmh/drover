package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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
			Depths: []drover.QueueDepth{{Queue: "default", State: "available", Count: 42}},
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
	if !strings.Contains(body, "default") || !strings.Contains(body, "available") || !strings.Contains(body, "42") {
		t.Fatalf("missing depth row: %s", body)
	}
	if !strings.Contains(body, "1.500") {
		t.Fatalf("missing oldest age: %s", body)
	}
	if !strings.Contains(body, "email") || !strings.Contains(body, `method="post" action="/jobs/9/retry"`) || !strings.Contains(body, `method="post" action="/jobs/9/cancel"`) {
		t.Fatalf("dead job missing retry/cancel POST forms: %s", body)
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
	if strings.Count(body, `method="post"`) < 5 {
		t.Fatalf("want POST method on retry+cancel forms, got %d: %s", strings.Count(body, `method="post"`), body)
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
		job       *drover.JobRow
	}{
		{name: "defaults", rawQuery: "", wantState: drover.StateDead, wantLimit: 100, job: &drover.JobRow{ID: 11, Kind: "kind-default", Queue: "default", State: drover.StateDead, Args: json.RawMessage(`{}`)}},
		{name: "all states", rawQuery: "state=all", wantState: "", wantLimit: 100, job: &drover.JobRow{ID: 12, Kind: "kind-all", Queue: "q", State: drover.StateRunning, Args: json.RawMessage(`{}`)}},
		{name: "named state", rawQuery: "state=running", wantState: drover.StateRunning, wantLimit: 100, job: &drover.JobRow{ID: 13, Kind: "kind-running", Queue: "q", State: drover.StateRunning, Args: json.RawMessage(`{}`)}},
		{name: "queue and limit", rawQuery: "queue=bulk&state=available&limit=10", wantQueue: "bulk", wantState: drover.StateAvailable, wantLimit: 10, job: &drover.JobRow{ID: 14, Kind: "kind-bulk", Queue: "bulk", State: drover.StateAvailable, Args: json.RawMessage(`{}`)}},
		{name: "limit one", rawQuery: "state=all&limit=1", wantState: "", wantLimit: 1, job: &drover.JobRow{ID: 15, Kind: "kind-lim1", Queue: "q", State: drover.StateDead, Args: json.RawMessage(`{}`)}},
		{name: "limit max", rawQuery: "state=all&limit=1000", wantState: "", wantLimit: 1000, job: &drover.JobRow{ID: 16, Kind: "kind-lim1000", Queue: "q", State: drover.StateDead, Args: json.RawMessage(`{}`)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeInspector{stats: &drover.QueueStats{}, jobs: []*drover.JobRow{tt.job}}
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
			body := rec.Body.String()
			if !strings.Contains(body, tt.job.Kind) {
				t.Fatalf("rendered jobs missing %q: %s", tt.job.Kind, body)
			}
			if strings.Contains(body, "kind-absent") {
				t.Fatalf("rendered a job ListJobs did not return")
			}
		})
	}
}

func TestGETStatusPageFilterForm(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{stats: &drover.QueueStats{}}
	rec := httptest.NewRecorder()
	newStatusHandler(fake, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `method="get"`) {
		t.Fatalf("missing GET filter form: %s", body)
	}
	if !strings.Contains(body, `name="queue"`) || !strings.Contains(body, `name="state"`) || !strings.Contains(body, `name="limit"`) {
		t.Fatalf("filter form missing queue/state/limit fields: %s", body)
	}
	if !strings.Contains(body, `<select name="state"`) {
		t.Fatalf("state field is not a GET select: %s", body)
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
			if u == "/?state=nope" && !strings.Contains(rec.Body.String(), "nope") {
				t.Fatalf("400 body did not name invalid state: %s", rec.Body.String())
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

	t.Run("keeps non-default state and limit", func(t *testing.T) {
		t.Parallel()
		fake := &fakeInspector{stats: &drover.QueueStats{}}
		rec := httptest.NewRecorder()
		newStatusHandler(fake, 5*time.Second).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/?state=running&limit=10", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		body := rec.Body.String()
		if !strings.Contains(body, `content="5;url=`) {
			t.Fatalf("missing 5s refresh: %s", body)
		}
		if !strings.Contains(body, "state=running") || !strings.Contains(body, "limit=10") {
			t.Fatalf("refresh URL dropped state/limit: %s", body)
		}
		if strings.Contains(body, "notice=") || strings.Contains(body, "error=") {
			t.Fatalf("flash leaked into refresh URL: %s", body)
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
		query      string
		want       string
		unwant     string
		omitBanner bool
	}{
		{query: "notice=retried&id=9", want: "redrove job 9"},
		{query: "notice=cancelled&id=4", want: "cancelled job 4"},
		{query: "error=not_found&id=8", want: "job 8 not found"},
		{query: "error=invalid_transition&id=2", want: "job 2 refused the transition"},
		{query: "error=%3Cscript%3E&id=1", unwant: "<script>", omitBanner: true},
		{query: "error=mystery&id=1", unwant: "mystery", omitBanner: true},
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
			if tt.omitBanner && (strings.Contains(body, `role="status"`) || strings.Contains(body, `class="banner`)) {
				t.Fatalf("unknown flash still rendered a banner: %s", body)
			}
		})
	}
}

func TestPOSTRetrySuccess(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		stats:    &drover.QueueStats{},
		retryJob: &drover.JobRow{ID: 9, State: drover.StateAvailable},
	}
	rec := postStatus(newStatusHandler(fake, 0), "/jobs/9/retry", "queue=mail&state=dead&limit=50")
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status %d body=%s", rec.Code, rec.Body.String())
	}
	if fake.retryID != 9 {
		t.Fatalf("retry id=%d", fake.retryID)
	}
	got := locationQuery(t, rec)
	if got.Get("notice") != "retried" || got.Get("id") != "9" || got.Get("queue") != "mail" || got.Get("state") != "dead" || got.Get("limit") != "50" {
		t.Fatalf("Location query %v", got)
	}
}

func TestPOSTCancelSuccess(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		stats:     &drover.QueueStats{},
		cancelJob: &drover.JobRow{ID: 4, State: drover.StateCancelled},
	}
	rec := postStatus(newStatusHandler(fake, 0), "/jobs/4/cancel", "state=all")
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status %d", rec.Code)
	}
	if fake.cancelID != 4 {
		t.Fatalf("cancel id=%d", fake.cancelID)
	}
	got := locationQuery(t, rec)
	if got.Get("notice") != "cancelled" || got.Get("id") != "4" || got.Get("state") != "all" {
		t.Fatalf("Location query %v", got)
	}
}

func TestPOSTRetryNotFoundFlash(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{retryErr: fmt.Errorf("job: %w", drover.ErrNotFound)}
	rec := postStatus(newStatusHandler(fake, 0), "/jobs/99/retry", "")
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status %d", rec.Code)
	}
	if fake.retryID != 99 {
		t.Fatalf("retry id=%d", fake.retryID)
	}
	got := locationQuery(t, rec)
	if got.Get("error") != "not_found" || got.Get("id") != "99" || got.Get("notice") != "" {
		t.Fatalf("Location query %v", got)
	}
}

func TestPOSTCancelInvalidTransitionFlash(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{cancelErr: fmt.Errorf("job: %w", drover.ErrInvalidTransition)}
	rec := postStatus(newStatusHandler(fake, 0), "/jobs/5/cancel", "")
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status %d", rec.Code)
	}
	if fake.cancelID != 5 {
		t.Fatalf("cancel id=%d", fake.cancelID)
	}
	got := locationQuery(t, rec)
	if got.Get("error") != "invalid_transition" || got.Get("id") != "5" {
		t.Fatalf("Location query %v", got)
	}
}

func TestPOSTRetryUnexpectedError500(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{retryErr: errors.New("db down")}
	rec := postStatus(newStatusHandler(fake, 0), "/jobs/1/retry", "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestGETPageRetryFormPostsHiddenFilters(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		stats: &drover.QueueStats{},
		jobs: []*drover.JobRow{
			{ID: 9, Kind: "email", Queue: "mail", State: drover.StateDead, Args: json.RawMessage(`{}`)},
		},
	}
	rec := httptest.NewRecorder()
	newStatusHandler(fake, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/?queue=mail&state=dead&limit=25", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `name="queue" value="mail"`) || !strings.Contains(body, `name="state" value="dead"`) || !strings.Contains(body, `name="limit" value="25"`) {
		t.Fatalf("hidden filters missing: %s", body)
	}
}

func postStatus(h http.Handler, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", "http://"+req.Host)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func locationQuery(t *testing.T, rec *httptest.ResponseRecorder) url.Values {
	t.Helper()
	loc := rec.Header().Get("Location")
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("Location %q: %v", loc, err)
	}
	return u.Query()
}

func TestPOSTCSRFRequiresSameOrigin(t *testing.T) {
	t.Parallel()

	t.Run("no origin or referer", func(t *testing.T) {
		t.Parallel()
		fake := &fakeInspector{retryJob: &drover.JobRow{ID: 1}}
		req := httptest.NewRequest(http.MethodPost, "/jobs/1/retry", nil)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		newStatusHandler(fake, 0).ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status %d", rec.Code)
		}
		if fake.retryID != 0 {
			t.Fatalf("Inspector called, id=%d", fake.retryID)
		}
	})

	t.Run("origin host mismatch", func(t *testing.T) {
		t.Parallel()
		fake := &fakeInspector{retryJob: &drover.JobRow{ID: 1}}
		req := httptest.NewRequest(http.MethodPost, "/jobs/1/retry", nil)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Origin", "http://evil.test")
		rec := httptest.NewRecorder()
		newStatusHandler(fake, 0).ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status %d", rec.Code)
		}
		if fake.retryID != 0 {
			t.Fatalf("Inspector called, id=%d", fake.retryID)
		}
	})

	t.Run("referer host mismatch", func(t *testing.T) {
		t.Parallel()
		fake := &fakeInspector{retryJob: &drover.JobRow{ID: 1}}
		req := httptest.NewRequest(http.MethodPost, "/jobs/1/retry", nil)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Referer", "http://evil.test/")
		rec := httptest.NewRecorder()
		newStatusHandler(fake, 0).ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status %d", rec.Code)
		}
		if fake.retryID != 0 {
			t.Fatalf("Inspector called, id=%d", fake.retryID)
		}
	})

	t.Run("matching referer only", func(t *testing.T) {
		t.Parallel()
		fake := &fakeInspector{retryJob: &drover.JobRow{ID: 7, State: drover.StateAvailable}}
		req := httptest.NewRequest(http.MethodPost, "/jobs/7/retry", nil)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Referer", "http://"+req.Host+"/")
		rec := httptest.NewRecorder()
		newStatusHandler(fake, 0).ServeHTTP(rec, req)
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("status %d body=%s", rec.Code, rec.Body.String())
		}
		if fake.retryID != 7 {
			t.Fatalf("retry id=%d", fake.retryID)
		}
	})
}

func TestPOSTInvalidJobID400(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"/jobs/0/retry", "/jobs/foo/retry", "/jobs/-1/cancel"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			fake := &fakeInspector{retryJob: &drover.JobRow{ID: 1}, cancelJob: &drover.JobRow{ID: 1}}
			rec := postStatus(newStatusHandler(fake, 0), path, "")
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d", rec.Code)
			}
			if fake.retryID != 0 || fake.cancelID != 0 {
				t.Fatalf("Inspector called retry=%d cancel=%d", fake.retryID, fake.cancelID)
			}
		})
	}
}

func TestGETMutationMethodNotAllowed(t *testing.T) {
	t.Parallel()
	h := newStatusHandler(&fakeInspector{}, 0)
	for _, path := range []string{"/jobs/1/retry", "/jobs/1/cancel"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status %d want 405", path, rec.Code)
		}
	}
}

func TestUnknownPath404(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	newStatusHandler(&fakeInspector{}, 0).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != htmlContentType {
		t.Fatalf("Content-Type %q", rec.Header().Get("Content-Type"))
	}
}
