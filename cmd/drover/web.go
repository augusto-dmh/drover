package main

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/augusto-dmh/drover"
)

//go:embed webui/*.html
var webUI embed.FS

const (
	defaultWebListen  = "127.0.0.1:7180"
	defaultWebRefresh = 5 * time.Second
	minWebRefresh     = time.Second
	htmlContentType   = "text/html; charset=utf-8"
	cspStatusPage     = "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
)

var knownJobStates = map[drover.JobState]struct{}{
	drover.StateAvailable: {},
	drover.StateScheduled: {},
	drover.StateRunning:   {},
	drover.StateRetryable: {},
	drover.StateCompleted: {},
	drover.StateCancelled: {},
	drover.StateDead:      {},
}

type statusHandler struct {
	inspector inspector
	refresh   time.Duration
	tmpl      *template.Template
}

type filterView struct {
	Queue string
	State string // form value: "dead", "all", or a JobState
	Limit int
}

type jobView struct {
	Row       *drover.JobRow
	CanRetry  bool
	CanCancel bool
}

type pageData struct {
	Depths          []drover.QueueDepth
	Oldest          []drover.QueueAge
	Jobs            []jobView
	Filters         filterView
	RefreshSeconds  int
	RefreshURL      string
	Flash           string
	FlashKind       string
}

func parseStatusTemplate() *template.Template {
	return template.Must(template.ParseFS(webUI, "webui/page.html"))
}

func newStatusHandler(in inspector, refresh time.Duration) http.Handler {
	h := &statusHandler{
		inspector: in,
		refresh:   refresh,
		tmpl:      parseStatusTemplate(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", h.getPage)
	return mux
}

func (h *statusHandler) getPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	filters, err := parsePageFilters(r.URL.Query())
	if err != nil {
		h.writeHTML(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	stats, err := h.inspector.Stats(ctx)
	if err != nil {
		h.writeHTML(w, http.StatusInternalServerError, err.Error())
		return
	}
	opts := listOptsFromFilters(filters)
	jobs, err := h.inspector.ListJobs(ctx, opts)
	if err != nil {
		h.writeHTML(w, http.StatusInternalServerError, err.Error())
		return
	}

	data := pageData{
		Filters:        filters,
		RefreshSeconds: refreshSeconds(h.refresh),
		RefreshURL:     refreshURL(filters),
	}
	if stats != nil {
		data.Depths = stats.Depths
		data.Oldest = stats.Oldest
	}
	data.Jobs = make([]jobView, 0, len(jobs))
	for _, row := range jobs {
		data.Jobs = append(data.Jobs, viewForJob(row))
	}
	h.writePage(w, http.StatusOK, data)
}

func (h *statusHandler) writePage(w http.ResponseWriter, code int, data pageData) {
	var buf bytes.Buffer
	if err := h.tmpl.Execute(&buf, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", htmlContentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", cspStatusPage)
	w.WriteHeader(code)
	_, _ = buf.WriteTo(w)
}

func (h *statusHandler) writeHTML(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", htmlContentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", cspStatusPage)
	w.WriteHeader(code)
	_, _ = fmt.Fprintf(w, "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>drover</title></head><body><p>%s</p></body></html>", template.HTMLEscapeString(msg))
}

func parsePageFilters(q url.Values) (filterView, error) {
	f := filterView{
		Queue: q.Get("queue"),
		State: q.Get("state"),
		Limit: droverDefaultListLimit(),
	}
	if f.State == "" {
		f.State = string(drover.StateDead)
	}
	if lim := q.Get("limit"); lim != "" {
		n, err := strconv.Atoi(lim)
		if err != nil || n < 1 || n > 1000 {
			return filterView{}, fmt.Errorf("limit must be an integer between 1 and 1000")
		}
		f.Limit = n
	}
	if f.State != "all" {
		if _, ok := knownJobStates[drover.JobState(f.State)]; !ok {
			return filterView{}, fmt.Errorf("invalid job state %q", f.State)
		}
	}
	return f, nil
}

func listOptsFromFilters(f filterView) *drover.ListJobsOpts {
	opts := &drover.ListJobsOpts{Queue: f.Queue, Limit: f.Limit}
	if f.State != "all" {
		opts.State = drover.JobState(f.State)
	}
	return opts
}

func viewForJob(row *drover.JobRow) jobView {
	return jobView{
		Row:       row,
		CanRetry:  row.State == drover.StateDead,
		CanCancel: cancellableState(row.State),
	}
}

func refreshSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(d / time.Second)
}

func refreshURL(f filterView) string {
	v := url.Values{}
	if f.Queue != "" {
		v.Set("queue", f.Queue)
	}
	if f.State != "" && f.State != string(drover.StateDead) {
		v.Set("state", f.State)
	}
	if f.Limit != droverDefaultListLimit() {
		v.Set("limit", strconv.Itoa(f.Limit))
	}
	enc := v.Encode()
	if enc == "" {
		return "/"
	}
	return "/?" + enc
}

func droverDefaultListLimit() int {
	return 100
}

func cancellableState(state drover.JobState) bool {
	switch state {
	case drover.StateAvailable, drover.StateScheduled, drover.StateRetryable, drover.StateDead:
		return true
	default:
		return false
	}
}
