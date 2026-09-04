package main

import (
	"bytes"
	"context"
	"embed"
	"errors"
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
	Depths         []drover.QueueDepth
	Oldest         []drover.QueueAge
	Jobs           []jobView
	Filters        filterView
	RefreshSeconds int
	RefreshURL     string
	Flash          string
	FlashKind      string
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
	mux.HandleFunc("GET /{$}", h.getPage)
	mux.HandleFunc("GET /jobs/{id}/retry", h.methodNotAllowed)
	mux.HandleFunc("GET /jobs/{id}/cancel", h.methodNotAllowed)
	mux.HandleFunc("POST /jobs/{id}/retry", h.postRetry)
	mux.HandleFunc("POST /jobs/{id}/cancel", h.postCancel)
	mux.HandleFunc("/", h.notFound)
	return mux
}

func (h *statusHandler) methodNotAllowed(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Allow", http.MethodPost)
	h.writeHTML(w, http.StatusMethodNotAllowed, "method not allowed")
}

func (h *statusHandler) notFound(w http.ResponseWriter, r *http.Request) {
	h.writeHTML(w, http.StatusNotFound, "not found")
}

func (h *statusHandler) postRetry(w http.ResponseWriter, r *http.Request) {
	h.postMutation(w, r, "retried", h.inspector.RetryJob)
}

func (h *statusHandler) postCancel(w http.ResponseWriter, r *http.Request) {
	h.postMutation(w, r, "cancelled", h.inspector.CancelJob)
}

func (h *statusHandler) postMutation(
	w http.ResponseWriter,
	r *http.Request,
	notice string,
	mutate func(context.Context, int64) (*drover.JobRow, error),
) {
	if !sameOrigin(r) {
		h.writeHTML(w, http.StatusForbidden, "cross-origin request refused")
		return
	}
	id, err := parseJobID(r.PathValue("id"))
	if err != nil {
		h.writeHTML(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := r.ParseForm(); err != nil {
		h.writeHTML(w, http.StatusBadRequest, err.Error())
		return
	}
	_, err = mutate(r.Context(), id)
	if err != nil && !errors.Is(err, drover.ErrNotFound) && !errors.Is(err, drover.ErrInvalidTransition) {
		h.writeHTML(w, http.StatusInternalServerError, err.Error())
		return
	}
	http.Redirect(w, r, mutationRedirect(r.Form, notice, err, id), http.StatusSeeOther)
}

func mutationRedirect(form url.Values, notice string, mutErr error, id int64) string {
	v := url.Values{}
	if q := form.Get("queue"); q != "" {
		v.Set("queue", q)
	}
	if s := form.Get("state"); s != "" {
		v.Set("state", s)
	}
	if lim := form.Get("limit"); lim != "" {
		v.Set("limit", lim)
	}
	v.Set("id", strconv.FormatInt(id, 10))
	switch {
	case mutErr == nil:
		v.Set("notice", notice)
	case errors.Is(mutErr, drover.ErrNotFound):
		v.Set("error", "not_found")
	case errors.Is(mutErr, drover.ErrInvalidTransition):
		v.Set("error", "invalid_transition")
	}
	return "/?" + v.Encode()
}

func sameOrigin(r *http.Request) bool {
	if o := r.Header.Get("Origin"); o != "" {
		u, err := url.Parse(o)
		return err == nil && u.Host == r.Host
	}
	ref := r.Header.Get("Referer")
	if ref == "" {
		return false
	}
	u, err := url.Parse(ref)
	return err == nil && u.Host == r.Host
}

func (h *statusHandler) getPage(w http.ResponseWriter, r *http.Request) {
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
	data.Flash, data.FlashKind = flashFromQuery(r.URL.Query())
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

func flashFromQuery(q url.Values) (text, kind string) {
	id, err := strconv.ParseInt(q.Get("id"), 10, 64)
	if err != nil || id <= 0 {
		return "", ""
	}
	switch q.Get("notice") {
	case "retried":
		return fmt.Sprintf("redrove job %d", id), "notice"
	case "cancelled":
		return fmt.Sprintf("cancelled job %d", id), "notice"
	}
	switch q.Get("error") {
	case "not_found":
		return fmt.Sprintf("job %d not found", id), "error"
	case "invalid_transition":
		return fmt.Sprintf("job %d refused the transition", id), "error"
	}
	return "", ""
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
