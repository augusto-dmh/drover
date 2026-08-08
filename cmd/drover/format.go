package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/augusto-dmh/drover"
)

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

func writeStatsHuman(w io.Writer, stats *drover.QueueStats) {
	cliPrintln(w, "QUEUE\tSTATE\tCOUNT")
	for _, d := range stats.Depths {
		cliPrintf(w, "%s\t%s\t%d\n", d.Queue, d.State, d.Count)
	}
	cliPrintln(w, "QUEUE\tOLDEST_AGE_SECONDS")
	for _, a := range stats.Oldest {
		cliPrintf(w, "%s\t%.3f\n", a.Queue, a.AgeSeconds)
	}
}

func writeJobsHuman(w io.Writer, jobs []*drover.JobRow) {
	cliPrintln(w, "ID\tKIND\tQUEUE\tSTATE\tATTEMPT\tSCHEDULED_AT\tCREATED_AT")
	for _, j := range jobs {
		cliPrintf(w, "%d\t%s\t%s\t%s\t%d\t%s\t%s\n",
			j.ID, j.Kind, j.Queue, j.State, j.Attempt,
			formatTime(j.ScheduledAt), formatTime(j.CreatedAt))
	}
}

func writeJobHuman(w io.Writer, j *drover.JobRow) {
	cliPrintf(w, "id=%d kind=%s queue=%s state=%s attempt=%d/%d\n",
		j.ID, j.Kind, j.Queue, j.State, j.Attempt, j.MaxAttempts)
	cliPrintf(w, "scheduled_at=%s created_at=%s\n",
		formatTime(j.ScheduledAt), formatTime(j.CreatedAt))
	if j.LeasedUntil != nil {
		cliPrintf(w, "leased_until=%s\n", formatTime(*j.LeasedUntil))
	}
	if j.FinalizedAt != nil {
		cliPrintf(w, "finalized_at=%s\n", formatTime(*j.FinalizedAt))
	}
	cliPrintf(w, "args=%s\n", string(j.Args))
	if len(j.Errors) > 0 && string(j.Errors) != "null" {
		cliPrintf(w, "errors=%s\n", string(j.Errors))
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func parseJobID(s string) (int64, error) {
	id, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid job id %q", s)
	}
	return id, nil
}
