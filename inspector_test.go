package drover

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/augusto-dmh/drover/internal/driver"
	"github.com/augusto-dmh/drover/internal/memdriver"
)

func TestInspectorReadyWithoutStart(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	in := newInspector(mem)

	stats, err := in.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats without Start: %v", err)
	}
	if stats == nil {
		t.Fatal("Stats returned nil")
	}
}

func TestInspectorStatsMatchesPublishedStates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := memdriver.New()
	in := newInspector(mem)

	mustEnqueue(t, in, "a", "default", `{}`)
	mustEnqueue(t, in, "b", "default", `{}`)
	_ = claimOne(t, mem, "default") // leave one running
	dead := claimOne(t, mem, "default")
	if err := mem.MarkDead(ctx, driver.Lease{ID: dead.ID, Attempt: dead.Attempt}, []byte(`{"error":"boom"}`)); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	mustEnqueue(t, in, "done", "default", `{}`)
	completed := claimOne(t, mem, "default")
	if err := mem.MarkCompleted(ctx, driver.Lease{ID: completed.ID, Attempt: completed.Attempt}); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}

	cancelled := mustEnqueue(t, in, "cancel-me", "default", `{}`)
	if _, err := mem.OperatorCancel(ctx, cancelled.ID); err != nil {
		t.Fatalf("OperatorCancel: %v", err)
	}

	stats, err := in.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	counts := map[string]int64{}
	for _, d := range stats.Depths {
		if d.Queue != "default" {
			continue
		}
		counts[d.State] = d.Count
		switch d.State {
		case "available", "scheduled", "retryable", "running", "dead":
			// published
		case "completed", "cancelled":
			t.Errorf("Stats included terminal state %q (AD-048 published set excludes it)", d.State)
		default:
			t.Errorf("Stats included unexpected state %q", d.State)
		}
	}
	if counts["running"] < 1 {
		t.Errorf("running count = %d, want ≥1", counts["running"])
	}
	if counts["dead"] < 1 {
		t.Errorf("dead count = %d, want ≥1", counts["dead"])
	}
	if _, ok := counts["completed"]; ok {
		t.Error("completed appeared in depths")
	}
	if _, ok := counts["cancelled"]; ok {
		t.Error("cancelled appeared in depths")
	}
}

func TestInspectorListJobsFiltersOrderAndLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := memdriver.New()
	in := newInspector(mem)

	for i := 0; i < 3; i++ {
		mustEnqueue(t, in, "alpha", "alpha", `{}`)
	}
	mustEnqueue(t, in, "beta", "beta", `{}`)
	dead := claimOne(t, mem, "beta")
	if err := mem.MarkDead(ctx, driver.Lease{ID: dead.ID, Attempt: dead.Attempt}, []byte(`{"error":"x"}`)); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	t.Run("id descending", func(t *testing.T) {
		t.Parallel()
		rows, err := in.ListJobs(ctx, &ListJobsOpts{Limit: 100})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(rows) < 2 {
			t.Fatalf("got %d rows, want ≥2", len(rows))
		}
		for i := 1; i < len(rows); i++ {
			if rows[i-1].ID < rows[i].ID {
				t.Fatalf("ids not descending: %d then %d", rows[i-1].ID, rows[i].ID)
			}
		}
	})

	t.Run("queue filter", func(t *testing.T) {
		t.Parallel()
		rows, err := in.ListJobs(ctx, &ListJobsOpts{Queue: "alpha", Limit: 100})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("got %d alpha rows, want 3", len(rows))
		}
		for _, row := range rows {
			if row.Queue != "alpha" {
				t.Errorf("queue = %q, want alpha", row.Queue)
			}
		}
	})

	t.Run("state filter", func(t *testing.T) {
		t.Parallel()
		rows, err := in.ListJobs(ctx, &ListJobsOpts{State: StateDead, Limit: 100})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(rows) != 1 || rows[0].State != StateDead || rows[0].Queue != "beta" {
			t.Fatalf("ListJobs dead = %+v, want one beta dead job", rows)
		}
	})

	t.Run("explicit limit", func(t *testing.T) {
		t.Parallel()
		rows, err := in.ListJobs(ctx, &ListJobsOpts{Limit: 1})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("got %d rows, want 1", len(rows))
		}
	})

	t.Run("default limit when non-positive", func(t *testing.T) {
		t.Parallel()
		extra := memdriver.New()
		extraIn := newInspector(extra)
		for i := 0; i < 101; i++ {
			mustEnqueue(t, extraIn, "bulk", "default", `{}`)
		}
		for _, opts := range []*ListJobsOpts{nil, {Limit: 0}, {Limit: -1}} {
			rows, err := extraIn.ListJobs(ctx, opts)
			if err != nil {
				t.Fatalf("ListJobs(%v): %v", opts, err)
			}
			if len(rows) != 100 {
				t.Fatalf("ListJobs(%v) returned %d rows, want default cap 100", opts, len(rows))
			}
		}
	})
}

func TestInspectorGetJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := memdriver.New()
	in := newInspector(mem)

	inserted := mustEnqueue(t, in, "greet", "default", `{"name":"ada"}`)

	got, err := in.GetJob(ctx, inserted.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.ID != inserted.ID || got.Kind != "greet" || got.State != StateAvailable {
		t.Errorf("GetJob = %+v, want inserted available greet job", got)
	}
	if string(got.Args) != `{"name":"ada"}` {
		t.Errorf("Args = %s, want {\"name\":\"ada\"}", got.Args)
	}

	_, err = in.GetJob(ctx, 99999)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetJob unknown: %v, want ErrNotFound", err)
	}
}

func TestInspectorEnqueue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("valid kind and JSON", func(t *testing.T) {
		t.Parallel()
		mem := memdriver.New()
		in := newInspector(mem)

		row, err := in.Enqueue(ctx, "notify", json.RawMessage(`{"to":"a@b.c"}`), &InsertOpts{Queue: "mail"})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if row.Kind != "notify" || row.Queue != "mail" || row.State != StateAvailable {
			t.Errorf("Enqueue = %+v, want notify/mail/available", row)
		}
		if string(row.Args) != `{"to":"a@b.c"}` {
			t.Errorf("Args = %s", row.Args)
		}
		stored, ok := mem.Row(row.ID)
		if !ok || stored.Kind != "notify" {
			t.Errorf("stored missing or wrong: %+v", stored)
		}
	})

	t.Run("empty kind refuses without insert", func(t *testing.T) {
		t.Parallel()
		mem := memdriver.New()
		in := newInspector(mem)

		_, err := in.Enqueue(ctx, "", json.RawMessage(`{}`), nil)
		if !errors.Is(err, ErrInvalidKind) {
			t.Fatalf("error = %v, want ErrInvalidKind", err)
		}
		assertInspectorEmpty(t, in)
	})

	t.Run("invalid JSON refuses without insert", func(t *testing.T) {
		t.Parallel()
		mem := memdriver.New()
		in := newInspector(mem)

		_, err := in.Enqueue(ctx, "notify", json.RawMessage(`{`), nil)
		if err == nil {
			t.Fatal("Enqueue succeeded with invalid JSON")
		}
		assertInspectorEmpty(t, in)
	})
}

func TestInspectorCancelJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name      string
		setup     func(t *testing.T, mem *memdriver.Driver, in *Inspector) int64
		wantState JobState
		wantErr   error
	}{
		{
			name: "available",
			setup: func(t *testing.T, _ *memdriver.Driver, in *Inspector) int64 {
				return mustEnqueue(t, in, "k", "default", `{}`).ID
			},
			wantState: StateCancelled,
		},
		{
			name: "scheduled",
			setup: func(t *testing.T, _ *memdriver.Driver, in *Inspector) int64 {
				row, err := in.Enqueue(ctx, "k", json.RawMessage(`{}`), &InsertOpts{
					ScheduledAt: time.Now().Add(time.Hour),
				})
				if err != nil {
					t.Fatalf("Enqueue: %v", err)
				}
				if row.State != StateScheduled {
					t.Fatalf("State = %q, want scheduled", row.State)
				}
				return row.ID
			},
			wantState: StateCancelled,
		},
		{
			name: "retryable",
			setup: func(t *testing.T, mem *memdriver.Driver, in *Inspector) int64 {
				mustEnqueue(t, in, "k", "default", `{}`)
				row := claimOne(t, mem, "default")
				if err := mem.MarkRetryable(ctx, driver.Lease{ID: row.ID, Attempt: row.Attempt},
					time.Now().Add(time.Hour), []byte(`{"error":"later"}`)); err != nil {
					t.Fatalf("MarkRetryable: %v", err)
				}
				return row.ID
			},
			wantState: StateCancelled,
		},
		{
			name: "dead",
			setup: func(t *testing.T, mem *memdriver.Driver, in *Inspector) int64 {
				mustEnqueue(t, in, "k", "default", `{}`)
				row := claimOne(t, mem, "default")
				if err := mem.MarkDead(ctx, driver.Lease{ID: row.ID, Attempt: row.Attempt},
					[]byte(`{"error":"boom"}`)); err != nil {
					t.Fatalf("MarkDead: %v", err)
				}
				return row.ID
			},
			wantState: StateCancelled,
		},
		{
			name: "running refused",
			setup: func(t *testing.T, mem *memdriver.Driver, in *Inspector) int64 {
				mustEnqueue(t, in, "k", "default", `{}`)
				return claimOne(t, mem, "default").ID
			},
			wantErr: ErrInvalidTransition,
		},
		{
			name: "completed refused",
			setup: func(t *testing.T, mem *memdriver.Driver, in *Inspector) int64 {
				mustEnqueue(t, in, "k", "default", `{}`)
				row := claimOne(t, mem, "default")
				if err := mem.MarkCompleted(ctx, driver.Lease{ID: row.ID, Attempt: row.Attempt}); err != nil {
					t.Fatalf("MarkCompleted: %v", err)
				}
				return row.ID
			},
			wantErr: ErrInvalidTransition,
		},
		{
			name: "cancelled refused",
			setup: func(t *testing.T, _ *memdriver.Driver, in *Inspector) int64 {
				id := mustEnqueue(t, in, "k", "default", `{}`).ID
				if _, err := in.CancelJob(ctx, id); err != nil {
					t.Fatalf("first CancelJob: %v", err)
				}
				return id
			},
			wantErr: ErrInvalidTransition,
		},
		{
			name:    "unknown id",
			setup:   func(*testing.T, *memdriver.Driver, *Inspector) int64 { return 99999 },
			wantErr: ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mem := memdriver.New()
			in := newInspector(mem)
			id := tt.setup(t, mem, in)
			before, _ := mem.Row(id)

			got, err := in.CancelJob(ctx, id)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("CancelJob: %v, want %v", err, tt.wantErr)
				}
				if before != nil {
					after, ok := mem.Row(id)
					if !ok {
						t.Fatal("job disappeared after refused cancel")
					}
					if after.State != before.State {
						t.Errorf("state changed to %s on refusal, was %s", after.State, before.State)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("CancelJob: %v", err)
			}
			if got.State != tt.wantState || got.FinalizedAt == nil {
				t.Errorf("CancelJob = state=%s finalized=%v, want %s with finalized_at",
					got.State, got.FinalizedAt, tt.wantState)
			}
			stored, ok := mem.Row(id)
			if !ok || stored.State != "cancelled" || stored.FinalizedAt == nil {
				t.Errorf("stored = %+v, want cancelled with finalized_at", stored)
			}
		})
	}
}

func TestInspectorRetryJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("dead redriven", func(t *testing.T) {
		t.Parallel()
		mem := memdriver.New()
		in := newInspector(mem)
		mustEnqueue(t, in, "k", "default", `{}`)
		row := claimOne(t, mem, "default")
		detail := []byte(`{"attempt":1,"error":"boom"}`)
		if err := mem.MarkDead(ctx, driver.Lease{ID: row.ID, Attempt: row.Attempt}, detail); err != nil {
			t.Fatalf("MarkDead: %v", err)
		}
		before, ok := mem.Row(row.ID)
		if !ok {
			t.Fatal("dead job missing")
		}

		got, err := in.RetryJob(ctx, row.ID)
		if err != nil {
			t.Fatalf("RetryJob: %v", err)
		}
		if got.State != StateAvailable || got.Attempt != 0 || got.LeasedUntil != nil || got.FinalizedAt != nil {
			t.Errorf("RetryJob = %+v, want available attempt=0 cleared lease/finalized", got)
		}
		if string(got.Errors) != string(before.Errors) {
			t.Errorf("errors = %s, want retained %s", got.Errors, before.Errors)
		}
		if !got.ScheduledAt.Before(time.Now().Add(time.Second)) {
			t.Errorf("scheduled_at %v is not at or before now", got.ScheduledAt)
		}
	})

	t.Run("non-dead refused", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name  string
			setup func(t *testing.T, mem *memdriver.Driver, in *Inspector) int64
		}{
			{"available", func(t *testing.T, _ *memdriver.Driver, in *Inspector) int64 {
				return mustEnqueue(t, in, "k", "default", `{}`).ID
			}},
			{"running", func(t *testing.T, mem *memdriver.Driver, in *Inspector) int64 {
				mustEnqueue(t, in, "k", "default", `{}`)
				return claimOne(t, mem, "default").ID
			}},
			{"completed", func(t *testing.T, mem *memdriver.Driver, in *Inspector) int64 {
				mustEnqueue(t, in, "k", "default", `{}`)
				row := claimOne(t, mem, "default")
				if err := mem.MarkCompleted(ctx, driver.Lease{ID: row.ID, Attempt: row.Attempt}); err != nil {
					t.Fatal(err)
				}
				return row.ID
			}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				mem := memdriver.New()
				in := newInspector(mem)
				id := tt.setup(t, mem, in)
				before, _ := mem.Row(id)
				_, err := in.RetryJob(ctx, id)
				if !errors.Is(err, ErrInvalidTransition) {
					t.Fatalf("RetryJob: %v, want ErrInvalidTransition", err)
				}
				after, ok := mem.Row(id)
				if !ok || after.State != before.State || after.Attempt != before.Attempt {
					t.Errorf("row mutated on refusal: before=%+v after=%+v", before, after)
				}
			})
		}
	})

	t.Run("unknown id", func(t *testing.T) {
		t.Parallel()
		_, err := newInspector(memdriver.New()).RetryJob(ctx, 99999)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("RetryJob: %v, want ErrNotFound", err)
		}
	})
}

func mustEnqueue(t *testing.T, in *Inspector, kind, queue, args string) *JobRow {
	t.Helper()
	row, err := in.Enqueue(context.Background(), kind, json.RawMessage(args), &InsertOpts{Queue: queue})
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", kind, err)
	}
	return row
}

func claimOne(t *testing.T, mem *memdriver.Driver, queue string) *driver.JobRow {
	t.Helper()
	rows, err := mem.FetchAvailable(context.Background(), queue, time.Minute, 1)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("FetchAvailable returned %d rows, want 1", len(rows))
	}
	return rows[0]
}

func assertInspectorEmpty(t *testing.T, in *Inspector) {
	t.Helper()
	rows, err := in.ListJobs(context.Background(), &ListJobsOpts{Limit: 100})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("found %d jobs, want 0", len(rows))
	}
}
