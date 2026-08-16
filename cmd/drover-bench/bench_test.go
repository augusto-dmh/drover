package main

import (
	"context"
	"testing"
	"time"

	"github.com/augusto-dmh/drover"
)

type fakeInserter struct {
	sizes []int
	opts  []*drover.InsertOpts
	kinds []string
	next  int64
}

func (f *fakeInserter) InsertMany(_ context.Context, items []drover.InsertItem) ([]*drover.JobRow, error) {
	f.sizes = append(f.sizes, len(items))
	rows := make([]*drover.JobRow, len(items))
	for i, item := range items {
		f.kinds = append(f.kinds, item.Args.Kind())
		f.opts = append(f.opts, item.Opts)
		f.next++
		rows[i] = &drover.JobRow{ID: f.next}
	}
	return rows, nil
}

func TestInsertManyChunksOfBatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		jobs  int
		batch int
		queue string
		want  []int
	}{
		{name: "uneven last chunk", jobs: 10, batch: 3, queue: "default", want: []int{3, 3, 3, 1}},
		{name: "exact batches", jobs: 8, batch: 4, queue: "default", want: []int{4, 4}},
		{name: "batch larger than jobs", jobs: 5, batch: 256, queue: "mail", want: []int{5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ins := &fakeInserter{}
			insertedAt := make(map[int64]time.Time)
			rows, err := insertNoopJobs(context.Background(), ins, tt.jobs, tt.batch, tt.queue, insertedAt)
			if err != nil {
				t.Fatalf("insertNoopJobs: %v", err)
			}
			if len(rows) != tt.jobs {
				t.Fatalf("inserted %d rows, want %d", len(rows), tt.jobs)
			}
			if len(ins.sizes) != len(tt.want) {
				t.Fatalf("InsertMany called %d times with sizes %v, want %v", len(ins.sizes), ins.sizes, tt.want)
			}
			for i, got := range ins.sizes {
				if got != tt.want[i] {
					t.Fatalf("chunk %d size %d, want %d (all %v)", i, got, tt.want[i], ins.sizes)
				}
			}
			for i, kind := range ins.kinds {
				if kind != noopKind {
					t.Errorf("item %d kind=%q, want %q", i, kind, noopKind)
				}
			}
			if tt.queue != "" && tt.queue != "default" {
				for i, opts := range ins.opts {
					if opts == nil || opts.Queue != tt.queue {
						t.Errorf("item %d opts=%v, want queue %q", i, opts, tt.queue)
					}
				}
			}
			if len(insertedAt) != tt.jobs {
				t.Fatalf("recorded %d insert times, want %d", len(insertedAt), tt.jobs)
			}
		})
	}
}

func TestNoopWorkerSignalsWhenAllJobsComplete(t *testing.T) {
	t.Parallel()
	start := time.Now().Add(-20 * time.Millisecond)
	insertedAt := map[int64]time.Time{1: start, 2: start}
	w := newNoopWorker(2, insertedAt)

	if err := w.Work(context.Background(), &drover.Job[noopArgs]{ID: 1}); err != nil {
		t.Fatalf("Work(1): %v", err)
	}
	select {
	case <-w.allDone:
		t.Fatal("signaled complete after 1 of 2 jobs")
	default:
	}

	if err := w.Work(context.Background(), &drover.Job[noopArgs]{ID: 2}); err != nil {
		t.Fatalf("Work(2): %v", err)
	}
	select {
	case <-w.allDone:
	default:
		t.Fatal("want complete after every job returned")
	}

	if len(w.latencies) != 2 {
		t.Fatalf("recorded %d latencies, want 2", len(w.latencies))
	}
	for i, lat := range w.latencies {
		if lat < 20*time.Millisecond {
			t.Errorf("latency[%d]=%s, want enqueue-to-completion from insert time (>=20ms)", i, lat)
		}
	}
}
