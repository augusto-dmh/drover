package main

import (
	"context"
	"encoding/json"

	"github.com/augusto-dmh/drover"
)

// fakeInspector is a test double for command-level CLI tests.
type fakeInspector struct {
	stats    *drover.QueueStats
	statsErr error

	jobs     []*drover.JobRow
	listOpts *drover.ListJobsOpts
	listErr  error

	cancelID  int64
	cancelJob *drover.JobRow
	cancelErr error

	retryID  int64
	retryJob *drover.JobRow
	retryErr error

	enqueueKind string
	enqueueArgs json.RawMessage
	enqueueOpts *drover.InsertOpts
	enqueueJob  *drover.JobRow
	enqueueErr  error
	enqueued    bool
}

func (f *fakeInspector) Stats(context.Context) (*drover.QueueStats, error) {
	return f.stats, f.statsErr
}

func (f *fakeInspector) ListJobs(_ context.Context, opts *drover.ListJobsOpts) ([]*drover.JobRow, error) {
	f.listOpts = opts
	return f.jobs, f.listErr
}

func (f *fakeInspector) CancelJob(_ context.Context, id int64) (*drover.JobRow, error) {
	f.cancelID = id
	return f.cancelJob, f.cancelErr
}

func (f *fakeInspector) RetryJob(_ context.Context, id int64) (*drover.JobRow, error) {
	f.retryID = id
	return f.retryJob, f.retryErr
}

func (f *fakeInspector) Enqueue(_ context.Context, kind string, args json.RawMessage, opts *drover.InsertOpts) (*drover.JobRow, error) {
	f.enqueued = true
	f.enqueueKind = kind
	f.enqueueArgs = args
	f.enqueueOpts = opts
	return f.enqueueJob, f.enqueueErr
}
