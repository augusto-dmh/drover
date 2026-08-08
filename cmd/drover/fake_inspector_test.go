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

	getJob    *drover.JobRow
	getErr    error
	cancelJob *drover.JobRow
	cancelErr error
	retryJob  *drover.JobRow
	retryErr  error

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

func (f *fakeInspector) GetJob(context.Context, int64) (*drover.JobRow, error) {
	return f.getJob, f.getErr
}

func (f *fakeInspector) CancelJob(context.Context, int64) (*drover.JobRow, error) {
	return f.cancelJob, f.cancelErr
}

func (f *fakeInspector) RetryJob(context.Context, int64) (*drover.JobRow, error) {
	return f.retryJob, f.retryErr
}

func (f *fakeInspector) Enqueue(_ context.Context, kind string, args json.RawMessage, opts *drover.InsertOpts) (*drover.JobRow, error) {
	f.enqueued = true
	f.enqueueKind = kind
	f.enqueueArgs = args
	f.enqueueOpts = opts
	return f.enqueueJob, f.enqueueErr
}
