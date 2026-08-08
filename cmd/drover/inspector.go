package main

import (
	"context"
	"encoding/json"

	"github.com/augusto-dmh/drover"
)

// inspector is the operator surface the CLI commands call. *drover.Inspector
// satisfies it; tests inject fakes so command behaviour needs no Postgres.
type inspector interface {
	Stats(ctx context.Context) (*drover.QueueStats, error)
	ListJobs(ctx context.Context, opts *drover.ListJobsOpts) ([]*drover.JobRow, error)
	GetJob(ctx context.Context, id int64) (*drover.JobRow, error)
	CancelJob(ctx context.Context, id int64) (*drover.JobRow, error)
	RetryJob(ctx context.Context, id int64) (*drover.JobRow, error)
	Enqueue(ctx context.Context, kind string, args json.RawMessage, opts *drover.InsertOpts) (*drover.JobRow, error)
}
