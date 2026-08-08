package main

import (
	"context"
	"flag"
	"io"

	"github.com/augusto-dmh/drover"
)

func runJobsList(ctx context.Context, in inspector, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("jobs list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	queue := fs.String("queue", "", "filter by queue name")
	state := fs.String("state", "", "filter by job state")
	limit := fs.Int("limit", 0, "max jobs to return (default 100, maximum 1000)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		cliPrintf(stderr, "drover: unexpected arguments %v\n", fs.Args())
		return 2
	}
	if *limit > 1000 {
		cliPrintf(stderr, "drover: --limit %d exceeds maximum 1000\n", *limit)
		return 2
	}

	opts := &drover.ListJobsOpts{
		Queue: *queue,
		Limit: *limit,
	}
	if *state != "" {
		opts.State = drover.JobState(*state)
	}

	jobs, err := in.ListJobs(ctx, opts)
	if err != nil {
		cliPrintf(stderr, "drover: %v\n", err)
		return 1
	}
	if jsonOut {
		if jobs == nil {
			jobs = []*drover.JobRow{}
		}
		if err := writeJSON(stdout, jobs); err != nil {
			cliPrintf(stderr, "drover: %v\n", err)
			return 1
		}
		return 0
	}
	writeJobsHuman(stdout, jobs)
	return 0
}
