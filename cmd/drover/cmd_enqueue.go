package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"

	"github.com/augusto-dmh/drover"
)

func runEnqueue(ctx context.Context, in inspector, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	fs.SetOutput(stderr)
	kind := fs.String("kind", "", "job kind (required)")
	queue := fs.String("queue", "", "queue name (default: default)")
	argsJSON := fs.String("args", "{}", "job args as a JSON value")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		cliPrintf(stderr, "drover: unexpected arguments %v\n", fs.Args())
		return 2
	}
	if *kind == "" {
		cliPrintf(stderr, "drover: --kind is required\n")
		return 2
	}
	raw := json.RawMessage(*argsJSON)
	if !json.Valid(raw) {
		cliPrintf(stderr, "drover: --args must be valid JSON\n")
		return 2
	}

	var opts *drover.InsertOpts
	if *queue != "" {
		opts = &drover.InsertOpts{Queue: *queue}
	}
	job, err := in.Enqueue(ctx, *kind, raw, opts)
	if err != nil {
		cliPrintf(stderr, "drover: %v\n", err)
		return 1
	}
	if jsonOut {
		if err := writeJSON(stdout, job); err != nil {
			cliPrintf(stderr, "drover: %v\n", err)
			return 1
		}
		return 0
	}
	cliPrintf(stdout, "enqueued job id=%d kind=%s queue=%s\n", job.ID, job.Kind, job.Queue)
	return 0
}
