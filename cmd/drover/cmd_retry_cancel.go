package main

import (
	"context"
	"io"
)

func runRetry(ctx context.Context, in inspector, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		cliPrintf(stderr, "drover: usage: drover retry <id>\n")
		return 2
	}
	id, err := parseJobID(args[0])
	if err != nil {
		cliPrintf(stderr, "drover: %v\n", err)
		return 2
	}
	job, err := in.RetryJob(ctx, id)
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
	cliPrintf(stdout, "redrove job %d to state=%s attempt=%d\n", job.ID, job.State, job.Attempt)
	writeJobHuman(stdout, job)
	return 0
}

func runCancel(ctx context.Context, in inspector, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		cliPrintf(stderr, "drover: usage: drover cancel <id>\n")
		return 2
	}
	id, err := parseJobID(args[0])
	if err != nil {
		cliPrintf(stderr, "drover: %v\n", err)
		return 2
	}
	job, err := in.CancelJob(ctx, id)
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
	cliPrintf(stdout, "cancelled job %d\n", job.ID)
	writeJobHuman(stdout, job)
	return 0
}
