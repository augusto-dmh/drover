package main

import (
	"context"
	"fmt"
	"io"
)

func runRetry(ctx context.Context, in inspector, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintf(stderr, "drover: usage: drover retry <id>\n")
		return 2
	}
	id, err := parseJobID(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "drover: %v\n", err)
		return 2
	}
	job, err := in.RetryJob(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "drover: %v\n", err)
		return 1
	}
	if jsonOut {
		if err := writeJSON(stdout, job); err != nil {
			fmt.Fprintf(stderr, "drover: %v\n", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "redrove job %d to state=%s attempt=%d\n", job.ID, job.State, job.Attempt)
	writeJobHuman(stdout, job)
	return 0
}

func runCancel(ctx context.Context, in inspector, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintf(stderr, "drover: usage: drover cancel <id>\n")
		return 2
	}
	id, err := parseJobID(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "drover: %v\n", err)
		return 2
	}
	job, err := in.CancelJob(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "drover: %v\n", err)
		return 1
	}
	if jsonOut {
		if err := writeJSON(stdout, job); err != nil {
			fmt.Fprintf(stderr, "drover: %v\n", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "cancelled job %d\n", job.ID)
	writeJobHuman(stdout, job)
	return 0
}
