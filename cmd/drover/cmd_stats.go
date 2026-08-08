package main

import (
	"context"
	"fmt"
	"io"
)

func runStats(ctx context.Context, in inspector, jsonOut bool, stdout, stderr io.Writer) int {
	stats, err := in.Stats(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "drover: %v\n", err)
		return 1
	}
	if jsonOut {
		if err := writeJSON(stdout, stats); err != nil {
			fmt.Fprintf(stderr, "drover: %v\n", err)
			return 1
		}
		return 0
	}
	writeStatsHuman(stdout, stats)
	return 0
}
