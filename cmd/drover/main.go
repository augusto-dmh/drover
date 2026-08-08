// Command drover is the operator CLI for a Drover queue.
package main

import (
	"fmt"
	"io"
	"os"
)

// version is set at link time by GoReleaser (-X main.version=…).
// Local builds keep the default.
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}

func run(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	cfg, rest, err := peelGlobals(args)
	if err != nil {
		fmt.Fprintf(stderr, "drover: %v\n", err)
		return 2
	}
	if cfg.help {
		printUsage(stdout)
		return 0
	}
	if cfg.version {
		fmt.Fprintln(stdout, version)
		return 0
	}
	if len(rest) == 0 {
		printUsage(stderr)
		return 2
	}

	switch {
	case rest[0] == "version":
		fmt.Fprintln(stdout, version)
		return 0
	default:
		fmt.Fprintf(stderr, "drover: unknown command %q\n\n", rest[0])
		printUsage(stderr)
		return 2
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, `Usage: drover [global flags] <command> [command flags]

Commands:
  stats              Print per-queue depth and oldest-claimable age
  jobs list          List jobs (optional --queue, --state, --limit)
  retry <id>         Redrive a dead job to available
  cancel <id>        Cancel a waiting or dead job
  enqueue            Insert a job (--kind required; --queue, --args optional)
  version            Print the drover version

Global flags:
  --database URL     Postgres URL (default: $DATABASE_URL)
  --json             Machine-readable JSON output
  --version          Print the drover version
  -h, --help         Show this help
`)
}
