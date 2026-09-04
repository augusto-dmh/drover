// Command drover is the operator CLI for a Drover queue.
package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// version is set at link time by GoReleaser (-X main.version=…).
// Local builds keep the default.
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, os.Getenv, nil))
}

// openInspectorFn opens an Inspector for the given DSN. Tests inject a stub.
type openInspectorFn func(ctx context.Context, dsn string) (inspector, func(), error)

func run(args []string, stdout, stderr io.Writer, getenv func(string) string, open openInspectorFn) int {
	if open == nil {
		open = defaultOpenInspector
	}
	cfg, rest, err := peelGlobals(args)
	if err != nil {
		cliPrintf(stderr, "drover: %v\n", err)
		return 2
	}
	if cfg.help {
		printUsage(stdout)
		return 0
	}
	if cfg.version {
		cliPrintln(stdout, version)
		return 0
	}
	if len(rest) == 0 {
		printUsage(stderr)
		return 2
	}

	ctx := context.Background()
	switch rest[0] {
	case "version":
		cliPrintln(stdout, version)
		return 0
	case "stats":
		return withInspector(ctx, cfg, getenv, open, stderr, func(in inspector) int {
			return runStats(ctx, in, cfg.json, stdout, stderr)
		})
	case "jobs":
		if len(rest) < 2 || rest[1] != "list" {
			cliPrintf(stderr, "drover: unknown command %q\n\n", strings.Join(rest, " "))
			printUsage(stderr)
			return 2
		}
		return withInspector(ctx, cfg, getenv, open, stderr, func(in inspector) int {
			return runJobsList(ctx, in, rest[2:], cfg.json, stdout, stderr)
		})
	case "retry":
		return withInspector(ctx, cfg, getenv, open, stderr, func(in inspector) int {
			return runRetry(ctx, in, rest[1:], cfg.json, stdout, stderr)
		})
	case "cancel":
		return withInspector(ctx, cfg, getenv, open, stderr, func(in inspector) int {
			return runCancel(ctx, in, rest[1:], cfg.json, stdout, stderr)
		})
	case "enqueue":
		return withInspector(ctx, cfg, getenv, open, stderr, func(in inspector) int {
			return runEnqueue(ctx, in, rest[1:], cfg.json, stdout, stderr)
		})
	case "web":
		if cfg.json {
			cliPrintf(stderr, "drover: --json is not valid with web\n")
			return 2
		}
		webCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer stop()
		return withInspector(webCtx, cfg, getenv, open, stderr, func(in inspector) int {
			return runWeb(webCtx, in, rest[1:], stdout, stderr)
		})
	default:
		cliPrintf(stderr, "drover: unknown command %q\n\n", rest[0])
		printUsage(stderr)
		return 2
	}
}

func withInspector(
	ctx context.Context,
	cfg globalConfig,
	getenv func(string) string,
	open openInspectorFn,
	stderr io.Writer,
	fn func(inspector) int,
) int {
	dsn, err := resolveDSN(cfg.database, getenv)
	if err != nil {
		cliPrintf(stderr, "drover: %v\n", err)
		return 2
	}
	in, cleanup, err := open(ctx, dsn)
	if err != nil {
		cliPrintf(stderr, "drover: %v\n", err)
		return 1
	}
	if cleanup != nil {
		defer cleanup()
	}
	return fn(in)
}

func defaultOpenInspector(ctx context.Context, dsn string) (inspector, func(), error) {
	in, pool, err := openInspector(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	return in, pool.Close, nil
}

func printUsage(w io.Writer) {
	cliPrintf(w, `Usage: drover [global flags] <command> [command flags]

Commands:
  stats              Print per-queue depth and oldest-claimable age
  jobs list          List jobs (optional --queue, --state, --limit)
  retry <id>         Redrive a dead job to available
  cancel <id>        Cancel a waiting or dead job
  enqueue            Insert a job (--kind required; --queue, --args optional)
  web                Serve a server-rendered status page (retry/cancel)
  version            Print the drover version

Global flags:
  --database URL     Postgres URL (default: $DATABASE_URL)
  --json             Machine-readable JSON output
  --version          Print the drover version
  -h, --help         Show this help
`)
}
