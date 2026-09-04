package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"time"
)

type webConfig struct {
	Listen  string
	Refresh time.Duration
}

func parseWebConfig(args []string, stderr io.Writer) (webConfig, int) {
	fs := flag.NewFlagSet("web", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", defaultWebListen, "address to bind (host:port)")
	refresh := fs.Duration("refresh", defaultWebRefresh, "page auto-refresh interval; 0 disables")
	if err := fs.Parse(args); err != nil {
		return webConfig{}, 2
	}
	if fs.NArg() != 0 {
		cliPrintf(stderr, "drover: unexpected arguments %v\n", fs.Args())
		return webConfig{}, 2
	}
	if *refresh > 0 && *refresh < minWebRefresh {
		cliPrintf(stderr, "drover: --refresh must be 0 or at least 1s\n")
		return webConfig{}, 2
	}
	return webConfig{Listen: *listen, Refresh: *refresh}, 0
}

func runWeb(ctx context.Context, in inspector, args []string, stdout, stderr io.Writer) int {
	cfg, code := parseWebConfig(args, stderr)
	if code != 0 {
		return code
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		cliPrintf(stderr, "drover: %v\n", err)
		return 1
	}
	srv := &http.Server{
		Handler:           newStatusHandler(in, cfg.Refresh),
		ReadHeaderTimeout: 5 * time.Second,
	}
	cliPrintf(stdout, "http://%s/\n", ln.Addr().String())

	done := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			done <- err
			return
		}
		done <- nil
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			_ = srv.Close()
		}
		if err := <-done; err != nil {
			cliPrintf(stderr, "drover: %v\n", err)
			return 1
		}
		return 0
	case err := <-done:
		if err != nil {
			cliPrintf(stderr, "drover: %v\n", err)
			return 1
		}
		return 0
	}
}
