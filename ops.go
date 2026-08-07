package drover

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// opsServer serves /metrics, /healthz, and /readyz on a listener the
// caller already bound. It owns nothing about when to start or stop —
// that is the client's lifecycle.
type opsServer struct {
	server *http.Server
	ln     net.Listener
	logger *slog.Logger
	done   chan struct{}
}

// newOpsServer builds the mux and server over an already-bound listener.
// ready is consulted on every /readyz: a nil return is ready, any error
// is 503 with the error text as the body. A nil ready function means
// always ready.
func newOpsServer(
	ln net.Listener,
	reg *prometheus.Registry,
	ready func() error,
	logger *slog.Logger,
) *opsServer {
	if logger == nil {
		logger = slog.Default()
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		if err := ready(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	return &opsServer{
		server: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		ln:     ln,
		logger: logger,
		done:   make(chan struct{}),
	}
}

// serve runs until the server is shut down. It closes done when Serve
// returns so shutdown can join this goroutine — Shutdown alone does not.
func (s *opsServer) serve() {
	defer close(s.done)
	err := s.server.Serve(s.ln)
	if err != nil && err != http.ErrServerClosed {
		s.logger.Error("drover: ops server", "error", err)
	}
}

// shutdown stops accepting and waits until the serving goroutine has
// returned. Waiting on done is what makes goleak pass; Shutdown alone
// does not join Serve.
func (s *opsServer) shutdown(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	<-s.done
	return err
}
