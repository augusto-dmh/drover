package drover

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/augusto-dmh/drover/internal/memdriver"
)

type badArgs struct {
	Ch chan int
}

func (badArgs) Kind() string { return "bad" }

type emptyKindArgs struct{}

func (emptyKindArgs) Kind() string { return "" }

func assertNothingPersisted(t *testing.T, mem *memdriver.Driver) {
	t.Helper()
	rows, err := mem.FetchAvailable(context.Background(), "default", time.Minute, 1)
	if err != nil {
		t.Fatalf("FetchAvailable: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("found %d persisted jobs, want 0", len(rows))
	}
}

func TestInsertPersistsTypedJob(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	c := newClient(mem, Config{})

	row, err := c.Insert(context.Background(), greetArgs{Name: "ada"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	if row.Kind != "greet" {
		t.Errorf("Kind = %q, want greet", row.Kind)
	}
	if row.Queue != "default" {
		t.Errorf("Queue = %q, want default", row.Queue)
	}
	if row.State != StateAvailable {
		t.Errorf("State = %q, want %q", row.State, StateAvailable)
	}
	if string(row.Args) != `{"name":"ada"}` {
		t.Errorf("Args = %s, want {\"name\":\"ada\"}", row.Args)
	}
	if stored, ok := mem.Row(row.ID); !ok || stored.State != "available" {
		t.Errorf("stored row missing or not available: %+v", stored)
	}
}

func TestInsertRejectsEmptyKind(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	c := newClient(mem, Config{})

	_, err := c.Insert(context.Background(), emptyKindArgs{})

	if !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("error = %v, want ErrInvalidKind", err)
	}
	assertNothingPersisted(t, mem)
}

func TestInsertWrapsMarshalFailure(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	c := newClient(mem, Config{})

	_, err := c.Insert(context.Background(), badArgs{})

	if err == nil {
		t.Fatal("Insert succeeded with unmarshalable args")
	}
	if !strings.Contains(err.Error(), `kind "bad"`) {
		t.Errorf("error = %q, want it to name the kind", err)
	}
	assertNothingPersisted(t, mem)
}

func TestInsertTxValidatesBeforeTouchingTransaction(t *testing.T) {
	t.Parallel()
	mem := memdriver.New()
	c := newClient(mem, Config{})

	if _, err := c.InsertTx(context.Background(), nil, emptyKindArgs{}); !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("InsertTx empty kind: %v, want ErrInvalidKind", err)
	}
	if _, err := c.InsertTx(context.Background(), nil, badArgs{}); err == nil || !strings.Contains(err.Error(), `kind "bad"`) {
		t.Fatalf("InsertTx marshal failure: %v, want wrapped error naming the kind", err)
	}
	assertNothingPersisted(t, mem)
}

func TestNewClientRejectsNilPool(t *testing.T) {
	t.Parallel()

	_, err := NewClient(nil, Config{})
	if err == nil {
		t.Fatal("NewClient(nil) returned no error")
	}
}

func TestConfigZeroValuesGetDefaults(t *testing.T) {
	t.Parallel()

	c := newClient(memdriver.New(), Config{})

	if c.logger != slog.Default() {
		t.Error("Logger default is not slog.Default()")
	}
	if c.pollInterval != time.Second {
		t.Errorf("PollInterval default = %v, want 1s", c.pollInterval)
	}
	if c.workers == nil {
		t.Error("Workers default is nil, want empty registry")
	}
	if _, ok := c.retryPolicy.(ExponentialRetryPolicy); !ok {
		t.Errorf("RetryPolicy default = %T, want ExponentialRetryPolicy", c.retryPolicy)
	}
	if c.leaseDuration != time.Minute {
		t.Errorf("LeaseDuration default = %v, want 1m", c.leaseDuration)
	}
	if c.heartbeatInterval != 20*time.Second {
		t.Errorf("HeartbeatInterval default = %v, want a third of the lease", c.heartbeatInterval)
	}
	if c.rescueInterval != time.Minute {
		t.Errorf("RescueInterval default = %v, want the lease duration", c.rescueInterval)
	}
	if c.inflight == nil {
		t.Error("in-flight set is nil, want an empty set")
	}
}

func TestTimingConfigFallsBackToUsableValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cfg           Config
		wantLease     time.Duration
		wantHeartbeat time.Duration
		wantRescue    time.Duration
	}{
		{
			name:          "negative values are treated as unset",
			cfg:           Config{LeaseDuration: -time.Second, HeartbeatInterval: -time.Second, RescueInterval: -time.Second},
			wantLease:     time.Minute,
			wantHeartbeat: 20 * time.Second,
			wantRescue:    time.Minute,
		},
		{
			name:          "a shortened lease pulls the derived intervals with it",
			cfg:           Config{LeaseDuration: 30 * time.Second},
			wantLease:     30 * time.Second,
			wantHeartbeat: 10 * time.Second,
			wantRescue:    30 * time.Second,
		},
		{
			// A heartbeat this slow could only ever renew after the lease
			// it renews has already lapsed, so every job outliving one
			// lease would be rescued while still running.
			name:          "a heartbeat as slow as the lease is replaced",
			cfg:           Config{LeaseDuration: time.Minute, HeartbeatInterval: time.Minute},
			wantLease:     time.Minute,
			wantHeartbeat: 20 * time.Second,
			wantRescue:    time.Minute,
		},
		{
			name:          "a heartbeat slower than the lease is replaced",
			cfg:           Config{LeaseDuration: time.Minute, HeartbeatInterval: 5 * time.Minute},
			wantLease:     time.Minute,
			wantHeartbeat: 20 * time.Second,
			wantRescue:    time.Minute,
		},
		{
			name:          "explicit values are kept",
			cfg:           Config{LeaseDuration: time.Minute, HeartbeatInterval: 5 * time.Second, RescueInterval: 2 * time.Minute},
			wantLease:     time.Minute,
			wantHeartbeat: 5 * time.Second,
			wantRescue:    2 * time.Minute,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newClient(memdriver.New(), tc.cfg)

			if c.leaseDuration != tc.wantLease {
				t.Errorf("leaseDuration = %v, want %v", c.leaseDuration, tc.wantLease)
			}
			if c.heartbeatInterval != tc.wantHeartbeat {
				t.Errorf("heartbeatInterval = %v, want %v", c.heartbeatInterval, tc.wantHeartbeat)
			}
			if c.rescueInterval != tc.wantRescue {
				t.Errorf("rescueInterval = %v, want %v", c.rescueInterval, tc.wantRescue)
			}
			if c.heartbeatInterval >= c.leaseDuration {
				t.Errorf("heartbeatInterval %v is not shorter than the lease %v — a lease "+
					"renewed this slowly always lapses first",
					c.heartbeatInterval, c.leaseDuration)
			}
		})
	}
}
