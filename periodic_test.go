package drover

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/augusto-dmh/drover/internal/memdriver"
)

func TestPeriodicJobsEmptySliceDoesNotPanic(t *testing.T) {
	t.Parallel()

	for _, jobs := range [][]PeriodicJob{nil, {}} {
		c := newClient(memdriver.New(), Config{PeriodicJobs: jobs})
		if len(c.periodic) != 0 {
			t.Fatalf("empty PeriodicJobs stored %d entries, want 0", len(c.periodic))
		}
	}
}

func TestPeriodicJobsValidSliceIsAccepted(t *testing.T) {
	t.Parallel()

	c := newClient(memdriver.New(), Config{
		PeriodicJobs: []PeriodicJob{
			{ID: "hourly", Cron: "0 * * * *", Args: pingArgs{}},
			{ID: "every", Cron: "@every 30s", Args: greetArgs{Name: "ada"}, Location: time.UTC},
		},
	})
	if len(c.periodic) != 2 {
		t.Fatalf("stored %d periodic jobs, want 2", len(c.periodic))
	}
	if c.periodic[0].job.ID != "hourly" || c.periodic[1].job.ID != "every" {
		t.Errorf("stored IDs = %q, %q; want hourly, every", c.periodic[0].job.ID, c.periodic[1].job.ID)
	}
	if c.periodic[0].schedule == nil || c.periodic[1].schedule == nil {
		t.Fatal("parsed schedule was not stored")
	}
}

func TestPeriodicJobConstructionPanics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		jobs []PeriodicJob
		want []string
	}{
		{
			name: "empty ID",
			jobs: []PeriodicJob{{ID: "", Cron: "@every 1s", Args: pingArgs{}}},
			want: []string{"PeriodicJobs[0]", "empty ID"},
		},
		{
			name: "duplicate ID",
			jobs: []PeriodicJob{
				{ID: "tick", Cron: "@every 1s", Args: pingArgs{}},
				{ID: "tick", Cron: "@every 2s", Args: greetArgs{Name: "ada"}},
			},
			want: []string{"PeriodicJobs[1]", "tick", "PeriodicJobs[0]"},
		},
		{
			name: "nil Args",
			jobs: []PeriodicJob{{ID: "tick", Cron: "@every 1s"}},
			want: []string{"PeriodicJobs[0]", "nil Args"},
		},
		{
			name: "empty kind",
			jobs: []PeriodicJob{{ID: "tick", Cron: "@every 1s", Args: emptyKindArgs{}}},
			want: []string{"PeriodicJobs[0]", "empty kind"},
		},
		{
			name: "unparseable cron",
			jobs: []PeriodicJob{{ID: "tick", Cron: "not-a-cron", Args: pingArgs{}}},
			want: []string{"PeriodicJobs[0]", "invalid cron"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("constructing the client did not panic")
				}
				msg := fmt.Sprint(r)
				for _, want := range tt.want {
					if !strings.Contains(msg, want) {
						t.Errorf("panic = %q, want it to contain %q", msg, want)
					}
				}
			}()
			newClient(memdriver.New(), Config{PeriodicJobs: tt.jobs})
		})
	}
}
