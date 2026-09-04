package drover

import (
	"fmt"
	"time"

	"github.com/augusto-dmh/drover/internal/cron"
)

// PeriodicJob registers a cron or @every schedule that an elected
// leader enqueues. ID identifies the schedule, not the job kind; kind
// still comes from Args.Kind().
type PeriodicJob struct {
	ID   string
	Cron string
	Args JobArgs

	// Opts are enqueue choices for each tick. UniqueKey is overwritten
	// at enqueue with id plus the fire instant, so a caller key is
	// never used as-is.
	Opts *InsertOpts

	// Location interprets 5-field cron times. Nil is UTC. @every
	// stays Unix-epoch aligned regardless of Location.
	Location *time.Location
}

type periodicEntry struct {
	job      PeriodicJob
	schedule cron.Schedule
}

func checkedPeriodicJobs(jobs []PeriodicJob) []periodicEntry {
	seen := make(map[string]int, len(jobs))
	out := make([]periodicEntry, 0, len(jobs))
	for i, job := range jobs {
		if job.ID == "" {
			panic(fmt.Sprintf("drover: Config.PeriodicJobs[%d] has an empty ID", i))
		}
		if prev, ok := seen[job.ID]; ok {
			panic(fmt.Sprintf("drover: Config.PeriodicJobs[%d] duplicates ID %q from PeriodicJobs[%d]", i, job.ID, prev))
		}
		seen[job.ID] = i
		if job.Args == nil {
			panic(fmt.Sprintf("drover: Config.PeriodicJobs[%d] (id %q) has nil Args", i, job.ID))
		}
		if job.Args.Kind() == "" {
			panic(fmt.Sprintf("drover: Config.PeriodicJobs[%d] (id %q) has an empty kind", i, job.ID))
		}
		schedule, err := cron.ParseIn(job.Cron, job.Location)
		if err != nil {
			panic(fmt.Sprintf("drover: Config.PeriodicJobs[%d] (id %q) has an invalid cron %q: %v", i, job.ID, job.Cron, err))
		}
		out = append(out, periodicEntry{job: job, schedule: schedule})
	}
	return out
}
