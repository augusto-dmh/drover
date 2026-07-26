package drover

import (
	"slices"
	"sync"

	"github.com/augusto-dmh/drover/internal/driver"
)

// inflightSet is the set of leases this client is currently executing
// under. The heartbeat reads it to decide which leases to renew, so a
// lease belongs in the set for exactly as long as losing it would be
// wrong — from the moment the job is claimed to the moment it is
// finalized.
//
// It holds leases rather than bare ids because a renewal has to name the
// attempt it is renewing: a job whose lease already lapsed may have been
// rescued and re-claimed elsewhere, and this client extending it then
// would be taking back a job another worker is running.
//
// It is a set because the worker pool arrives in a later version.
// Nothing here assumes one job at a time, so the pool changes who calls
// add and remove, not what they mean.
type inflightSet struct {
	mu       sync.Mutex
	attempts map[int64]int
}

func newInflightSet() *inflightSet {
	return &inflightSet{attempts: make(map[int64]int)}
}

func (s *inflightSet) add(lease driver.Lease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[lease.ID] = lease.Attempt
}

func (s *inflightSet) remove(lease driver.Lease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Guarded by attempt so a finishing job cannot untrack a later claim
	// of the same row that another goroutine has already registered.
	if held, ok := s.attempts[lease.ID]; ok && held == lease.Attempt {
		delete(s.attempts, lease.ID)
	}
}

// snapshot returns the held leases in ascending id order, or nil when
// nothing is running. Callers get a copy: the heartbeat must not hold
// the lock while it talks to the database.
func (s *inflightSet) snapshot() []driver.Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.attempts) == 0 {
		return nil
	}
	leases := make([]driver.Lease, 0, len(s.attempts))
	for id, attempt := range s.attempts {
		leases = append(leases, driver.Lease{ID: id, Attempt: attempt})
	}
	slices.SortFunc(leases, func(a, b driver.Lease) int {
		return int(a.ID - b.ID)
	})
	return leases
}
