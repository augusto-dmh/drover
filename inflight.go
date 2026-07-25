package drover

import (
	"slices"
	"sync"
)

// inflightSet is the set of jobs this client is currently executing. The
// heartbeat reads it to decide whose leases to extend, so an id belongs
// in the set for exactly as long as losing that job's lease would be
// wrong — from the moment it is claimed to the moment it is finalized.
//
// It is a set rather than a single id because the worker pool arrives in
// a later version. Nothing here assumes one job at a time, so the pool
// changes who calls add and remove, not what they mean.
type inflightSet struct {
	mu  sync.Mutex
	ids map[int64]struct{}
}

func newInflightSet() *inflightSet {
	return &inflightSet{ids: make(map[int64]struct{})}
}

func (s *inflightSet) add(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids[id] = struct{}{}
}

func (s *inflightSet) remove(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ids, id)
}

// snapshot returns the tracked ids in ascending order, or nil when
// nothing is running. Callers get a copy: the heartbeat must not hold
// the lock while it talks to the database.
func (s *inflightSet) snapshot() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ids) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(s.ids))
	for id := range s.ids {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
