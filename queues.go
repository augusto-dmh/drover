package drover

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
)

// weightedQueue is one configured queue and its relative share of the
// fetch loop's attention.
type weightedQueue struct {
	name   string
	weight int
}

// checkedQueues turns the configured map into the slice the fetch loop
// samples from.
//
// The result is sorted by name so the structure a client runs on is
// deterministic — only the sampling is random, which is what makes a
// distributional test of the selection meaningful.
//
// The two kinds of mistake are treated differently, following what the
// rest of Config already does. An empty queue name is structural: no
// caller can address it, because an empty Queue at enqueue time means
// "default", so a client configured that way could never run the jobs it
// claims to work. A weight below one is a tuning value with an obvious
// safe reading, so it is corrected and reported, the way an unusable
// heartbeat interval is.
func checkedQueues(queues map[string]int, logger *slog.Logger) []weightedQueue {
	if len(queues) == 0 {
		return []weightedQueue{{name: defaultQueue, weight: 1}}
	}

	out := make([]weightedQueue, 0, len(queues))
	for name, weight := range queues {
		if name == "" {
			panic("drover: Config.Queues contains an empty queue name")
		}
		if weight < 1 {
			logger.Warn("drover: queue weight must be at least one; using one instead",
				"queue", name, "weight", weight)
			weight = 1
		}
		out = append(out, weightedQueue{name: name, weight: weight})
	}
	slices.SortFunc(out, func(a, b weightedQueue) int {
		return strings.Compare(a.name, b.name)
	})
	return out
}

// weightedOrder returns the order in which one fetch round should try
// the queues: a random permutation in which the chance of a queue
// landing in the first position is its weight over the total, the second
// position is drawn the same way from what remains, and so on.
//
// Every queue appears exactly once, which is what makes the scheme
// starvation-free. Weighting decides how often a queue is tried *first*,
// never whether it is tried at all — so even a queue weighted one against
// a saturated neighbour weighted a thousand still gets its jobs claimed
// within the same round, rather than waiting for a sampling that may not
// come for a long time.
//
// dst, if it has room, is reused so a fetch loop running every poll
// interval does not allocate a fresh slice each time.
//
// The randomness comes from math/rand/v2 directly and is not injectable.
// A test proves the distribution over many samples rather than pinning
// one draw, the same way the retry jitter is tested (AD-017).
func weightedOrder(qs []weightedQueue, dst []string) []string {
	dst = dst[:0]
	if len(qs) == 0 {
		return dst
	}
	if len(qs) == 1 {
		return append(dst, qs[0].name)
	}

	// Summed as int64 so a caller who expresses priority with very large
	// weights cannot overflow the running total and invert the ordering
	// they asked for.
	var total int64
	for _, q := range qs {
		total += int64(q.weight)
	}

	// Copied because selection consumes the set: each pick is removed so
	// the next is drawn from what is left.
	remaining := make([]weightedQueue, len(qs))
	copy(remaining, qs)

	for len(remaining) > 0 {
		pick := rand.Int64N(total) //nolint:gosec // queue selection, not a secret

		// Defaults to the last entry so that the loop always selects
		// something: any rounding or accounting slip lands on a real queue
		// rather than leaving one unpicked and the permutation short.
		idx := len(remaining) - 1
		var acc int64
		for i, q := range remaining {
			acc += int64(q.weight)
			if pick < acc {
				idx = i
				break
			}
		}

		dst = append(dst, remaining[idx].name)
		total -= int64(remaining[idx].weight)
		remaining = slices.Delete(remaining, idx, idx+1)
	}
	return dst
}

// queueNames is the configured order, for a log record that has to name
// what this client will work.
func queueNames(qs []weightedQueue) string {
	parts := make([]string, len(qs))
	for i, q := range qs {
		parts[i] = fmt.Sprintf("%s=%d", q.name, q.weight)
	}
	return strings.Join(parts, ",")
}
