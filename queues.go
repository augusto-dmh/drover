package drover

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
)

// maxQueueWeight bounds a configured weight. It is far past any ratio a
// priority scheme expresses — a queue weighted a thousand against one is
// already served first virtually always — and it makes the running total
// in weightedOrder unable to overflow however many queues are
// configured. Widening int64 over int does not achieve that: int is 64
// bits on every 64-bit build, so the conversion is a no-op and two
// enormous weights are enough to wrap the sum negative. A negative total
// reaches rand.Int64N, which panics, on the fetch goroutine, which has
// nothing above it to recover.
const maxQueueWeight = 100_000

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
		switch {
		case weight < 1:
			logger.Warn("drover: queue weight must be at least one; using one instead",
				"queue", name, "weight", weight)
			weight = 1
		case weight > maxQueueWeight:
			logger.Warn("drover: queue weight is above the maximum; using the maximum instead",
				"queue", name, "weight", weight, "maximum", maxQueueWeight)
			weight = maxQueueWeight
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
// dst and scratch are both reused when they have room, so a fetch loop
// running every poll interval allocates nothing per round. scratch holds
// the queues still unpicked; dst receives the ordering.
//
// The randomness comes from math/rand/v2 directly and is not injectable.
// A test proves the distribution over many samples rather than pinning
// one draw, the same way the retry jitter is tested (AD-017).
func weightedOrder(qs []weightedQueue, dst []string, scratch []weightedQueue) ([]string, []weightedQueue) {
	dst, scratch = dst[:0], scratch[:0]
	if len(qs) == 0 {
		return dst, scratch
	}
	if len(qs) == 1 {
		return append(dst, qs[0].name), scratch
	}

	// Cannot overflow: checkedQueues caps each weight at maxQueueWeight,
	// so the total is bounded by that times the number of queues.
	var total int64
	for _, q := range qs {
		total += int64(q.weight)
	}

	// Copied because selection consumes the set: each pick is removed so
	// the next is drawn from what is left.
	remaining := append(scratch, qs...)

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

		// Swap-removed rather than shifted: the accumulator above walks
		// whatever order the set happens to be in, so reordering what is
		// left cannot change any queue's probability, and this keeps the
		// whole selection linear per pick instead of copying the tail.
		remaining[idx] = remaining[len(remaining)-1]
		remaining = remaining[:len(remaining)-1]
	}
	return dst, remaining
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
