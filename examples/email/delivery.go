package main

import (
	"fmt"
	"hash/fnv"
)

// isFlaky reports whether to belongs to the deterministic subset of
// addresses this example's delivery stub always fails to reach on their
// first attempt. Membership comes from hashing the address — never from
// math/rand — so the same run always retries the same jobs: an address
// whose FNV-1a hash mod 3 is zero is flaky, everything else is not. A
// reader can compute isFlaky over the recipient list before ever running
// the program and know exactly which enqueued jobs will retry.
func isFlaky(to string) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(to)) // hash.Hash.Write never returns an error
	return h.Sum32()%3 == 0
}

// deliver simulates sending an email to "to" on the given attempt
// number. It is the only place failure is decided: an address in the
// flaky subset (see isFlaky) fails on its first attempt and succeeds on
// every attempt after that; every other address succeeds immediately.
// Failing here is what drives drover's ordinary retry path — the worker
// just returns the error deliver hands back.
func deliver(to string, attempt int) error {
	if isFlaky(to) && attempt <= 1 {
		return fmt.Errorf("delivery: smtp timeout sending to %s", to)
	}
	return nil
}
