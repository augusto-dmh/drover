package main

import "testing"

// TestIsFlaky pins down the documented failure pattern in delivery.go
// against known addresses, so a change to the hash or the modulus is
// caught here rather than discovered by a reader whose prediction turns
// out wrong.
func TestIsFlaky(t *testing.T) {
	t.Parallel()

	tests := []struct {
		to    string
		flaky bool
	}{
		{"ada@example.com", true},
		{"grace@example.com", false},
		{"alan@example.com", false},
		{"margaret@example.com", false},
		{"user1@example.com", true},
		{"user5@example.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.to, func(t *testing.T) {
			t.Parallel()
			if got := isFlaky(tt.to); got != tt.flaky {
				t.Errorf("isFlaky(%q) = %v, want %v", tt.to, got, tt.flaky)
			}
		})
	}
}

// TestIsFlakyDeterministic guards the "never randomness" claim: repeated
// calls for the same address must always agree, with nothing seeded and
// nothing that varies between runs.
func TestIsFlakyDeterministic(t *testing.T) {
	t.Parallel()

	const to = "repeat@example.com"
	want := isFlaky(to)
	for i := 0; i < 100; i++ {
		if got := isFlaky(to); got != want {
			t.Fatalf("isFlaky(%q) = %v on call %d, want %v", to, got, i, want)
		}
	}
}

// TestDeliver asserts the full attempt-dependent pattern deliver adds on
// top of isFlaky: a flaky address fails once and then succeeds, a
// non-flaky address never fails.
func TestDeliver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		to      string
		attempt int
		wantErr bool
	}{
		{"flaky recipient fails on its first attempt", "ada@example.com", 1, true},
		{"flaky recipient succeeds on the next attempt", "ada@example.com", 2, false},
		{"flaky recipient keeps succeeding after that", "ada@example.com", 5, false},
		{"non-flaky recipient succeeds on the first attempt", "grace@example.com", 1, false},
		{"non-flaky recipient succeeds on a later attempt too", "grace@example.com", 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := deliver(tt.to, tt.attempt)
			if (err != nil) != tt.wantErr {
				t.Errorf("deliver(%q, %d) error = %v, wantErr %v", tt.to, tt.attempt, err, tt.wantErr)
			}
		})
	}
}
