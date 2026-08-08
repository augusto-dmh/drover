# LESSONS — auto-maintained by scripts/lessons.py

> Machine-owned. Do NOT hand-edit. Changes are overwritten on the next `lessons.py` write.
> Canonical state lives in `.specs/lessons.json`. Edit lessons only via the script.
> promote_threshold=2 distinct features · window_days=45 · quarantine_threshold=2

## Confirmed (load these at Specify/Design)

Corroborated across multiple features. Safe to apply as guidance.

_none_

## Candidates (under observation — do NOT load as guidance yet)

Seen once or not yet corroborated. Tracked, not trusted.

### L-001 — Interval/timing ACs need a bounded-count (or fake-clock) assertion; a lower-bound-only count passes under a busy loop and discriminates nothing.
- signal: `surviving_mutant` · recurrence: 1 feature(s) · scope: `testing/timing` · harmful: 0
- features: cycle-a-walking-skeleton
- evidence: loop.go:120 M5 / CORE-03 AC7 (testing/timing)
- last seen: 2026-07-25T14:42:47Z

### L-002 — When a test fixture's stub returns the same value the un-implemented path would produce (e.g. an immediate-retry policy answering time.Now()), the assertion cannot tell 'consulted' from 'ignored' — give at least one test a distinctive value.
- signal: `surviving_mutant` · recurrence: 1 feature(s) · scope: `testing/fixtures` · harmful: 0
- features: cycle-b-reliability-core
- evidence: loop.go:194 M16 / RESCUE-01 (spec P1-Story3 AC1) (testing/fixtures)
- last seen: 2026-07-25T22:49:20Z

### L-003 — Run a coverage report over the feature's new files before declaring an AC covered; a function at 0% means the criterion has no evidence no matter how many tests appear to name it.
- signal: `ac_gap` · recurrence: 1 feature(s) · scope: `testing/coverage` · harmful: 0
- features: cycle-c-concurrency
- evidence: pool.go:229 abandon 0.0% coverage / P1-Story2 AC6 (SHUT-05) (testing/coverage)
- last seen: 2026-07-26T14:03:33Z

### L-004 — An AC saying an operation must NOT wait for a timer needs an upper-bound elapsed-time assertion; without one, deleting the early-exit channel arm survives.
- signal: `surviving_mutant` · recurrence: 1 feature(s) · scope: `testing/timing` · harmful: 0
- features: cycle-c-concurrency
- evidence: pool.go:433 M9 / edge case: Stop must not wait out a poll interval (testing/timing)
- last seen: 2026-07-26T14:03:33Z

### L-005 — When an AC distinguishes two error classifications only by how they are logged, assert on the log record's level and message; otherwise both branches are indistinguishable to the suite.
- signal: `surviving_mutant` · recurrence: 1 feature(s) · scope: `testing/logging` · harmful: 0
- features: cycle-c-concurrency
- evidence: loop.go:247 M13 / P1-Story3 AC3 (testing/logging)
- last seen: 2026-07-26T14:03:33Z

### L-006 — A best-effort loop that must continue past a failing element needs a test where exactly one element fails and a later one is asserted to have still been processed.
- signal: `surviving_mutant` · recurrence: 1 feature(s) · scope: `testing/error-paths` · harmful: 0
- features: cycle-c-concurrency
- evidence: pool.go:266 M14 / edge case: requeue failure at shutdown (testing/error-paths)
- last seen: 2026-07-26T14:03:33Z

### L-007 — When a deferred cleanup performs the same side effect as the code under test, assert the effect happens before the function returns or in a required order; otherwise the deferred path silently satisfies the test.
- signal: `spec_precision_gap` · recurrence: 1 feature(s) · scope: `testing/lifecycle` · harmful: 0
- features: cycle-c-concurrency
- evidence: pool_test.go:557 M6 / P1-Story2 AC3, AC5 (testing/lifecycle)
- last seen: 2026-07-26T14:03:33Z

### L-008 — Assert scrape-or-Gather before the first successful gauge refresh still publishes configured queue series at zero rather than omitting them
- signal: `ac_gap` · recurrence: 1 feature(s) · scope: `metrics` · harmful: 0
- features: cycle-e-observability
- evidence: EDGE-01 (metrics)
- last seen: 2026-08-07T03:56:07Z

### L-009 — Keep a compile-or-Example test for every README config snippet that names shipped API fields
- signal: `ac_gap` · recurrence: 1 feature(s) · scope: `docs` · harmful: 0
- features: cycle-e-observability
- evidence: OBS-13.3 (docs)
- last seen: 2026-08-07T03:56:07Z

### L-010 — When a constructor gains a parameter beyond the design signature, leave a SPEC_DEVIATION with the reason at the definition site
- signal: `spec_deviation` · recurrence: 1 feature(s) · scope: `metrics` · harmful: 0
- features: cycle-e-observability
- evidence: metrics.go:38 SPEC_DEVIATION (metrics)
- last seen: 2026-08-07T03:56:07Z

### L-011 — When wrapping Driver.Stats, assert oldest-claimable ages in the Inspector suite, not only published-state depth counts
- signal: `ac_gap` · recurrence: 1 feature(s) · scope: `inspector` · harmful: 0
- features: cycle-f-cli-introspection
- evidence: Inspector Stats AC #2 / CLI-02; inspector_test.go:28-84 (inspector)
- last seen: 2026-08-08T18:03:26Z

### L-012 — Discrimination on Stats adapters must mutate age mapping away; depth-only assertions leave age regressions green
- signal: `surviving_mutant` · recurrence: 1 feature(s) · scope: `inspector` · harmful: 0
- features: cycle-f-cli-introspection
- evidence: sensor M8 inspector.go Stats Oldest mapping dropped (inspector)
- last seen: 2026-08-08T18:03:27Z

## Quarantined (failed when applied — ignore)

A confirmed lesson that recurred alongside failure. Kept for the maintainer to review.

_none_
