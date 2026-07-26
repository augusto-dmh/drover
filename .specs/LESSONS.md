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

## Quarantined (failed when applied — ignore)

A confirmed lesson that recurred alongside failure. Kept for the maintainer to review.

_none_
