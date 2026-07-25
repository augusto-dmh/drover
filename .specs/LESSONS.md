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

## Quarantined (failed when applied — ignore)

A confirmed lesson that recurred alongside failure. Kept for the maintainer to review.

_none_
