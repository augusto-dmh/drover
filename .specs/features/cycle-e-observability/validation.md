# Observability Validation

**Date**: 2026-08-08
**Spec**: `.specs/features/cycle-e-observability/spec.md`
**Diff range**: `main..HEAD` on `feat/observability` (`54dbc62`…`b9fa89e`, planning through fix iteration 1)
**Verifier**: independent sub-agent (author ≠ verifier)
**Re-verification**: after fix iteration 1 (`f8e8b71` EDGE-01, `b9fa89e` OBS-13.3); overwrites prior FAIL report

---

## Task Completion

| Task | Status | Notes |
| ---- | ------ | ----- |
| T1–T17 | ✅ Done | All Done-when boxes checked in `tasks.md`; commits present on branch |

---

## Spec-Anchored Acceptance Criteria

### P1: Ops port (OBS-01, OBS-02)

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| OBS-01.1 OpsAddr set → `GET /metrics` Prometheus text | 200 + Prometheus text format from client registry | `ops_test.go:73-81` — `code == 200`, body contains `# TYPE … counter`; client wiring `client_test.go:691+` (`TestOpsAnswersThroughoutDrain`) | ✅ PASS |
| OBS-01.2 Empty OpsAddr → no listener / no ops goroutine | `runner.ops == nil` | `client_test.go:529-533` — `opsNil` must be true | ✅ PASS |
| OBS-01.3 Stop → address rebindable | `net.Listen` on prior addr succeeds | `client_test.go:586-611` — `Listen` after `Stop` | ✅ PASS |
| OBS-01.4 Unbindable addr → Start error naming addr, not partially started | error contains addr; `runner == nil`; Startable again | `client_test.go:541-578` | ✅ PASS |
| OBS-01.5 Two clients / distinct registries construct | neither panics; distinct metric sets | `client_test.go:162-179`, `metrics_test.go:478-492` | ✅ PASS |

### P1: Execution counters / histogram (OBS-03, OBS-04, OBS-05)

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| OBS-03.1 Success → completed +1, failed 0 | exact counter deltas on queue label | `metrics_test.go:307-356` — `wantCompleted=1, wantFailed=0` | ✅ PASS |
| OBS-03.2 Error → failed +1, completed 0 | exact counter deltas | `metrics_test.go:307-356` | ✅ PASS |
| OBS-03.3 Duration observed success or fail | histogram sample count == 1 | `metrics_test.go:359-365` | ✅ PASS |
| OBS-03.4 Panic → failed, not completed | failed=1, completed absent/0 | `metrics_test.go:377-412` | ✅ PASS |
| OBS-05 Metrics from middleware, not execution path | chain records metrics; ordering vs Logging / user MW | `middleware_test.go:550-591`, `599-631` | ✅ PASS |
| OBS-05.6 User middleware cannot disable metrics | completed=1 with user MW present | `middleware_test.go:636-664` | ✅ PASS |

### P1: Queue gauges (OBS-06, OBS-07, OBS-08, OBS-09)

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| OBS-06.1 Claimable jobs → age ≈ DB wait | age within ±5s of overdue | `metrics_integration_test.go:111-119` | ✅ PASS |
| OBS-06.2 No claimable → age 0 | `laterAge.value == 0` | `metrics_integration_test.go:121-127`; unit seed `stats_test.go:90-97` | ✅ PASS |
| OBS-07 Depth per queue/state | depths match inserted states | `metrics_integration_test.go:97-134`; `stats_test.go:241-288` | ✅ PASS |
| OBS-08 Scrape issues no Stats / N scrapes ≠ more queries | Stats call count unchanged across Gathers | `client_test.go:439-470` | ✅ PASS |
| OBS-09 Refresh failure → warn, gauges unchanged, freshness not advanced | depth/age stay 7/9; fresh false after bound | `stats_test.go:290-327`; warn via `stats_test.go:333+` (`TestRunLogsWarningOnRefreshFailure`) | ✅ PASS |
| OBS-06.7 Refresher stops with client | goleak after Stop | `client_test.go:478-499` | ✅ PASS |

### P1: Health endpoints (OBS-10, OBS-11)

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| OBS-10.1 `/healthz` always 200, no DB | 200 even when ready fails | `ops_test.go:93-96`; after DB loss `ops_integration_test.go:95-98` | ✅ PASS |
| OBS-11.2 Started + reachable DB → `/readyz` 200 | 200 healthy | `ops_integration_test.go:69-72` | ✅ PASS |
| OBS-11.3 DB unreachable → readyz 503, healthz 200 | diverge | `ops_integration_test.go:79-105` | ✅ PASS |
| OBS-11.4 Stop/drain → readyz 503 immediately | body contains `draining` while Stop waits | `client_test.go:616-676` | ✅ PASS |
| OBS-11.5 Check cannot hang a probe | D-7: freshness is memory read (not per-probe DB ping) | `stats_test.go:290-327` (`fresh` pure); `pool.go:113-120` ready fn | ✅ PASS (outcome via D-7; AC text still says “timeout”) |

### P2: Pool saturation (OBS-12)

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| OBS-12.1 Executing gauge ≤ concurrency; at peak == N | `peak.value == concurrency` | `metrics_test.go:452-461` | ✅ PASS |
| OBS-12.2 Idle → 0 | `settled.value == 0` | `metrics_test.go:466-472` | ✅ PASS |
| OBS-12.3 Configured concurrency published | gauge == configured | `metrics_test.go:167-192` | ✅ PASS |

### P2: README (OBS-13) — prior FAIL cleared

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| OBS-13.1 List every metric family + type + labels | seven families documented | `README.md:105-113` | ✅ PASS (doc inspection) |
| OBS-13.2 Concrete oldest-age alert expression + rationale | PromQL + “primary alerting” prose | `README.md:119-123` | ✅ PASS (doc inspection) |
| OBS-13.3 Config snippet compiles against shipped package | Independent Test: extract and compile | `example_test.go:97-106` — `ExampleConfig_observability` typechecks `OpsAddr`/`StatsInterval`/`MetricsRegistry` matching README; package compile under `go test -race ./...` (Example without `// Output:` is compile-checked) | ✅ PASS |

### Edge cases — prior EDGE-01 FAIL cleared

| Criterion | Spec-defined outcome | `file:line` + assertion | Result |
| --------- | -------------------- | ----------------------- | ------ |
| EDGE-01 Scrape before first refresh → families at zero, no block/omit | zero values present, not omitted; Gather does not block | `stats.go:79-97` (`seedConfiguredZeros` at construction); `stats_test.go:124-200` — `TestGatherBeforeFirstRefreshServesConfiguredQueueZeros` asserts series at 0 while `Stats` blocked and Gather finishes within 2s | ✅ PASS |
| EDGE-02 Configured empty queue publishes zeros | depth/age present at 0 | `stats_test.go:64-98`; `metrics_integration_test.go:138-149` | ✅ PASS |
| EDGE-03 DB-only queue published under own label | foreign queue series | `stats_test.go:203-238` | ✅ PASS |
| EDGE-04 Non-positive StatsInterval → default + warn | warn string + default interval | `client_test.go:395-408` | ✅ PASS |
| EDGE-05 Stop during refresh → no leak | goleak | `client_test.go:478-499` | ✅ PASS |
| EDGE-06 Unknown ops path → 404 | `StatusNotFound` | `ops_test.go:140-149` | ✅ PASS |
| EDGE-07 Two clients own registries | no panic / distinct sets | `client_test.go:162-179` | ✅ PASS |
| EDGE-08 Unusable queue label → warn, skip one series, no panic | 1 series remains; WARN | `metrics_test.go:220-299` | ✅ PASS |
| EDGE-09 Never started → no Stats | calls == 0 | `client_test.go:423-433` | ✅ PASS |

### Assumptions (ASM) — outcome-bearing

| Criterion | Evidence | Result |
| --------- | -------- | ------ |
| ASM-02 Ops opt-in | `client_test.go:514-533` | ✅ |
| ASM-03 Refresh ≠ scrape | `client_test.go:439-470` | ✅ |
| ASM-04 Per-client registry | `client_test.go:162-179` | ✅ |
| ASM-05 `queue` label only | `metrics_test.go:91-136` | ✅ |
| ASM-06 Failed = execution error | `metrics_test.go:307-367` | ✅ |
| ASM-07 Empty age = 0 | `stats_test.go:90-97` | ✅ |
| ASM-08 Claimable-only age | `internal/memdriver/memdriver_test.go:1050-1075`; `metrics_integration_test.go:121-127` | ✅ |
| ASM-09 DB clock ages | `internal/pgdriver/pgdriver_integration_test.go:1128-1157` | ✅ |
| ASM-10 Fail preserves gauges | `stats_test.go:290-327` | ✅ |

**Status**: ✅ All ACs covered (38/38 story+edge; prior EDGE-01 + OBS-13.3 gaps closed)

**Also noted**: `metrics.go:38` `// SPEC_DEVIATION` — `newMetricSet` takes a logger beyond the design signature (justified for EDGE-08 logging). Not a verification failure.

---

## Discrimination Sensor

Scratch only: temp copy at `/tmp/drover-obs-sensor-copy` from `git archive HEAD` (real working tree never mutated). Files restored between mutants; DEST verified identical to clean backups after run.

**Sensor depth**: expanded lightweight (≥5) — includes EDGE-01 seeding

| # | Mutation | File | Description | Killed? |
| - | -------- | ---- | ----------- | ------- |
| 1 | Remove pre-refresh seed | `stats.go` | Delete `r.seedConfiguredZeros()` from `newStatsRefresher` | ✅ `TestGatherBeforeFirstRefreshServesConfiguredQueueZeros` |
| 2 | Failed refresh zeros gauges | `stats.go` | On Stats error, set depth/age to 0 before return | ✅ `TestRefreshFailureLeavesGaugesAndFreshnessUntouched` |
| 3 | healthz consults ready | `ops.go` | `/healthz` calls `ready()` and can 503 | ✅ `TestOpsHealthzAlwaysOK` |
| 4 | Empty OpsAddr still binds | `loop.go` | Empty addr → `127.0.0.1:0` Listen | ✅ `TestEmptyOpsAddrStartsNoOpsServer` |
| 5 | readyz ignores draining | `pool.go` | Remove `draining` branch from ready fn | ✅ `TestReadyz503FromInstantStop` |
| 6 | Flip outcome counters | `metrics.go` | Success increments failed; error increments completed | ✅ `TestMetricsMiddlewareMovesExactlyOneCounterPerExecution`, `TestMetricsMiddlewareCountsAPanickingWorkerAsFailed` |

**Sensor depth**: expanded lightweight (6 mutations)
**Result**: 6/6 killed — sensor PASS ✅

---

## Interactive UAT Results

Not performed — backend / ops instrumentation; automated gates sufficient.

---

## Code Quality

| Principle | Status |
| --------- | ------ |
| Minimum code | ✅ |
| Surgical changes | ✅ |
| No scope creep | ✅ (SPA/CLI/OTel excluded as specified) |
| Matches patterns | ✅ (goleak lifecycle, memdriver+pgdriver Stats, AD-037 tuning) |
| Spec-anchored outcome check | ✅ (EDGE-01 + OBS-13.3 closed) |
| Per-layer coverage expectation | ✅ storage unit+integration; root unit; ops integration |
| Every test maps to AC/edge/Done-when | ✅ feature tests map; pre-existing suite untouched |
| Documented guidelines | ✅ `CLAUDE.md` (table-driven, `-race`, testcontainers, goleak) |

---

## Edge Cases

- [x] EDGE-01: Gather-before-first-refresh zeros — `seedConfiguredZeros` + discriminating test
- [x] EDGE-02 … EDGE-09: covered (see table)

---

## Gate Check

- **Gate commands**: `go build ./... && go vet ./...`; `go test -race ./...`; `go test -race -tags=integration ./...`; `golangci-lint v2 run`
- **Result**: all pass (0 failed). Integration required Docker (`all` permissions); unit suite Docker-free.
- **golangci-lint**: 0 issues
- **Test count before feature** (`main`): **200** `func Test`
- **Test count after feature** (`HEAD`): **251** `func Test`
- **Delta**: **+51** (includes EDGE-01 + Example compile coverage from fix commits)
- **Tasks baseline note**: tasks.md cited 202; measured main tip is 200 — no decrease
- **Skipped tests**: none observed
- **Failures**: none

---

## Fix Plans

None — prior Fix 1 (EDGE-01) and Fix 2 (OBS-13.3) verified closed.

---

## Requirement Traceability Update

| Requirement | Previous Status | New Status |
| ----------- | --------------- | ---------- |
| OBS-01 … OBS-12 | ✅ Verified (prior run) | ✅ Verified |
| OBS-13 | ❌ Needs Fix (OBS-13.3) | ✅ Verified |
| EDGE-01 | ❌ Needs Fix | ✅ Verified |
| EDGE-02 … EDGE-09 | ✅ Verified | ✅ Verified |

---

## Summary

**Overall**: ✅ Ready

**Spec-anchored check**: 38/38 story+edge ACs matched spec outcome; 0 gaps; OBS-11.5 satisfied via D-7 freshness
**Sensor**: 6/6 mutations killed (including EDGE-01 seed removal)
**Gate**: build/vet/unit/integration/lint all green (+51 tests vs main)

**What works**: Ops lifecycle, scrape≠Stats, refresh failure preservation, pre-first-refresh zero seeding, README snippet compile Example, healthz/readyz divergence, claimable-only age, middleware counters/histogram, pool gauges, goleak joins.

**Issues found**: none

**Next steps**: none for verification — feature ready to proceed in ship cycle.
