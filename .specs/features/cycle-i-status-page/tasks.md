# Operator Status Page Tasks

## Execution Protocol (MANDATORY — do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its
Execute flow and Critical Rules.** Do not search for skill files by filesystem path.

**If the skill cannot be activated, STOP and tell the orchestrator — do not proceed without it.**

---

**Spec**: `.specs/features/cycle-i-status-page/spec.md`
**Design**: `.specs/features/cycle-i-status-page/design.md`
**Context**: `.specs/features/cycle-i-status-page/context.md`
**Status**: Approved

---

## Test Coverage Matrix

> Generated from codebase, project guidelines, and spec. Guidelines found: `CLAUDE.md`
> (table-driven, `t.Parallel`, `-race`, unit suite without Docker, goleak on lifecycle),
> `.github/workflows/ci.yml` (`go test -race ./...`, integration tags, golangci-lint).

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| GET `/` renderer | unit | WEB-02–WEB-05, WEB-06, WEB-11; Stats/ListJobs opts; empty; 500; escaped Args | `cmd/drover/web_test.go` | `go test -race ./cmd/drover/` |
| Filters + refresh | unit | WEB-04, WEB-10, 400s, meta URL omits flash | `cmd/drover/web_test.go` | `go test -race ./cmd/drover/` |
| POST retry/cancel | unit | WEB-07, WEB-08; fake Inspector ids; PRG Location | `cmd/drover/web_test.go` | `go test -race ./cmd/drover/` |
| CSRF / methods / ids | unit | WEB-12; POST without Origin/Referer must not call Inspector | `cmd/drover/web_test.go` | `go test -race ./cmd/drover/` |
| `runWeb` / `run` flags | unit | WEB-01, WEB-09; `--json`; bind fail; shutdown goleak | `cmd/drover/cmd_web_test.go` | `go test -race ./cmd/drover/` |
| README / ADR | none | Build + human review; usage lists `web` (covered in T5) | `README.md`, `docs/adr/` | build gate |

## Parallelism Assessment

| Test Type | Parallel-Safe? | Isolation Model | Evidence |
| --- | --- | --- | --- |
| Handler / flag unit | Yes | Fresh mux + `fakeInspector` per test | Existing `cmd/drover/*_test.go` uses `t.Parallel` |
| `runWeb` listen | Yes | `127.0.0.1:0` per test | Distinct listeners |
| Integration | n/a | No new Docker tests this cycle | Inspector pgdriver already covered |

## Gate Check Commands

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After each task | `go test -race ./cmd/drover/` |
| Full | Phase boundary | `go test -race ./...` |
| Build | Phase completion | `go build ./... && go vet ./...` |

**Before each phase-closing commit**, also run
`go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run`.
No `.sql` changes this cycle — do not regenerate sqlc.

**Environment**: Docker not required for Cycle I tests.

**Commit hygiene**: scoped Conventional Commit subjects (`feat(cli): …`);
no task/decision/cycle IDs; no AI attribution. After each commit inspect
`git log -1 --format=%B` and strip any `Co-authored-by` / generated-with trailer
before continuing.

---

## Execution Plan

### Phase 1: Read path (Sequential)

```
T1 → T2
```

### Phase 2: Mutations (Sequential)

```
T3 → T4
```

### Phase 3: Process + docs (Sequential)

```
T5 → T6
```

---

## Phase 1: Read path

### T1: GET `/` status page

**What**: Embedded `page.html`, `statusHandler` serving `GET /`: Stats tables,
job rows (default `ListJobs` state `dead`, limit 100), empty captions, 500 on
Inspector errors, HTML-escaped Args/Errors, retry/cancel forms by state, CSP
and content-type headers.
**Where**: `cmd/drover/web.go`, `cmd/drover/webui/page.html`, `cmd/drover/web_test.go`
**Depends on**: None
**Reuses**: `inspector`, `fakeInspector`, `JobState`, `ListJobsOpts`
**Requirement**: WEB-02, WEB-03 (default dead), WEB-05, WEB-06, WEB-11

**Done when**:

- [ ] `GET /` is 200 `text/html; charset=utf-8` with depths, ages, and listed jobs
- [ ] Default `ListJobs` uses `State=dead`, `Limit=100`
- [ ] Dead rows have retry+cancel forms; running/completed/cancelled have neither; available has cancel only
- [ ] `<script>` in Args appears escaped; CSP includes `script-src` absent or `'none'` via `default-src 'none'`
- [ ] Stats or list error → 500 HTML with escaped error text
- [ ] Empty stats and empty jobs → 200 with empty-state copy
- [ ] Quick gate green

**Tests**: unit
**Gate**: quick
**Commit**: `feat(cli): serve an Inspector-backed HTML status page`

### T2: Filters, validation, and meta refresh

**What**: Query `queue`/`state`/`limit`; `state=all` means unfiltered; invalid
state or limit → 400; GET filter form; `--refresh` duration on the handler
(default 5s meta, 0 omits); flash query codes render canned banners; meta URL
drops `notice`/`error`.
**Where**: `cmd/drover/web.go`, `cmd/drover/webui/page.html`, `cmd/drover/web_test.go`
**Depends on**: T1
**Reuses**: Inspector limit max 1000
**Requirement**: WEB-03 (explicit filters), WEB-04, WEB-08 (GET half), WEB-10

**Done when**:

- [ ] Table-driven tests for default dead, `state=all`, named state, queue, custom limit
- [ ] 400 on unknown state, non-positive or >1000 limit, non-integer limit
- [ ] Default refresh injects 5s meta whose url has filters and no flash
- [ ] Refresh 0 omits meta
- [ ] Known flash codes show canned text; unknown code omits banner
- [ ] Quick gate green

**Tests**: unit
**Gate**: quick
**Commit**: `feat(cli): filter and auto-refresh the status page`

---

## Phase 2: Mutations

### T3: POST retry and cancel

**What**: `POST /jobs/{id}/retry` and `.../cancel` call Inspector, 303 PRG with
coded flash and preserved hidden filter fields; `ErrNotFound` /
`ErrInvalidTransition` become error flashes rather than 500.
**Where**: `cmd/drover/web.go`, `cmd/drover/webui/page.html`, `cmd/drover/web_test.go`
**Depends on**: T2
**Reuses**: `RetryJob`, `CancelJob`, `parseJobID`, `errors.Is`
**Requirement**: WEB-07, WEB-08 (POST half)

**Done when**:

- [ ] Successful retry/cancel: Inspector called with id; 303 Location has notice code + id + filters
- [ ] Not found / invalid transition: 303 with error code; no 500
- [ ] Forms POST to the designed paths and include hidden filter fields
- [ ] Quick gate green

**Tests**: unit
**Gate**: quick
**Commit**: `feat(cli): retry and cancel jobs from the status page`

### T4: CSRF and bad requests

**What**: Same-origin Origin (else Referer) required on POST; missing/mismatch
→ 403 and Inspector unused; id ≤ 0 or non-numeric → 400; GET on mutation paths
→ 405; unknown path → 404 HTML.
**Where**: `cmd/drover/web.go`, `cmd/drover/web_test.go`
**Depends on**: T3
**Reuses**: `net/url` parse of Origin/Referer vs `r.Host`
**Requirement**: WEB-12

**Done when**:

- [ ] POST with matching Origin succeeds (existing T3 cases set Origin)
- [ ] POST with no Origin and no Referer: 403, retry/cancel id stays 0
- [ ] POST with Origin host ≠ `r.Host`: 403
- [ ] POST with only matching Referer allowed
- [ ] Bad id 400; GET mutation 405; `/nope` 404
- [ ] Quick gate green

**Tests**: unit
**Gate**: quick
**Commit**: `fix(cli): reject cross-origin status-page mutations`

---

## Phase 3: Process + docs

### T5: `drover web` command lifecycle

**What**: Wire `web` in `run`: `--json` exit 2; DSN required; parse `--listen`
(default `127.0.0.1:7180`) and `--refresh` (reject `0 < d < 1s`); `runWeb`
listens, prints URL, shuts down on ctx cancel without leaking; bind failure
exit 1; usage lists `web`.
**Where**: `cmd/drover/main.go`, `cmd/drover/cmd_web.go`, `cmd/drover/cmd_web_test.go`
**Depends on**: T4
**Reuses**: `withInspector`, `peelGlobals`, `ops.go` Shutdown join pattern
**Requirement**: WEB-01, WEB-09

**Done when**:

- [ ] `run([]string{"web","--json"}, ...)` exits 2 mentioning json
- [ ] Missing DSN exits 2
- [ ] `--refresh 500ms` exits 2; `--refresh 0` and `--refresh 2s` accepted
- [ ] `runWeb` on `127.0.0.1:0` prints `http://` URL; GET `/` works; cancel returns 0; goleak clean
- [ ] Occupied listen address (or invalid) exits 1
- [ ] Usage text includes `web`
- [ ] Full gate + lint green

**Tests**: unit
**Gate**: full
**Commit**: `feat(cli): add drover web listen and shutdown`

### T6: README and scope ADR

**What**: Document `drover web` in the README CLI section (flags, loopback,
not a dashboard). Add ADR-0006 recording CLI-first + one server-rendered page
versus a SPA (RQ05).
**Where**: `README.md`, `docs/adr/0006-cli-first-server-rendered-status-page.md`
**Depends on**: T5
**Reuses**: RQ05, RFC-0001 locked decisions
**Requirement**: Success criteria README bullet

**Done when**:

- [ ] README table includes `web` and states default listen + `--refresh`
- [ ] ADR-0006 accepted, links RFC-0001 and RQ05
- [ ] Build gate green

**Tests**: none
**Gate**: build
**Commit**: `docs(cli): document the operator status page`

---

## Parallel Execution Map

```
Phase 1 (Sequential):
  T1 ──→ T2

Phase 2 (Sequential):
  T2 complete, then:
  T3 ──→ T4

Phase 3 (Sequential):
  T4 complete, then:
  T5 ──→ T6
```

`[P]`: none — each task builds on the previous handler.

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1: GET `/` page | one handler + template | ✅ cohesive |
| T2: filters + refresh | same handler, query/meta | ✅ cohesive |
| T3: POST retry/cancel | two routes, one protocol | ✅ cohesive |
| T4: CSRF / 4xx | one policy | ✅ granular |
| T5: CLI lifecycle | command wiring | ✅ cohesive |
| T6: docs | README + ADR | ✅ granular |

---

## Diagram-Definition Cross-Check

| Task | Depends On (body) | Diagram Shows | Status |
| --- | --- | --- | --- |
| T1 | None | start | ✅ |
| T2 | T1 | T1 → T2 | ✅ |
| T3 | T2 | T2 → T3 | ✅ |
| T4 | T3 | T3 → T4 | ✅ |
| T5 | T4 | T4 → T5 | ✅ |
| T6 | T5 | T5 → T6 | ✅ |

---

## Test Co-location Validation

| Task | Code Layer | Matrix Requires | Task Says | Status |
| --- | --- | --- | --- | --- |
| T1 | GET renderer | unit | unit | ✅ |
| T2 | Filters + refresh | unit | unit | ✅ |
| T3 | POST retry/cancel | unit | unit | ✅ |
| T4 | CSRF / methods | unit | unit | ✅ |
| T5 | runWeb / flags | unit | unit | ✅ |
| T6 | README / ADR | none | none | ✅ |

---

## Requirement Traceability (updated)

| Requirement ID | Task |
| --- | --- |
| WEB-01 | T5 |
| WEB-02 | T1 |
| WEB-03 | T1, T2 |
| WEB-04 | T2 |
| WEB-05 | T1 |
| WEB-06 | T1 |
| WEB-07 | T3 |
| WEB-08 | T2, T3 |
| WEB-09 | T5 |
| WEB-10 | T2 |
| WEB-11 | T1 |
| WEB-12 | T4 |
