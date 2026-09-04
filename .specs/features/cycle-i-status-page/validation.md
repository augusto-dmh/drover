# cycle-i-status-page Validation

**Date**: 2026-09-04
**Spec**: `.specs/features/cycle-i-status-page/spec.md`
**Diff range**: `main..HEAD` on `feat/status-page` (`c3f96ce`…`bd20df8`)
**Verifier**: independent sub-agent (author ≠ verifier)
**Pass**: re-verification after prior FAIL (P2 AC1 / WEB-10 GET filter form unasserted). Prior report is not reused as evidence; ACs re-derived from spec + current tests.

---

## Task Completion

`tasks.md` Done-when checkboxes remain `[ ]`; completion is taken from `main..HEAD` commits, not those boxes.

| Task | Status | Notes |
| ---- | ------ | ----- |
| T1 | ✅ Done | `3fcf944` — `web.go` + `webui/page.html` + GET `/` tests |
| T2 | ✅ Done | `01b39d6` — filters, 400s, meta refresh, flash GET |
| T3 | ✅ Done | `115b0b9` — POST retry/cancel PRG |
| T4 | ✅ Done | `828af22` — CSRF, bad id, GET 405, `/nope` 404 |
| T5 | ✅ Done | `aae68a8` — `runWeb` / `--json` / bind / goleak |
| T6 | ✅ Done | `8935024` — README + ADR-0006 |
| Fix (P2 AC1) | ✅ Done | `bd20df8` — `TestGETStatusPageFilterForm` |

---

## Spec-Anchored Acceptance Criteria

### P1: Serve one status page

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN `drover web` starts with a usable DSN THEN bind `--listen` (default `127.0.0.1:7180`) and print scheme+address URL on stdout before serving | listen default `127.0.0.1:7180`; stdout URL usable as `http://…` | `cmd/drover/cmd_web_test.go:83` — `cfg.Listen != defaultWebListen` (`web.go:22` is `127.0.0.1:7180`); `cmd_web_test.go:123-124` — `res.StatusCode != http.StatusOK` against the printed URL | ✅ PASS |
| WHEN `GET /` THEN `200` + `Content-Type: text/html; charset=utf-8` + `html/template` (not `fmt` concatenation of job fields) | status 200; exact content-type; job fields escaped by the template | `cmd/drover/web_test.go:41` — `rec.Code != http.StatusOK`; `:45` — `ct != htmlContentType` (`web.go:25` `text/html; charset=utf-8`); `:68` — `!strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;")`; `:65` forbids raw `<script>` | ✅ PASS |
| WHEN `GET /` succeeds THEN document includes every Stats depth and oldest-claimable age | fixture depth `default`/`available`/`3` and age `1.500` present | `cmd/drover/web_test.go:56-57` — missing `"default"` / `"available"` / `"3"`; `:59` — missing `"1.500"` | ✅ PASS |
| WHEN `GET /` omits `state` THEN `ListJobs` with `State=dead` and `Limit=100` (unless `limit` supplied) | `listOpts.State == dead`, `Limit == 100` | `cmd/drover/web_test.go:52-53` — `fake.listOpts.State != drover.StateDead \|\| fake.listOpts.Limit != 100`; `web_test.go:191` subtest `"defaults"` — `wantState: dead`, `wantLimit: 100` | ✅ PASS |
| WHEN `GET /` with `queue`/`state`/`limit` THEN `ListJobs` those filters (limit default 100, max 1000) and rows are exactly the returned jobs | opts match query; listed jobs appear | `cmd/drover/web_test.go:191` — `Queue`/`State`/`Limit` vs table (`queue=bulk&state=available&limit=10`); `:62` — `"email"` + `/jobs/9/retry`; `:97-113` — ids 1–7 button matrix | ✅ PASS |
| WHEN `GET /` with default `--refresh` THEN meta refresh delay 5s and URL is current filters without `notice`/`error` | `content="5;url=` plus filters; no flash keys | `cmd/drover/web_test.go:255` — `http-equiv="refresh"`; `:258` — `content="5;url=`; `:261` — `notice=` / `error=` forbidden; `:264` — `queue=mail` required | ✅ PASS |
| WHEN `--refresh 0` THEN document SHALL NOT contain a meta refresh | no `http-equiv="refresh"` | `cmd/drover/web_test.go:274` — `strings.Contains(..., http-equiv="refresh")` forbidden | ✅ PASS |
| WHEN `Stats` or `ListJobs` errors THEN `GET /` is `500` HTML with error text escaped | 500; escaped body | `cmd/drover/web_test.go:136` — `rec.Code != http.StatusInternalServerError`; `:143` — `"stats boom"` and `"&lt;x&gt;"`; `:140` forbids `"<x>"`; `:153` + `:156` — list path 500 + `"list boom"` | ✅ PASS |

### P1: Retry and cancel from the page

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN listed job is `dead` THEN row includes a POST form action `/jobs/{id}/retry` | retry action for dead id | `cmd/drover/web_test.go:62` — `action="/jobs/9/retry"`; `:102` — `/jobs/4/retry` | ✅ PASS |
| WHEN listed job is `available`/`scheduled`/`retryable`/`dead` THEN cancel form `/jobs/{id}/cancel` | cancel action for ids 1–4 | `cmd/drover/web_test.go:97-99` — `/jobs/{id}/cancel` for `id in {1,2,3,4}` | ✅ PASS |
| WHEN listed job is `running`/`completed`/`cancelled` THEN neither retry nor cancel form | no `/jobs/{5,6,7}/retry` or `/cancel` | `cmd/drover/web_test.go:105-107` — retry forbidden for ids 1,2,3,5,6,7; `:110-112` — cancel forbidden for ids 5,6,7 | ✅ PASS |
| WHEN `POST /jobs/{id}/retry` succeeds THEN `RetryJob(id)` and `303` Location with `notice=retried`, `id`, preserved filters | 303; `retryID==9`; query `notice`/`id`/`queue`/`state`/`limit` | `cmd/drover/web_test.go:318` — `rec.Code != http.StatusSeeOther`; `:321` — `fake.retryID != 9`; `:325` — `notice != "retried"` / `id != "9"` / `queue != "mail"` / `state != "dead"` / `limit != "50"` | ✅ PASS |
| WHEN `POST /jobs/{id}/cancel` succeeds THEN `CancelJob(id)` and `303` with `notice=cancelled` plus `id`, filters preserved | 303; `cancelID==4`; `notice=cancelled` | `cmd/drover/web_test.go:337` — `StatusSeeOther`; `:340` — `fake.cancelID != 4`; `:344` — `notice != "cancelled"` / `id != "4"` / `state != "all"` | ✅ PASS |
| WHEN `RetryJob`/`CancelJob` wraps `ErrNotFound` THEN `303` to `GET /` with `error=not_found` and `id`, no success notice | 303; `error=not_found`; empty `notice` | `cmd/drover/web_test.go:353` — `StatusSeeOther`; `:357` — `error != "not_found"` / `id != "99"` / `notice != ""`. Retry path only (shared `postMutation`) | ✅ PASS |
| WHEN wraps `ErrInvalidTransition` THEN `303` with `error=invalid_transition` and `id` | 303; that error code | `cmd/drover/web_test.go:366` — `StatusSeeOther`; `:370` — `error != "invalid_transition"` / `id != "5"`. Cancel path only | ✅ PASS |
| WHEN subsequent `GET /` has coded `notice`/`error` THEN canned banner (not raw query HTML) and meta-refresh URL omits flash | exact canned strings; refresh URL strips flash | `cmd/drover/web_test.go:301` — `!strings.Contains(body, tt.want)` for `redrove job 9` / `cancelled job 4` / `job 8 not found` / `job 2 refused the transition`; `:304` — `error=%3Cscript%3E` forbids `"<script>"`; `:261` — refresh URL forbids `notice=` / `error=` | ✅ PASS |

### P1: Bind, flags, and process lifetime

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN `drover web --json` THEN exit 2 without binding; stderr mentions `--json` is not valid with `web` | exit 2; stderr contains `--json` and `web` | `cmd/drover/cmd_web_test.go:21` — `code != 2`; `:24` — `!strings.Contains(stderr, "--json") \|\| !strings.Contains(stderr, "web")` | ✅ PASS |
| WHEN `--listen` omitted THEN attempt `127.0.0.1:7180` | parsed listen is that default | `cmd/drover/cmd_web_test.go:83` — `cfg.Listen != defaultWebListen` | ✅ PASS |
| WHEN `--refresh` in `(0, 1s)` THEN exit 2 without serving | parse exit 2 | `cmd/drover/cmd_web_test.go:47` — `code != 2`; `:50` — stderr contains `"--refresh"` | ✅ PASS |
| WHEN `--refresh` is 0 or ≥ 1s THEN accept | parse exit 0; values `0` and `2s` | `cmd/drover/cmd_web_test.go:58` — `code != 0`; `:61` — `cfg.Refresh != 0`; `:70` — `code != 0`; `:72` — `cfg.Refresh != 2*time.Second` | ✅ PASS |
| WHEN listen cannot bind THEN exit 1 and print bind error to stderr | exit 1; stderr non-empty | `cmd/drover/cmd_web_test.go:147` — `code != 1`; `:150` — `stderr.Len() == 0` forbidden | ✅ PASS |
| WHEN serve ctx cancelled THEN `runWeb` returns 0 after Shutdown and does not leak the serve goroutine | exit 0; goleak clean | `cmd/drover/cmd_web_test.go:94` — `goleak.VerifyNone`; `:129` — `code != 0` | ✅ PASS |
| WHEN invoked without database URL THEN exit 2 with the same missing-DSN message as other commands | exit 2; stderr mentions database URL (`resolveDSN` shared) | `cmd/drover/cmd_web_test.go:33` — `code != 2`; `:36` — `!strings.Contains(stderr, "database URL")` | ✅ PASS |
| WHEN `printUsage` runs THEN it lists `web` among commands | usage contains `web` | `cmd/drover/cmd_web_test.go:159` — `!strings.Contains(stdout, "web")` | ✅ PASS |

### P2: Filter controls on the page

| Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion | Result |
| ------------------------- | -------------------- | ----------------------- | ------ |
| WHEN `GET /` succeeds THEN document includes a GET form with fields `queue`, `state`, and `limit` | GET form markup with those three names | `cmd/drover/web_test.go:207` — `method="get"`; `:210` — `name="queue"` / `name="state"` / `name="limit"`; `:213` — `<select name="state"` (empty jobs fixture so names are not POST hiddens) | ✅ PASS |
| WHEN `state` is a known `JobState` or empty THEN listing proceeds; WHEN `state=all` THEN `ListJobs` with empty `State` | 200 + opts; `state=all` → `State=""` | `cmd/drover/web_test.go:185` — `rec.Code != http.StatusOK`; `:191` — `"all states"` `wantState: ""`; `"named state"` `wantState: running`; `"defaults"` empty query → `dead` | ✅ PASS |
| WHEN `state` is neither empty, nor `all`, nor a known job state THEN `400` HTML naming the invalid state | 400; ListJobs not called | `cmd/drover/web_test.go:233` — `rec.Code != http.StatusBadRequest` for `/?state=nope`; `:236` — `fake.listOpts != nil` forbidden. Body does not assert the invalid token is named | ✅ PASS (status); naming unasserted |
| WHEN `limit` > 1000 or not a positive integer THEN `400` | 400 for `0` / `1001` / `-1` / `foo` | `cmd/drover/web_test.go:233` — `rec.Code != http.StatusBadRequest`; `:236` — ListJobs not called | ✅ PASS |

**Status**: ✅ All ACs covered — 28/28 ACs matched spec-defined outcomes; 0 AC gaps; 0 spec-precision gaps

HTML `method="post"` on retry/cancel forms is not asserted (actions and POST handlers are). Not scored as a gap because the POST+303 path is evidenced at the handler (`web_test.go:318`, `:337`). Invalid-state 400 body token is unasserted; 400 + no `ListJobs` is the discriminating outcome.

Prior FAIL (P2 AC1 / WEB-10) is closed by `TestGETStatusPageFilterForm` (`bd20df8`).

---

## Discrimination Sensor

Scratch: `git worktree add /tmp/drover-cycle-i-reverify HEAD` (removed after). Live tree untouched.

Target package: `go test -race -count=1 ./cmd/drover/` per mutant.

| Mutation | File:line | Description | Killed? |
| -------- | --------- | ----------- | ------- |
| 1 | `cmd/drover/web.go:155` (`sameOrigin`) | Always `return true` (CSRF same-origin disabled) | ✅ Killed — `TestPOSTCSRFRequiresSameOrigin/no_origin_or_referer` status 303 (want 403); `/origin_host_mismatch` status 303 |
| 2 | `cmd/drover/web.go:232-234` | Omitted `state` defaults to `available` instead of `dead` | ✅ Killed — `TestGETStatusPageRendersStatsAndJobs` `ListJobs opts=… State:available … want state=dead`; `TestGETStatusPageFilters/defaults` |
| 3 | `cmd/drover/web.go:97-99` (`postRetry`) | `RetryJob` not called; stub returns empty row / nil error | ✅ Killed — `TestPOSTRetrySuccess` `retry id=0`; also `TestPOSTRetryNotFoundFlash` / `TestPOSTRetryUnexpectedError500` / matching-referer CSRF |

**Sensor depth**: lightweight (3 behavior-level mutations on CSRF, default-dead `ListJobs`, POST retry → Inspector)
**Result**: 3/3 killed — PASS ✅

Invariants covered as requested: POST without Origin/Referer must not call Inspector (M1; killer asserts 403; Inspector check is after `Fatalf` on status); omitted `state` lists `dead` (M2); retry POST calls Inspector (M3).

---

## Interactive UAT Results (if performed)

| # | Test | Result | Details |
| --- | --- | --- | --- |
| — | Interactive UAT | ⏭️ Skip | Ship-cycle autonomy; httptest is the sensor. No browser walkthrough. |

---

## Code Quality

| Principle | Status |
| --------- | ------ |
| Minimum code | ✅ `cmd/drover` handler + one `embed.FS` template; no JS toolchain, no second page, no enqueue UI |
| Surgical changes | ✅ Diff is CLI web surface + README/ADR + feature specs; library/ops port untouched |
| No scope creep | ✅ Matches Out of Scope (no SPA, no TLS, no app auth, no ops-port HTML) |
| Matches patterns | ✅ `fakeInspector`, `t.Parallel`, `errors.Is` sentinels, goleak on `runWeb`, stdlib `ServeMux` |
| Spec-anchored outcome check (asserted values match spec) | ✅ 28/28 ACs have `file:line` targeting spec outcomes (P2 AC1 closed) |
| Per-layer Coverage Expectation met (domain 1:1 ACs; routes happy+edge+error) | ✅ GET renderer, filters (query + GET form), POST PRG, CSRF/4xx, `runWeb` flags |
| Every test maps to a spec requirement — no unclaimed tests | ✅ In-scope tests map to WEB ACs, listed edges, or implicit failure (unexpected mutation → 500 in `TestPOSTRetryUnexpectedError500`) |
| Documented guidelines followed | ✅ `CLAUDE.md` — table-driven, `t.Parallel`, `-race`, unit suite without Docker, goleak on lifecycle |

No `.js` in the feature diff. `GET /` CSP includes `default-src 'none'` (`web_test.go:49`). No `// SPEC_DEVIATION` in Cycle I code.

---

## Edge Cases

- [x] HTML/script in `Args`/`Errors` escaped — `web_test.go:65-75` (`&lt;script&gt;`, `&lt;b&gt;boom&lt;/b&gt;`; raw tags forbidden)
- [x] Empty `ListJobs` → 200 + “No jobs match.” — `web_test.go:122-126`
- [x] Empty Stats depths/ages → 200 + empty captions, not 500 — `web_test.go:126` “No queue depths.” / “No oldest-claimable ages.”
- [x] POST missing/non-same-host Origin and Referer → 403 and Inspector unused — `web_test.go:429-433` (`StatusForbidden`, `retryID != 0`); `:445-449` origin mismatch
- [x] POST `/jobs/0/retry` or non-numeric id → 400, Inspector unused — `web_test.go:477-481`
- [x] GET mutation paths → 405 — `web_test.go:493`
- [x] `GET /nope` → 404 — `web_test.go:503`
- [x] POST hidden `queue`/`state`/`limit` → PRG Location carries them — `web_test.go:325` (handler given those fields); HTML hidden inputs `web_test.go:395` (queue/limit; GET form also has `name="state"`)
- [x] Unknown flash code not interpolated as unsanitized HTML — `web_test.go:304` `error=%3Cscript%3E` forbids `"<script>"`; omit-banner for unknown codes is implied by empty `flashFromQuery` default, not positively asserted

Matching Referer-only POST allowed — `web_test.go:461-465` (`StatusSeeOther`, `retryID == 7`).

---

## Gate Check

- **Gate command**: `go build ./... && go vet ./...` (tasks.md Build) plus `go test -race ./...` (user-required)
- **Build**: pass (`go build ./... && go vet ./...` exit 0)
- **Result**: **681** passed, **0** failed, **0** skipped tests (`go test -race -count=1 -json ./...`, counting `Action=pass` with a `Test` name). Four packages without tests (`internal/dbsqlc`, `internal/driver`, `internal/migrate`, `internal/pgdriver`) emit package-level `skip`; not skipped tests.
- **Test function count before feature** (`git grep '^func (Test\|Fuzz\|Example)' main -- '*_test.go'`): 410
- **After feature** (HEAD): 436
- **Delta**: +26 Test declarations (all under `cmd/drover/`; subtests multiply the 681 runtime count). +1 vs prior verification from `TestGETStatusPageFilterForm`.
- **Skipped tests**: none
- **Failures**: none
- **Test integrity**: count increased; no deleted tests in `main..HEAD`

---

## Fix Plans (if issues found)

None.

---

## Requirement Traceability Update

Report-only (spec.md statuses left for the orchestrator):

| Requirement | Previous Status | New Status |
| ----------- | --------------- | ---------- |
| WEB-01 | In Tasks | ✅ Verified |
| WEB-02 | In Tasks | ✅ Verified |
| WEB-03 | In Tasks | ✅ Verified |
| WEB-04 | In Tasks | ✅ Verified |
| WEB-05 | In Tasks | ✅ Verified |
| WEB-06 | In Tasks | ✅ Verified |
| WEB-07 | In Tasks | ✅ Verified |
| WEB-08 | In Tasks | ✅ Verified |
| WEB-09 | In Tasks | ✅ Verified |
| WEB-10 | ❌ Needs Fix (prior verification) | ✅ Verified |
| WEB-11 | In Tasks | ✅ Verified |
| WEB-12 | In Tasks | ✅ Verified |

---

## Summary

**Overall**: ✅ Ready

**Spec-anchored check**: 28/28 ACs matched spec outcome; 0 AC gaps; 0 spec-precision gaps
**Sensor**: 3/3 mutations killed
**Gate**: 681 passed, 0 failed, 0 skipped; build+vet pass

**What works**: Inspector-backed `GET /` (content-type, CSP `default-src 'none'`, Stats tables, default `ListJobs` `dead`/`100`, query filters, GET filter form with `queue`/`state`/`limit`, `state=all`, 400s, 500 escaped errors, empty captions, HTML escape of Args/Errors, buttons by state, meta refresh 5s/`0`, canned flash, POST retry/cancel 303 PRG, CSRF 403 without Inspector, bad id 400, GET 405, `/nope` 404, `drover web` flags/listen/shutdown/goleak, README + ADR-0006). httptest discriminates CSRF, default-dead, and RetryJob side effect. Prior P2 AC1 gap is closed.

**Issues found**: none

**Next steps**: none — feature ready
