# Operator Status Page Specification

## Problem Statement

The CLI is the primary inspection surface (Cycle F), but a demo and a 3am glance
still want a browser: queue depth, oldest-claimable age, recent dead jobs, and
the same retry/cancel actions without leaving a terminal session's muscle
memory for flags. Cycle I ships that as one optional server-rendered page —
deliberately not a dashboard product — so the scoping decision itself stays
visible.

## Goals

- [ ] `drover web` serves one auto-refreshing HTML page backed by `Inspector`.
- [ ] The page shows per-queue depth and oldest-claimable age, plus a job list
      that defaults to recent failures (`dead`) with the same filters the CLI uses.
- [ ] Retry and cancel are POST form actions that reuse Inspector semantics.
- [ ] Zero JavaScript toolchain: `html/template` + `embed.FS` only.
- [ ] Default listen address is loopback so opening the port is not accidentally
      exposing operator mutations.

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
| --- | --- |
| SPA / client-side router / JS framework / Node build | Permanently out of scope (RFC-0001, RQ05). |
| Second page (job detail, settings, login) | RFC row is one page. |
| Enqueue from the browser | RFC lists retry/cancel; enqueue stays CLI. |
| Batch retry / retry-all-dead | ADR-0003 scoped redrive is per-id. |
| Cancelling a `running` job | AD-052 / AD-019; same refusal as the CLI. |
| Application authentication (tokens, basic auth) | Cycle F: whoever holds the DSN can operate; the web control is bind address. |
| TLS termination in `drover web` | Deployment concern; loopback default. |
| Serving this UI from `Client` / the ops port | Ops is Prometheus/`/healthz`/`/readyz` (AD-043); this is a CLI process. |
| Pagination beyond `ListJobs` limit | Inspector already caps at 1000; no new driver method. |
| Live websocket / SSE updates | Auto-refresh is a full page reload. |
| Discard/delete of rows | Cancel is the quarantine; no delete API. |

---

## Assumptions & Open Questions

Every ambiguity is resolved or recorded here — nothing is left silently unclear.

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| ASM-01 — Process home | `cmd/drover` with `embed.FS`; talks to `Inspector`, not `Client` | CLI-only operator surface; library stays worker-shaped (AD-050). | y |
| ASM-02 — Listen | `--listen` default `127.0.0.1:7180`; bind failure is fatal | Loopback so CSRF/cross-site hits require a local browser; 7180 avoids 8080/9090. | y |
| ASM-03 — Auth | None beyond DSN + bind address | Matches Cycle F; documented in README. | y |
| ASM-04 — CSRF | POST-only mutations; Origin (else Referer) must be same-host as `r.Host`; missing both ⇒ 403 | Localhost CSRF from a malicious page is real; no cookie/session store. | y |
| ASM-05 — Auto-refresh | `<meta http-equiv="refresh">`; `--refresh` default 5s; `0` disables; non-zero must be ≥ 1s | Zero JS; RFC "auto-refreshing". | y |
| ASM-06 — Mutations | POST `/jobs/{id}/retry` and `/jobs/{id}/cancel`, then 303 See Other (PRG) | Inspector `RetryJob` / `CancelJob`; no GET side effects. | y |
| ASM-07 — Jobs table default | `state=dead` when the query omits `state` | Research: "recent failures" plus retry buttons on the first paint. | y |
| ASM-08 — Filters | GET query `queue`, `state`, `limit` (default limit 100, max 1000) | Same numbers as `drover jobs list`. | y |
| ASM-09 — `--json` | Usage error (exit 2) with `web` | A blocking HTML server has no machine-readable mode. | y |
| ASM-10 — Flash | Coded query params (`notice`/`error` + `id`); meta-refresh URL drops them | Avoids XSS via query text; notice is not sticky across refresh. | y |
| ASM-11 — Shutdown | SIGINT/SIGTERM (or cancelled `ctx` in tests) → `http.Server.Shutdown`; then pool close | Matches CLI process lifetime; goleak-clean. | y |
| ASM-12 — Unknown paths | 404 HTML; GET on mutation paths is 405 | One page does not mean a catch-all. | y |

**Open questions:** none — all resolved or logged above.

---

## User Stories

### P1: Serve one status page ⭐ MVP

**User Story**: As an operator, I want `drover web` to open a single HTML page
that shows queue health and recent dead jobs, so I can see the same facts as
`drover stats` and `drover jobs list --state=dead` in a browser.

**Why P1**: This is the RFC deliverable. Without the read path there is nothing
to attach retry/cancel to.

**Acceptance Criteria**:

1. WHEN `drover web` is started with a usable database URL THEN the process SHALL
   bind `--listen` (default `127.0.0.1:7180`) and print the URL on stdout,
   including the scheme and address, before serving.
2. WHEN `GET /` is requested THEN the response SHALL be `200` with
   `Content-Type: text/html; charset=utf-8` and a document rendered from an
   embedded `html/template` (not a string built with `fmt` concatenation of job
   fields).
3. WHEN `GET /` succeeds THEN the document SHALL include every queue/state depth
   and every oldest-claimable age returned by `Inspector.Stats`.
4. WHEN `GET /` is requested without a `state` query parameter THEN
   `Inspector.ListJobs` SHALL be called with `State` equal to `dead` and
   `Limit` 100 (unless `limit` is supplied).
5. WHEN `GET /` is requested with `queue`, `state`, and `limit` query parameters
   THEN `ListJobs` SHALL be called with those filters (`limit` default 100,
   maximum 1000) and the rendered rows SHALL be exactly the returned jobs.
6. WHEN `GET /` is requested with `--refresh` left at default THEN the document
   SHALL contain a meta refresh whose delay is 5 seconds and whose URL is the
   current filters without `notice` or `error`.
7. WHEN `--refresh 0` is set THEN the document SHALL NOT contain a meta refresh.
8. WHEN `Inspector.Stats` or `Inspector.ListJobs` returns an error THEN `GET /`
   SHALL respond `500` with an HTML body that includes the error text escaped.

**Independent Test**: `httptest` against the handler with a fake `inspector`;
assert status, content-type, ListJobs opts, and HTML substrings. No Docker.

---

### P1: Retry and cancel from the page ⭐ MVP

**User Story**: As an operator, I want retry and cancel buttons on the page so
that I can redrive a dead job or cancel a waiting/dead job without switching to
the CLI.

**Why P1**: RFC names retry/cancel actions as the cycle's write surface.

**Acceptance Criteria**:

1. WHEN a listed job is in state `dead` THEN the row SHALL include a POST form
   whose action is `/jobs/{id}/retry`.
2. WHEN a listed job is in `available`, `scheduled`, `retryable`, or `dead`
   THEN the row SHALL include a POST form whose action is `/jobs/{id}/cancel`.
3. WHEN a listed job is in `running`, `completed`, or `cancelled` THEN the row
   SHALL include neither retry nor cancel form.
4. WHEN `POST /jobs/{id}/retry` succeeds THEN the handler SHALL call
   `Inspector.RetryJob` with that id and respond `303` with `Location` pointing
   at `GET /` preserving `queue`/`state`/`limit` and a coded `notice=retried`
   plus `id`.
5. WHEN `POST /jobs/{id}/cancel` succeeds THEN the handler SHALL call
   `Inspector.CancelJob` with that id and respond `303` with `Location`
   containing `notice=cancelled` plus `id`, preserving filters.
6. WHEN `RetryJob` or `CancelJob` returns an error wrapping `ErrNotFound` THEN
   the handler SHALL respond `303` to `GET /` with `error=not_found` and `id`,
   without claiming success.
7. WHEN `RetryJob` or `CancelJob` returns an error wrapping
   `ErrInvalidTransition` THEN the handler SHALL respond `303` with
   `error=invalid_transition` and `id`.
8. WHEN a subsequent `GET /` includes a coded `notice` or `error` THEN the
   document SHALL show a banner whose text is the canned message for that code
   (not the raw query string interpolated as HTML), and the meta-refresh URL
   SHALL omit `notice` and `error`.

**Independent Test**: fake inspector records the id; httptest POST then follow
or inspect Location; GET with flash codes asserts banner text.

---

### P1: Bind, flags, and process lifetime ⭐ MVP

**User Story**: As an operator, I want `drover web` to take `--listen` and
`--refresh`, refuse `--json`, and stop cleanly on SIGINT, so the command fits
the existing CLI and does not leak goroutines.

**Why P1**: A page that cannot be aimed at loopback, or that ignores Ctrl-C, is
not shippable.

**Acceptance Criteria**:

1. WHEN `drover web --json` is invoked THEN the process SHALL exit 2 without
   binding a port, and stderr SHALL mention that `--json` is not valid with
   `web`.
2. WHEN `--listen` is omitted THEN the server SHALL attempt `127.0.0.1:7180`.
3. WHEN `--refresh` is set to a duration greater than 0 and less than 1s THEN
   the process SHALL exit 2 without serving.
4. WHEN `--refresh` is set to 0 or to a duration ≥ 1s THEN the process SHALL
   accept it.
5. WHEN the listen address cannot be bound THEN the process SHALL exit 1 and
   print the bind error to stderr.
6. WHEN the serve context is cancelled (SIGINT in production; test `ctx`) THEN
   `runWeb` SHALL return 0 after `Shutdown`, and SHALL NOT leak the serve
   goroutine.
7. WHEN `drover web` is invoked without a database URL THEN the process SHALL
   exit 2 with the same missing-DSN message as other commands.
8. WHEN `printUsage` runs THEN it SHALL list `web` among commands.

**Independent Test**: flag-parse unit tests; `runWeb` with `127.0.0.1:0` and a
cancelled context; `run([]string{"web","--json"}, ...)`.

---

### P2: Filter controls on the page

**User Story**: As an operator, I want to change queue, state, and limit from
the page so that I can inspect `running` or a named queue without restarting
the command.

**Why P2**: Default-dead is the demo; filters are how the page covers `jobs list`.

**Acceptance Criteria**:

1. WHEN `GET /` succeeds THEN the document SHALL include a GET form with
   fields `queue`, `state`, and `limit`.
2. WHEN `state` is a known `JobState` or empty (meaning default `dead` on omit,
   not "all states") THEN listing proceeds. WHEN `state=all` is supplied THEN
   `ListJobs` SHALL be called with empty `State` (no filter).
3. WHEN `state` is neither empty, nor `all`, nor a known job state THEN
   `GET /` SHALL respond `400` with HTML naming the invalid state.
4. WHEN `limit` is greater than 1000 or is not a positive integer THEN
   `GET /` SHALL respond `400`.

**Independent Test**: table-driven query strings against the handler.

---

## Edge Cases

- WHEN job `Args` or `Errors` contain HTML / script text THEN the page SHALL
  show them escaped (a literal `<script>` in the HTML source as `&lt;script&gt;`
  or equivalent template escape); no `template.HTML` for those fields.
- WHEN `ListJobs` returns an empty slice THEN the page SHALL be 200 and say
  that no jobs match, not 404.
- WHEN `Stats` returns empty depths and empty ages THEN the page SHALL be 200
  and render empty tables (or an explicit empty caption), not 500.
- WHEN `POST /jobs/{id}/retry` or `cancel` has a missing or non-same-host
  Origin and Referer THEN the handler SHALL respond `403` and SHALL NOT call
  Inspector.
- WHEN `POST /jobs/0/retry` or a non-numeric id THEN the handler SHALL
  respond `400` and SHALL NOT call Inspector.
- WHEN `GET /jobs/{id}/retry` or `GET /jobs/{id}/cancel` THEN the handler
  SHALL respond `405`.
- WHEN `GET /nope` THEN the handler SHALL respond `404`.
- WHEN POST bodies include hidden `queue`/`state`/`limit` THEN PRG Location
  SHALL carry those filters so the operator stays on the same view.
- WHEN a flash `notice` or `error` code is unknown THEN the banner SHALL be
  omitted (do not echo the code as unsanitized HTML beyond a canned "unknown
  result" string, itself a Go string constant).

---

## Implicit-requirement dimensions

| Dimension | Resolution |
| --- | --- |
| Input validation & bounds | Job id > 0; limit 1–1000; state enum or `all`; refresh 0 or ≥ 1s; listen via `net.Listen`. |
| Failure / partial-failure | Stats/list errors → 500; mutation Inspector errors → PRG flash, not a 500 that looks like success. |
| Idempotency / retry | Second retry/cancel is `ErrInvalidTransition` → flash; Inspector remains the source of truth. |
| Auth boundaries & rate limits | No app auth; loopback default; no rate limiter (single operator). CSRF Origin/Referer. |
| Concurrency / ordering | Two tabs racing Inspector: one wins, one flashes invalid_transition. |
| Data lifecycle / expiry | N/A — no new rows or retention policy. |
| Observability | Stdout URL; Inspector errors in HTML/stderr via existing CLI mapping; no new Prometheus series. |
| External-dependency failure | Database errors on GET are 500; Start/bind failure is process exit. |
| State-transition integrity | Delegated entirely to Inspector (AD-052–AD-053); the page does not invent transitions. |

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| WEB-01 | P1: Serve page (bind + URL) | T5 | Verified |
| WEB-02 | P1: Serve page (GET HTML + Stats) | T1 | Verified |
| WEB-03 | P1: Serve page (default dead list + filters) | T1, T2 | Verified |
| WEB-04 | P1: Serve page (meta refresh) | T2 | Verified |
| WEB-05 | P1: Serve page (Stats/List 500) | T1 | Verified |
| WEB-06 | P1: Retry/cancel buttons by state | T1 | Verified |
| WEB-07 | P1: POST retry/cancel PRG | T3 | Verified |
| WEB-08 | P1: Flash codes + refresh strips flash | T2, T3 | Verified |
| WEB-09 | P1: Flags, --json, bind fail, shutdown | T5 | Verified |
| WEB-10 | P2: Filter form + state=all + 400s | T2 | Verified |
| WEB-11 | Edge: HTML escape Args/Errors | T1 | Verified |
| WEB-12 | Edge: CSRF 403, bad id 400, GET 405, 404 | T4 | Verified |

**Coverage:** 12 total, 12 mapped to tasks, 0 unmapped.

---

## Success Criteria

- [ ] An operator can run `drover web`, open the printed URL, see depths and dead jobs, retry one, and watch the row leave `dead` after refresh.
- [ ] No JavaScript file is served; CSP `script-src 'none'` (or equivalent absence of script) on `GET /`.
- [ ] Unit tests cover the handler without Docker.
- [ ] README documents `web`, the loopback default, and that this is not a dashboard product.
