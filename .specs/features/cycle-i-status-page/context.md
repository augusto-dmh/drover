# Operator Status Page — Decision Record

Every decision below was made without a human in the loop, under the ship-cycle's
auto-decision rule. Each records the options considered with reasons for and against,
the choice, and why. They are mirrored into `.specs/STATE.md` as `AD-077`–`AD-083`.

**Gathered:** 2026-09-04
**Spec:** `.specs/features/cycle-i-status-page/spec.md`
**Status:** Ready for design

Decisions already binding and **not** relitigated: RFC-0001 (one `html/template`
page, no SPA), ADR-0004 (stdlib `net/http` ServeMux, `flag`, no extra HTTP
framework), AD-049 (stdlib flag dispatcher), AD-050 (Inspector, not Client),
AD-052/AD-053 (operator cancel/redrive semantics), AD-043 (ops bind is a
different listener on the worker).

---

## Feature Boundary

`drover web` serves one auto-refreshing HTML page: queue stats, a job table
(default `dead`), POST retry/cancel. No SPA, no second page, no enqueue form,
no app auth.

---

## D-1 — Where the UI lives

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `cmd/drover` + `embed.FS`; handlers call `inspector` | CLI is the operator surface (AD-049/AD-050); library stays free of HTML | Embedders who wanted a library UI must copy |
| (b) Root-package `StatusHandler` exported | Reusable in an embedder's mux | Pulls templates into the library; RFC named `drover web` |
| (c) Worker ops port grows a `/` HTML page | One listener | Mixes Prometheus with mutations; AD-043 bind-fail-starts-nothing is the wrong failure mode for an optional UI |

**Chosen: (a).** RFC command name and AD-050 both point here.

---

## D-2 — Listen address

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `--listen` default `127.0.0.1:7180` | Loopback by default; 7180 misses 8080 and the usual ops `:9090` | Operators must pass `--listen` to share on a LAN |
| (b) Default `:8080` / `0.0.0.0:8080` | Familiar | Accidental exposure of retry/cancel; 8080 collisions |
| (c) Always `:0` and print the port | Never collides | Operators cannot bookmark; bad for a status page |

**Chosen: (a).** Bind failure is exit 1 (same spirit as AD-043, applied to this process).

---

## D-3 — Authentication and CSRF

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) No app auth; CSRF = POST + Origin else Referer same-host as `r.Host`; missing both ⇒ 403 | Cycle F already refused a second auth layer; localhost CSRF is the remaining hole | No defense if you bind `0.0.0.0` on a public NIC |
| (b) Shared-secret query token | Extra lock when exposed | Token in logs/Referer; new operator ceremony this cycle does not need |
| (c) Skip CSRF because loopback | Simpler | A malicious page can POST to `127.0.0.1:7180` from the operator's browser |

**Chosen: (a).** README states that binding non-loopback is an explicit exposure of operator mutations.

---

## D-4 — Auto-refresh mechanism

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) `<meta http-equiv="refresh">`; `--refresh` duration; 0 disables; non-zero ≥ 1s | Zero JS; works with `script-src 'none'`; RFC auto-refresh | Full reload; loses nothing we persist (no client state) |
| (b) Tiny inline `fetch` + innerHTML | Smoother | Second client; RFC/RQ05 "zero JS toolchain" and "not a SPA" start sliding |
| (c) No refresh; operator hits reload | Simplest | RFC says auto-refreshing |

**Chosen: (a).** Meta-refresh target is the filter URL without flash params.

---

## D-5 — Mutation protocol

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) POST + 303 PRG; coded `notice`/`error` query | Idempotent reload; no XSS via error strings | Extra GET |
| (b) POST and re-render 200 | One round trip | Refresh resubmits the POST |
| (c) GET links for retry/cancel | Easy | Prefetch/crawlers mutate; CSRF-trivial |

**Chosen: (a).** Buttons only: retry on `dead`; cancel on available/scheduled/retryable/dead. No enqueue.

---

## D-6 — Jobs table default and filters

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Omit `state` ⇒ `dead`; `state=all` ⇒ unfiltered; GET form for queue/state/limit | Research "recent failures"; retry buttons on first paint | Must document `all` as the escape hatch |
| (b) Omit `state` ⇒ all jobs | Matches CLI default | First paint is a mixed list; dead jobs bury |
| (c) Hard-coded dead, no filters | Smaller | Cannot see `running` without CLI |

**Chosen: (a).** Limit default 100, max 1000 (Inspector).

---

## D-7 — `--json` with `web`

**Options**

| Option | For | Against |
| --- | --- | --- |
| (a) Exit 2, stderr explains | A blocking HTML server has no JSON mode; fail loud | Slightly stricter than ignoring |
| (b) Ignore `--json` | Friendly | Silent wrong expectation |
| (c) Serve `GET /` as JSON when `--json` | Two products | SPA-shaped API on the same command |

**Chosen: (a).**

---

## Implementation Decisions

### Process home
- CLI command, embedded templates, Inspector only (D-1).

### Listen and lifetime
- Default `127.0.0.1:7180`; `--listen`; `--refresh`; signal/`ctx` shutdown (D-2).

### Security
- No app auth; POST CSRF via Origin/Referer; CSP without scripts (D-3).

### Page
- One GET `/`; meta refresh; default-dead jobs; GET filter form (D-4, D-6).

### Mutations
- POST retry/cancel, PRG, coded flash (D-5).

### Agent's Discretion
- Visual design of the HTML/CSS (operator ledger, not a product dashboard).
- Exact canned flash copy.
- Whether CSS is inlined or a second embedded file (inline is fine).

### Declined / Undiscussed Gray Areas → Assumptions
- All seven D-n rows were auto-decided; none left unmarked. See spec ASM-01–ASM-12.

---

## Specific References

RQ05: "one Go `html/template` page (queue table, depth/latency, recent failures,
retry/cancel buttons) … zero JS toolchain." RFC-0001 Cycle I row. ADR-0004 HTTP
= stdlib ServeMux.

---

## Deferred Ideas

- Token auth when binding non-loopback.
- Job detail page.
- Enqueue form.
- SSE/websocket live updates.
- Serving the same templates from an embedder's mux.
