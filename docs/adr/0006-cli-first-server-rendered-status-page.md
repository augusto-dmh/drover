# ADR-0006: CLI-first inspection with one server-rendered status page

- **Date**: 2026-09-04
- **Status**: Accepted
- **Deciders**: Augusto de Melo Henriques
- **Tags**: scope, cli, ui

## Context and Problem Statement

Operators already inspect a live queue from the `drover` binary (`stats`, `jobs list`, `retry`, `cancel`). A browser is still useful for a demo and a glance at dead jobs. The Go queue ecosystem shows where that impulse goes wrong: asynqmon became a second product (355 commits, then abandonment); riverui is company-funded. Drover has one maintainer. The question is how much UI to ship without becoming a dashboard project.

## Decision Drivers

- RFC-0001 forbids a SPA dashboard; Cycle I is optional and droppable.
- RQ05: ~80% of demo value from one `html/template` page vs hundreds of commits of React.
- ADR-0004: stdlib `net/http` ServeMux, no web framework, no JS toolchain.
- Cycle F: the CLI is the primary inspection surface; `Inspector` is the operator API.

## Considered Options

- **A — Full SPA dashboard** (asynqmon / riverui class)
- **B — CLI only, no HTML** (drop Cycle I)
- **C — One `drover web` process serving a single auto-refreshing HTML page with retry/cancel**, templates in `embed.FS`, backed by `Inspector`

## Decision Outcome

Chosen option: **C**. `drover web` serves one page: queue depth and oldest-claimable age, a job table that defaults to `dead`, POST retry/cancel using the same Inspector semantics as the CLI. Auto-refresh is `<meta http-equiv="refresh">`. There is no JavaScript file, no second page, no enqueue form, and no application authentication — the default listen address is loopback (`127.0.0.1:7180`). Binding elsewhere is an explicit choice to expose operator mutations.

The page is not mounted on the worker ops port (`/metrics`, `/healthz`, `/readyz`). That listener stays metrics-only (ADR-0005).

### Positive Consequences

- Demo and 3am glance without a Node build or a second codebase.
- The scoping decision is itself visible: the page is small enough to read in one file.
- CSRF is Origin/Referer same-host on POST; missing both is 403.

### Negative Consequences

- A LAN-exposed `--listen` has no token/password. That is documented, not patched with a second auth system this cycle.
- Full page reload is the refresh model; there will be no live websocket view.

## Links

- Evidence: `docs/research/2026-07-22/rq05-product-scope.md`
- Roadmap: RFC-0001 Cycle I
- Related: ADR-0001 (scope brake), ADR-0004 (stdlib HTTP), ADR-0005 (ops port)
