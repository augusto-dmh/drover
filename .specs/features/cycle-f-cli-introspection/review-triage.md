# PR #11 Review Triage

Triage of review comments against the code as it exists on `feat/cli-introspection`.
Comments will be deleted in Stage 6; this file is the surviving record.

| # | Source | File:line | Verdict | Action | Rationale |
|---|---|---|---|---|---|
| 1 | inline 3741282060 | `inspector.go:134` | real | fix | Nil/`&InsertOpts{}` success path for default queue is undocumented by tests; forgetting `defaultQueue` would still pass. |
| 2 | inline 3741282089 | `cmd/drover/fake_inspector_test.go:51` | real | fix | Fake never records cancel/retry id; wrong-id wiring would pass. |
| 3 | inline 3741282103 | `cmd/drover/format.go:64` | real | fix | `parseJobID` non-numeric/≤0 → exit 2 is untested. |
| 4 | inline 3741282132 | `cmd/drover/cmd_retry_cancel.go:35` | real | fix | Cancel missing NotFound / missing-id / JSON coverage that retry already has. |
| 5 | inline 3741282150 | `cmd/drover/cmd_jobs.go:34` | real | fix | `listErr` on fake unused; list error→exit 1 untested. |
| 6 | inline 3741282166 | `cmd/drover/main.go:95` | real | fix | `open` failure branch never executed in tests. |
| 7 | inline 3741282445 | `internal/pgdriver/pgdriver.go:298` | real | fix | TOCTOU on refusal re-read can return InvalidTransition when the row is now eligible; retry the UPDATE once when re-read shows an allowed state (safe, no lease fight). Same for memdriver if applicable. |
| 8 | inline 3741283370 | `queries.sql:93` | real | won't-fix | True that OR filters weaken index use; ASM-09 / design accepted `ORDER BY id DESC` without a new index for this cycle. Fetch/list tuning belongs with the next hardening cycle. |
| 9 | inline 3741283408 | `inspector.go:95` | real | fix | Cap `ListJobs` at 1000 (CLI + Inspector); reject larger. Bounds memory without changing the default of 100. |
| 10 | inline 3741284541 | `inspector.go:176` | real | fix | Double-`%w` keeps `driver:` in the public error string and `errors.Is` chain; use `%v` for driver detail + `%w` for root sentinel. |
| 11 | inline 3741284566 | `inspector.go:180` | real | fix | Default branch should prefix `drover:` like sibling methods. |
| 12 | inline 3741284610 | `cmd/drover/inspector.go:15` | real | fix | Drop unused `GetJob` from the CLI-local interface. |
| 13 | inline 3741285931 | `memdriver.go:241` | real | fix | Reject `Limit <= 0` in both adapters so they agree when a caller bypasses Inspector. |
| — | issue 5227443557 | requirements summary | n/a | won't-fix | Meta summary, not a defect. |
| — | issue 5227459083 | review summary | n/a | won't-fix | Meta summary, not a defect. |

**Counts:** 13 inline findings — 12 real/fix, 1 real/won't-fix (list index tuning). 2 issue comments — meta, no code change.
