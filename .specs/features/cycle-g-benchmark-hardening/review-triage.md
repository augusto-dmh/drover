# PR #13 Review Triage

Triage of review comments against the code as it exists on `feat/batch-insert-notify-bench`.
Comments will be deleted in Stage 6; this file is the surviving record.

| # | Source | File:line | Verdict | Action | Rationale |
|---|---|---|---|---|---|
| 1 | inline 3792940775 | `memdriver.go:55` | real | fix | Concurrent `InsertMany` writers are a new `nextID` path; sequential tests would miss a duplicate-ID race. Cheap to add next to existing concurrent fetch tests. |
| 2 | inline 3792940807 | `client_test.go:1220` | real | fix | Write-failure coverage uses `Config{}` so a mistaken `nudge` on the error path would still pass. Enable `NotifyWakeup` and assert an empty wake channel. |
| 3 | inline 3792940834 | `cmd/drover-bench/main_test.go:125` | real | fix | `jobs/sec=` as a prefix does not pin the computed rate; 1234 jobs in 2s must print `617.00`. |
| 4 | inline 3792940850 | `cmd/drover-bench/main_test.go:163` | real | fix | Identical 3ms samples make p50/p95/p99 true for any rank; use a strictly increasing series so each percentile is a distinct value. |
| 5 | inline 3792940871 | `cmd/drover-bench/bench_test.go:110` | real | fix | Drain `Work` is the hot path under `--concurrency`; sequential calls miss races on `latN`/`done`. Run concurrent `Work` under `-race` and bound latency from above. |
| 6 | inline 3792942426 | `client.go:452` | real | fix | `notifier` is pgdriver-only and `Client` already imports `pgx`; `NotifyTx` should take `pgx.Tx` instead of `any`. |
| 7 | inline 3792942434 | `client.go:504` | real | fix | Wrapping the item index into the validation error is a one-line `%w` and makes a mid-batch reject diagnosable. |
| 8 | inline 3792942446 | `cmd/drover-bench/bench.go:113` | real | fix | `<-allDone` with no `ctx.Done()` arm ignores cancellation after `Start`; select on both. |
| 9 | inline 3792946037 | `client.go:494` | real | fix | `notifyTx` logs a `pg_notify` error and returns success, but a statement error aborts the caller's Postgres transaction. Insert already succeeded; `Commit` then fails. Isolate notify with a savepoint so a notify failure cannot poison the caller tx. Same defect as #12. |
| 10 | inline 3792946064 | `cmd/drover-bench/bench.go:53` | real | fix | `done` increments on every `Work`, including leftover jobs from a prior enqueue on the same database. Count only IDs recorded in `insertedAt`. |
| 11 | inline 3792946104 | `client_integration_test.go:234` | real | fix | A fixed 300ms sleep can insert before `LISTEN` is registered (NOTIFY is then dropped). Wait until `pg_stat_activity` shows `LISTEN drover`. |
| 12 | inline 3792952113 | `pgdriver.go:120` | real | fix | Duplicate of #9 at the `Exec` site: `SELECT pg_notify` on the caller `pgx.Tx` can abort that transaction. Savepoint around notify; rollback to it on error. |
| — | issue 5309916118 | requirements summary | n/a | won't-fix | Meta summary, not a defect. |
| — | issue 5309961646 | review summary | n/a | won't-fix | Meta summary, not a code defect. CI vulncheck on Go 1.26.5 is a merge blocker and is pinned separately. |

**Counts:** 12 inline findings — 12 real/fix (9 and 12 are the same notify-abort defect). 2 issue comments — meta, no product change. CI: pin the vulncheck toolchain to Go 1.26.6 (stdlib GO-2026-6090 and siblings).
