# PR #18 review triage

Triage of review comments on `feat/status-page` against the code as of `52ada666`. Comments will be deleted in Stage 6; this file is the surviving record.

| # | Source | File:line | Verdict | Action | Rationale |
| --- | --- | --- | --- | --- | --- |
| 1 | inline 3937988637 `<!-- drover-review:tests -->` | `cmd/drover/web_test.go:56` | real | fix | `Contains("3")` also matches `Attempt: 3` (`3/25`). Spec requires the depth count to be evidenced; a distinctive `Count` (e.g. 42) discriminates. |
| 2 | inline 3937989382 | `cmd/drover/web_test.go:62` | real | fix | Spec requires POST forms. `action=` alone passes if the template omits `method="post"` (HTML default GET → 405). Assert `method="post"`. Template already has it; tests do not. |
| 3 | inline 3937989453 | `cmd/drover/web_test.go:197` | real | fix | P1 AC5 requires rendered rows to be the jobs `ListJobs` returned. The table spies opts only with an empty slice. Seed distinct jobs and assert those ids appear. |
| 4 | inline 3937989509 | `cmd/drover/web.go:244` | real | fix | P2 AC3: 400 HTML must name the invalid state. Status-only assertion would accept a generic body. Assert `nope` in the body. |
| 5 | inline 3937989562 | `cmd/drover/web.go:298` | real | fix | `refreshURL` omits default `state`/`limit`. A regression that always drops non-default `state`/`limit` is invisible. Assert `state=running` and `limit=10` in the meta URL. |
| 6 | inline 3937989696 | `cmd/drover/web_test.go:297` | real | fix | Unknown flash must omit the banner, not merely escape the token. Assert no `role="status"` / banner for an unknown code with a valid id. |
| 7 | inline 3937989805 | `cmd/drover/web_test.go:351` | real | fix | Production wraps sentinels (`errors.Is`). Bare-sentinel tests would hide an `err == sentinel` branch and never prove Inspector was called. Wrap and assert recorded ids. |
| 8 | inline 3937989909 | `cmd/drover/web_test.go:401` | real | fix | Spec: hidden `queue`/`state`/`limit`. Test omits `state`, so dropping that input still passes. Assert `name="state" value="dead"`. |
| 9 | inline 3937989975 | `cmd/drover/web.go:166` | real | fix | Spec edge: non-same-host Referer with no Origin → 403, Inspector unused. Matching-Referer exists; mismatch does not. Add the subtest. |
| 10 | inline 3937990053 | `cmd/drover/web.go:237` | real | fix | Bounds are 1..1000; 400 covers outside, happy path uses 10. `limit=1` and `limit=1000` are the documented edges and cheap to add. |
| 11 | issue 5546615986 requirements | PR-level | real | won't-fix | Requirements review: no missing WEB-* at ≥80% confidence. The scheduled-column note is design prose, not an AC. No product change. |
| 12 | issue 5546681231 summary | PR-level | real | won't-fix | Roll-up of #1–#10; no additional finding. |

**Counts:** 12 findings (10 inline + 2 issue). Real: 12. False: 0. Fix: 10. Won't-fix: 2.

No finding disputes an ADR or recorded AD-077–AD-083. All accepted fixes are test assertions; handler/template already match the spec.
