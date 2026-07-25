# Skills Catalog

Workflow shape:

```
RESEARCH (dated fleet → synthesis) → RFC/ADR → tlc-spec-driven cycle → IMPLEMENT → PR
```

Project-local skills in `.claude/skills/`:

| Skill | Use for |
|---|---|
| `create-adr` | Recording an architectural decision after it's made |
| `create-rfc` | Proposing roadmaps or significant changes needing alignment |
| `create-technical-design-doc` | Design docs for large multi-cycle efforts |
| `tlc-spec-driven` | Planning + implementing one roadmap cycle (Specify → Design → Tasks → Execute), owns `.specs/` |
| `drover-finalize` | Branch names, Conventional Commits, and self-contained PR descriptions (validator + body renderer); load before any commit or PR |
| `pr-review` | Multi-agent PR review: 6 parallel lanes (concurrency, requirements, tests, idioms, regression, SQL/performance) with removable comments; invoke only when explicitly asked to review a PR |
| `drover-ship-cycle` | End-to-end orchestrator for one roadmap PR: pick cycle → tlc build → publish → fresh-context review → triage → fix → clean comments → user-gated merge; resumable via `.specs/.ship-status` |
| `skill-architect` | Building new project skills |
| `grilling` | Stress-testing a plan or design before committing to it |

Planned (create when first needed, via `skill-architect`):

- A Go-conventions skill distilled from `docs/research/2026-07-22/rq04-go-conventions.md` once patterns recur in implementation
