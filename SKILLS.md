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
| `skill-architect` | Building new project skills |
| `grilling` | Stress-testing a plan or design before committing to it |

Planned (create when first needed, via `skill-architect`):

- `drover-finalize` — branch/commit/PR conventions (adapt from learny-finalize)
- `pr-review` — multi-agent PR review adapted to Go (golangci-lint, `-race`, coverage)
- A Go-conventions skill distilled from `docs/research/2026-07-22/rq04-go-conventions.md` once patterns recur in implementation
