#!/usr/bin/env python3
"""Validate branch names, commit messages, and PR titles.

Two checks:
1. Conventional Commit format for the branch, commit subject, and PR title.
2. No internal references (task/phase IDs, decision/requirement IDs, planning
   labels, or internal spec paths) in the commit subject/body or PR title, so the
   permanent git history and the PR stay self-contained. See the
   "Self-Contained History" section of SKILL.md.
"""

from __future__ import annotations

import argparse
import re
import sys

TYPES = (
    "feat",
    "fix",
    "docs",
    "refactor",
    "test",
    "chore",
    "build",
    "ci",
    "perf",
    "style",
    "revert",
)
TYPE_PATTERN = "|".join(TYPES)
BRANCH_PATTERN = re.compile(
    rf"^(?:{TYPE_PATTERN})/(?:[1-9][0-9]*-)?[a-z0-9]+(?:-[a-z0-9]+)*$"
)
CONVENTIONAL_PATTERN = re.compile(
    rf"^(?:{TYPE_PATTERN})(?:\([a-z0-9]+(?:-[a-z0-9]+)*\))?!?: [a-z0-9][^\n]*$"
)

# Internal references that must never reach the git history or the PR. Each entry
# is (human label, compiled pattern). Kept deliberately tight to avoid false
# positives; rephrase in plain terms rather than working around a genuine hit.
INTERNAL_REF_PATTERNS = (
    ("task id", re.compile(r"\bT[0-9]{1,2}\b")),
    ("decision/requirement id", re.compile(r"\b(?:ADR|AD|RFC|TDD|FR|NFR|AC|Gap|CORE|G)-[A-Za-z0-9]")),
    ("cycle label", re.compile(r"\bcycle\s+[a-z0-9]{1,2}\b", re.IGNORECASE)),
    ("phase label", re.compile(r"\bphase\s+\d+", re.IGNORECASE)),
    ("gate label", re.compile(r"\bGate:")),
    ("spec-deviation label", re.compile(r"SPEC_DEVIATION")),
    ("design-section ref", re.compile(r"design\s+§")),
    ("internal spec path", re.compile(r"\.specs/")),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--branch", help="Branch name to validate.")
    parser.add_argument("--commit", help="Commit subject to validate.")
    parser.add_argument("--commit-body", help="Commit body to scan for internal references.")
    parser.add_argument("--pr-title", help="Pull request title to validate.")
    args = parser.parse_args()

    if not any((args.branch, args.commit, args.commit_body, args.pr_title)):
        parser.error(
            "provide at least one of --branch, --commit, --commit-body, or --pr-title"
        )

    return args


def validate(label: str, value: str | None, pattern: re.Pattern[str], example: str) -> bool:
    if value is None:
        return True

    if pattern.fullmatch(value):
        print(f"PASS {label}: {value}")
        return True

    print(f"FAIL {label}: {value}", file=sys.stderr)
    print(f"  Expected format, for example: {example}", file=sys.stderr)
    return False


def check_internal_refs(label: str, value: str | None) -> bool:
    if value is None:
        return True

    hits = [
        (name, match.group(0))
        for name, pattern in INTERNAL_REF_PATTERNS
        for match in pattern.finditer(value)
    ]
    if not hits:
        return True

    print(f"FAIL {label}: contains internal references (must be self-contained)", file=sys.stderr)
    for name, token in hits:
        print(f"  - {name}: {token!r}", file=sys.stderr)
    print(
        "  Rephrase in plain terms; keep traceability in .specs/, not in git history.",
        file=sys.stderr,
    )
    return False


def main() -> int:
    args = parse_args()
    valid = True

    valid &= validate("branch", args.branch, BRANCH_PATTERN, "chore/workflow-conventions")
    valid &= validate("commit", args.commit, CONVENTIONAL_PATTERN, "chore(workflow): document project conventions")
    valid &= validate("PR title", args.pr_title, CONVENTIONAL_PATTERN, "chore(workflow): document project conventions")

    valid &= check_internal_refs("commit subject", args.commit)
    valid &= check_internal_refs("commit body", args.commit_body)
    valid &= check_internal_refs("PR title", args.pr_title)

    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
