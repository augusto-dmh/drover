#!/usr/bin/env python3
"""Validate branch names, commit messages, and PR titles.

Checks:
1. Conventional Commit format with a **required** scope for commit subjects and
   PR titles (drover house style: `feat(driver): …`, never bare `feat: …`).
2. No internal references (task/phase IDs, decision/requirement IDs, planning
   labels, or internal spec paths) in the commit subject/body or PR title.
3. No AI/tooling attribution trailers or lines (`Co-authored-by`, `Generated with`,
   known agent emails) in commit subject/body or PR title.
4. Optional `--range REV` validates every commit in a git revision range
   (subject + body), so a clean proposed PR title cannot hide a dirty history.

See the "Self-Contained History" and "Commit And PR Hygiene" sections of SKILL.md.
"""

from __future__ import annotations

import argparse
import re
import subprocess
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
# Scope is mandatory for commit subjects and PR titles.
CONVENTIONAL_PATTERN = re.compile(
    rf"^(?:{TYPE_PATTERN})\([a-z0-9]+(?:-[a-z0-9]+)*\)!?: [a-z0-9][^\n]*$"
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

# Attribution that agent tooling often appends after the authored message.
# Match trailer lines and known agent emails — not the bare word "cursor".
ATTRIBUTION_PATTERNS = (
    ("Co-authored-by trailer", re.compile(r"(?im)^co-authored-by:\s*.+$")),
    ("Generated-with line", re.compile(r"(?im)^generated\s+with\b.*$")),
    ("Cursor agent email", re.compile(r"(?i)cursoragent@cursor\.com")),
    ("Signed-off-by Cursor", re.compile(r"(?im)^signed-off-by:\s*cursor\b.*$")),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--branch", help="Branch name to validate.")
    parser.add_argument("--commit", help="Commit subject to validate.")
    parser.add_argument("--commit-body", help="Commit body to scan for forbidden content.")
    parser.add_argument("--pr-title", help="Pull request title to validate.")
    parser.add_argument(
        "--range",
        dest="rev_range",
        metavar="REV",
        help="Git revision range to validate (e.g. main..HEAD). Checks every commit's subject and body.",
    )
    args = parser.parse_args()

    if not any(
        (args.branch, args.commit, args.commit_body, args.pr_title, args.rev_range)
    ):
        parser.error(
            "provide at least one of --branch, --commit, --commit-body, --pr-title, or --range"
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


def check_patterns(
    label: str,
    value: str | None,
    patterns: tuple[tuple[str, re.Pattern[str]], ...],
    heading: str,
    hint: str,
) -> bool:
    if value is None:
        return True

    hits = [
        (name, match.group(0).strip())
        for name, pattern in patterns
        for match in pattern.finditer(value)
    ]
    if not hits:
        return True

    print(f"FAIL {label}: {heading}", file=sys.stderr)
    for name, token in hits:
        print(f"  - {name}: {token!r}", file=sys.stderr)
    print(f"  {hint}", file=sys.stderr)
    return False


def check_internal_refs(label: str, value: str | None) -> bool:
    return check_patterns(
        label,
        value,
        INTERNAL_REF_PATTERNS,
        "contains internal references (must be self-contained)",
        "Rephrase in plain terms; keep traceability in .specs/, not in git history.",
    )


def check_attribution(label: str, value: str | None) -> bool:
    return check_patterns(
        label,
        value,
        ATTRIBUTION_PATTERNS,
        "contains AI/tooling attribution (forbidden in permanent history)",
        "Strip the trailer/line and recommit via history rewrite if already published; "
        "do not rely on the HEREDOC alone — agent git wrappers may append trailers after commit.",
    )


def validate_commit_fields(subject: str, body: str, *, label_prefix: str) -> bool:
    valid = True
    subj_label = f"{label_prefix} subject"
    body_label = f"{label_prefix} body"
    valid &= validate(
        subj_label,
        subject,
        CONVENTIONAL_PATTERN,
        "feat(driver): add operator list and redrive methods",
    )
    valid &= check_internal_refs(subj_label, subject)
    valid &= check_attribution(subj_label, subject)
    if body.strip():
        valid &= check_internal_refs(body_label, body)
        valid &= check_attribution(body_label, body)
    else:
        # Empty body still gets an attribution pass printed only when scanning ranges.
        pass
    return valid


def validate_range(rev_range: str) -> bool:
    try:
        raw = subprocess.check_output(
            ["git", "log", "--format=%H%x1f%s%x1f%b%x1e", rev_range],
            text=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as exc:
        print(f"FAIL range: git log {rev_range!r} failed", file=sys.stderr)
        err = (exc.stderr or "").strip()
        if err:
            print(f"  {err}", file=sys.stderr)
        return False

    records = [r for r in raw.split("\x1e") if r.strip()]
    if not records:
        print(f"FAIL range: no commits in {rev_range!r}", file=sys.stderr)
        return False

    valid = True
    for record in records:
        parts = record.lstrip("\n").split("\x1f", 2)
        if len(parts) != 3:
            print(f"FAIL range: malformed git log record in {rev_range!r}", file=sys.stderr)
            valid = False
            continue
        sha, subject, body = parts
        short = sha[:12]
        print(f"CHECK {short}: {subject}")
        if not validate_commit_fields(subject, body, label_prefix=short):
            valid = False

    if valid:
        print(f"PASS range: {len(records)} commit(s) in {rev_range}")
    return valid


def main() -> int:
    args = parse_args()
    valid = True

    valid &= validate("branch", args.branch, BRANCH_PATTERN, "chore/workflow-conventions")
    valid &= validate(
        "commit",
        args.commit,
        CONVENTIONAL_PATTERN,
        "feat(driver): add operator list and redrive methods",
    )
    valid &= validate(
        "PR title",
        args.pr_title,
        CONVENTIONAL_PATTERN,
        "feat(cli): add Inspector API and drover CLI",
    )

    valid &= check_internal_refs("commit subject", args.commit)
    valid &= check_attribution("commit subject", args.commit)
    valid &= check_internal_refs("commit body", args.commit_body)
    valid &= check_attribution("commit body", args.commit_body)
    valid &= check_internal_refs("PR title", args.pr_title)
    valid &= check_attribution("PR title", args.pr_title)

    if args.rev_range is not None:
        valid &= validate_range(args.rev_range)

    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
