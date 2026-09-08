#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# scripts/ci-search-test-report.py — fetch a GitHub Actions run of the search
# (or model) integration test workflow, and turn its raw log into a compact,
# structured report: pass/fail counts, per-test failure detail (panic
# message + location, or insta snapshot diff), whole-job infra issues (missing
# toolchain, compile errors, timeouts), and a reconciled outcome that
# distinguishes "tests failed" from "tests passed but the job hung/was
# cancelled afterward" (the two look identical from `gh run view` alone).
#
# Built for LLM/agent consumption: default output is JSON with bounded-size
# fields (diffs and messages are truncated, not dumped wholesale), and the
# process exit code reflects the outcome so a caller can branch without
# re-parsing (0 = success or tests-passed-job-hung, 1 = test failures,
# 2 = infra/tooling failure, 3 = cancelled/timed out before finishing).
#
# Usage:
#   scripts/ci-search-test-report.py                        # latest scheduled run of integration_search.yml on trunk
#   scripts/ci-search-test-report.py --run-id 31135129614   # a specific run
#   scripts/ci-search-test-report.py --branch my-branch --workflow integration_models.yml
#   scripts/ci-search-test-report.py --run-id 123 --format markdown
#   scripts/ci-search-test-report.py --run-id 123 --job-id 456   # skip job auto-selection
#
# Requires: gh (authenticated), jq is NOT required (this parses gh's own JSON).
# Pure stdlib otherwise.

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Any

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")
LOG_PREFIX_RE = re.compile(r"^[^\t]*\t[^\t]*\t(?P<rest>.*)$")
TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z ?(?P<rest>.*)$")

TEST_LINE_RE = re.compile(r"^test (?P<name>\S+) \.\.\. (?P<status>ok|FAILED|ignored)\b")
RESULT_LINE_RE = re.compile(
    r"^test result: (?P<result>ok|FAILED)\. (?P<passed>\d+) passed; "
    r"(?P<failed>\d+) failed; (?P<ignored>\d+) ignored; (?P<measured>\d+) measured; "
    r"(?P<filtered>\d+) filtered out; finished in (?P<duration>[\d.]+)s$"
)
STDOUT_BLOCK_RE = re.compile(r"^---- (?P<name>\S+) stdout ----$")
PANIC_RE = re.compile(r"^thread '(?P<name>[^']+)' \((?P<tid>\d+)\) panicked at (?P<location>[^:]+:\d+:\d+):$")
FAILURES_HEADER_RE = re.compile(r"^failures:$")
SNAPSHOT_SUMMARY_MARK = "Snapshot Summary"

INFRA_PATTERNS = [
    (re.compile(r"cannot execute tool '([^']+)' due to missing ([^\n]+)"), "missing build tool: {0} ({1})"),
    (re.compile(r"error\[E\d{4}\]"), "Rust compile error (error[Exxxx])"),
    (re.compile(r"error: could not compile"), "cargo build failure"),
    (re.compile(r"The job has exceeded the maximum execution time"), "job hit the workflow's max execution time"),
    (re.compile(r"The operation was canceled\."), "job/step was cancelled"),
]


@dataclass
class SnapshotDiff:
    snapshot_name: str | None = None
    snapshot_file: str | None = None
    source: str | None = None
    added_lines: int = 0
    removed_lines: int = 0
    diff: str = ""


@dataclass
class FailureDetail:
    test_name: str
    kind: str = "unknown"
    message: str = ""
    location: str | None = None
    snapshot_diff: SnapshotDiff | None = None


@dataclass
class TestSummary:
    result: str
    passed: int
    failed: int
    ignored: int
    measured: int
    filtered_out: int
    duration_seconds: float


def run_gh(args: list[str]) -> str:
    try:
        proc = subprocess.run(["gh", *args], capture_output=True, text=True, check=False)
    except FileNotFoundError:
        print("error: 'gh' CLI not found in PATH", file=sys.stderr)
        raise SystemExit(2)
    if proc.returncode != 0:
        print(f"error: gh {' '.join(args)} failed:\n{proc.stderr}", file=sys.stderr)
        raise SystemExit(2)
    return proc.stdout


def run_gh_json(args: list[str]) -> Any:
    return json.loads(run_gh(args))


def resolve_repo(explicit: str | None) -> str:
    if explicit:
        return explicit
    data = run_gh_json(["repo", "view", "--json", "nameWithOwner"])
    return data["nameWithOwner"]


def resolve_run_id(repo: str, workflow: str, branch: str) -> int:
    runs = run_gh_json(
        [
            "run",
            "list",
            "--repo",
            repo,
            "--workflow",
            workflow,
            "--branch",
            branch,
            "--limit",
            "1",
            "--json",
            "databaseId",
        ]
    )
    if not runs:
        print(f"error: no runs found for workflow={workflow!r} branch={branch!r} in {repo}", file=sys.stderr)
        raise SystemExit(2)
    return runs[0]["databaseId"]


def get_run_meta(repo: str, run_id: int) -> dict:
    return run_gh_json(
        [
            "run",
            "view",
            str(run_id),
            "--repo",
            repo,
            "--json",
            "databaseId,displayTitle,workflowName,headBranch,event,status,"
            "conclusion,createdAt,updatedAt,url,jobs",
        ]
    )


def select_jobs(jobs: list[dict], job_id: int | None, job_name_pattern: str | None, all_jobs: bool) -> list[dict]:
    if job_id is not None:
        matches = [j for j in jobs if j["databaseId"] == job_id]
        if not matches:
            print(f"error: job id {job_id} not found in this run", file=sys.stderr)
            raise SystemExit(2)
        return matches
    if all_jobs:
        return jobs
    pattern = re.compile(job_name_pattern or r"test|integration", re.IGNORECASE)
    matches = [j for j in jobs if pattern.search(j["name"])]
    if matches:
        return matches
    if len(jobs) == 1:
        return jobs
    names = ", ".join(j["name"] for j in jobs)
    print(
        f"error: no job name matched {pattern.pattern!r}; pass --job-id or --job-name. Jobs in this run: {names}",
        file=sys.stderr,
    )
    raise SystemExit(2)


def fetch_job_log_lines(repo: str, run_id: int, job_id: int) -> list[str]:
    raw = run_gh(["run", "view", str(run_id), "--repo", repo, "--job", str(job_id), "--log"])
    lines = []
    for line in raw.split("\n"):
        m = LOG_PREFIX_RE.match(line)
        rest = m.group("rest") if m else line
        rest = rest.replace("﻿", "")  # GitHub occasionally re-emits a UTF-8 BOM mid-stream
        ts = TIMESTAMP_RE.match(rest)
        rest = ts.group("rest") if ts else rest
        lines.append(ANSI_RE.sub("", rest))
    return lines


def _truncate(text: str, max_lines: int) -> str:
    lines = text.splitlines()
    if len(lines) <= max_lines:
        return text
    head = max_lines // 2
    tail = max_lines - head
    omitted = len(lines) - head - tail
    return "\n".join([*lines[:head], f"... ({omitted} lines omitted) ...", *lines[-tail:]])


def _extract_snapshot_diff(block: list[str], max_diff_lines: int) -> SnapshotDiff | None:
    try:
        start = next(i for i, l in enumerate(block) if SNAPSHOT_SUMMARY_MARK in l)
    except StopIteration:
        return None
    diff = SnapshotDiff()
    for line in block[start + 1 :]:
        if line.startswith("Snapshot file:"):
            diff.snapshot_file = line.split(":", 1)[1].strip()
        elif line.startswith("Snapshot:") and diff.snapshot_name is None:
            diff.snapshot_name = line.split(":", 1)[1].strip()
        elif line.startswith("Source:"):
            diff.source = line.split(":", 1)[1].strip()

    # The diff table's real top border is the separator containing '┬' (the
    # T-junction between the row-number gutter and the content column).
    # Plain '─'-only separators appear earlier too (around the "Expression:"
    # and "-old snapshot"/"+new results" labels) and must not be mistaken for it.
    body_start = None
    for i in range(start + 1, len(block)):
        if "┬" in block[i]:
            body_start = i + 1
            break
    if body_start is None:
        # No table border (e.g. a plain-text, non-tabular diff): fall back to
        # right after the "+new results" label.
        for i in range(start + 1, len(block)):
            if block[i].strip() == "+new results":
                body_start = i + 1
                break
    if body_start is None:
        body_start = start + 1

    end = len(block)
    for i in range(body_start, len(block)):
        if "┴" in block[i]:
            end = i
            break
    diff_lines = block[body_start:end]
    diff_text = "\n".join(diff_lines)
    diff.added_lines = diff_text.count("│+")
    diff.removed_lines = diff_text.count("│-")
    diff.diff = _truncate(diff_text, max_diff_lines)
    return diff


def _classify_panic_message(message: str) -> str:
    if "wait_until_true" in message or "is_ready" in message:
        return "runtime_did_not_become_ready"
    if "error sending request" in message or "Connection refused" in message or "ConnectionRefused" in message:
        return "connection_error"
    return "panic"


def _extract_failure_detail(test_name: str, block: list[str], max_diff_lines: int) -> FailureDetail:
    detail = FailureDetail(test_name=test_name)

    # A stale snapshot table can be present in a test's stdout alongside an
    # unrelated panic (e.g. it crashed mid-comparison); the panic is always
    # the primary failure signal when both are present, so it takes priority
    # for `kind`/`message` — the snapshot diff is still attached as context.
    detail.snapshot_diff = _extract_snapshot_diff(block, max_diff_lines)

    panic_idx = None
    for i, line in enumerate(block):
        m = PANIC_RE.match(line)
        if m:
            panic_idx = i
            detail.location = m.group("location")
            break

    if panic_idx is not None:
        msg_lines = []
        for line in block[panic_idx + 1 :]:
            stripped = line.strip()
            if not stripped or stripped.startswith("note: run with"):
                break
            msg_lines.append(stripped)
        detail.message = " ".join(msg_lines).strip()
        detail.kind = _classify_panic_message(detail.message)
    elif detail.snapshot_diff is not None:
        detail.kind = "snapshot_mismatch"
        detail.message = f"snapshot mismatch: {detail.snapshot_diff.snapshot_name or detail.snapshot_diff.snapshot_file}"
    else:
        # No panic marker and no snapshot table: look for the first "Error:"
        # line as a generic failure message (e.g. a query-planning/schema
        # error surfaced without a Rust panic).
        for line in block:
            stripped = line.strip()
            if stripped.startswith("Error:"):
                detail.kind = "runtime_error"
                detail.message = stripped
                break

    return detail


def parse_job_log(lines: list[str], max_diff_lines: int) -> dict:
    test_status: dict[str, str] = {}
    for line in lines:
        m = TEST_LINE_RE.match(line.strip())
        if m:
            test_status[m.group("name")] = m.group("status")

    result_summaries = []
    for line in lines:
        m = RESULT_LINE_RE.match(line.strip())
        if m:
            result_summaries.append(
                TestSummary(
                    result=m.group("result"),
                    passed=int(m.group("passed")),
                    failed=int(m.group("failed")),
                    ignored=int(m.group("ignored")),
                    measured=int(m.group("measured")),
                    filtered_out=int(m.group("filtered")),
                    duration_seconds=float(m.group("duration")),
                )
            )

    # Canonical failing-test list from the trailing "failures:\n    name\n    name" block
    # (the harness always prints this after all the per-test "---- name stdout ----"
    # sections, so it is the ground truth for which tests actually failed).
    canonical_failures: list[str] = []
    for i, line in enumerate(lines):
        if FAILURES_HEADER_RE.match(line.strip()):
            for follow in lines[i + 1 :]:
                s = follow.strip()
                if not s:
                    if canonical_failures:
                        break
                    continue
                if s.endswith("stdout ----") or s.startswith("test result:"):
                    break
                canonical_failures.append(s)
            if canonical_failures:
                break

    # Slice out each "---- name stdout ----" block for detail extraction.
    blocks: dict[str, list[str]] = {}
    current_name = None
    current: list[str] = []
    for line in lines:
        m = STDOUT_BLOCK_RE.match(line.strip())
        if m:
            if current_name is not None:
                blocks[current_name] = current
            current_name = m.group("name")
            current = []
            continue
        if current_name is not None:
            if FAILURES_HEADER_RE.match(line.strip()):
                blocks[current_name] = current
                current_name = None
                current = []
                continue
            current.append(line)
    if current_name is not None:
        blocks[current_name] = current

    failing_names = canonical_failures or [n for n, s in test_status.items() if s == "FAILED"]
    failures = [
        _extract_failure_detail(name, blocks.get(name, []), max_diff_lines) for name in failing_names
    ]

    full_text = "\n".join(lines)
    infra_issues = []
    for pattern, template in INFRA_PATTERNS:
        m = pattern.search(full_text)
        if m:
            infra_issues.append(template.format(*m.groups()) if m.groups() else template)

    return {
        "test_summary": result_summaries[-1].__dict__ if result_summaries else None,
        "failures": [f.__dict__ for f in failures],
        "infra_issues": infra_issues,
    }


def _failure_dict_clean(f: dict) -> dict:
    out = dict(f)
    if out.get("snapshot_diff") is not None:
        out["snapshot_diff"] = dict(out["snapshot_diff"].__dict__) if hasattr(out["snapshot_diff"], "__dict__") else out["snapshot_diff"]
    return out


def classify_outcome(job_conclusion: str, parsed: dict) -> tuple[str, list[str]]:
    notes: list[str] = []
    summary = parsed["test_summary"]
    failed_count = summary["failed"] if summary else None

    if job_conclusion == "success":
        return "success", notes

    if job_conclusion in ("cancelled", "timed_out"):
        if summary is not None and failed_count == 0:
            notes.append(
                "The test binary finished with 0 failures, but the job was cancelled/timed out "
                "afterward — the hang is in a step AFTER the tests (e.g. pushing snapshots), not "
                "in the tests themselves."
            )
            return "tests_passed_job_hung", notes
        if summary is not None and failed_count and failed_count > 0:
            notes.append("Tests failed AND the job was later cancelled/timed out.")
            return "test_failures_then_hung", notes
        notes.append("Job was cancelled/timed out before the test binary produced a result summary.")
        return "cancelled_before_tests_completed", notes

    # job_conclusion == "failure" (or anything else unexpected)
    if summary is not None and failed_count and failed_count > 0:
        return "test_failures", notes
    if parsed["infra_issues"]:
        return "infra_failure", notes
    notes.append("Job failed but no test result summary or known infra pattern was found in the log.")
    return "unknown_failure", notes


def build_report(repo: str, run_meta: dict, jobs: list[dict], max_diff_lines: int) -> dict:
    job_reports = []
    for job in jobs:
        lines = fetch_job_log_lines(repo, run_meta["databaseId"], job["databaseId"])
        parsed = parse_job_log(lines, max_diff_lines)
        outcome, notes = classify_outcome(job["conclusion"] or job["status"], parsed)
        job_reports.append(
            {
                "job_id": job["databaseId"],
                "job_name": job["name"],
                "job_status": job["status"],
                "job_conclusion": job["conclusion"],
                "outcome": outcome,
                "notes": notes,
                "test_summary": parsed["test_summary"],
                "failures": [_failure_dict_clean(f) for f in parsed["failures"]],
                "infra_issues": parsed["infra_issues"],
            }
        )

    return {
        "meta": {
            "repo": repo,
            "workflow": run_meta["workflowName"],
            "run_id": run_meta["databaseId"],
            "run_url": run_meta["url"],
            "branch": run_meta["headBranch"],
            "event": run_meta["event"],
            "run_status": run_meta["status"],
            "run_conclusion": run_meta["conclusion"],
            "created_at": run_meta["createdAt"],
            "updated_at": run_meta["updatedAt"],
        },
        "outcome": job_reports[0]["outcome"] if len(job_reports) == 1 else None,
        "jobs": job_reports,
    }


def render_markdown(report: dict) -> str:
    meta = report["meta"]
    out = [
        f"# {meta['workflow']} — run [{meta['run_id']}]({meta['run_url']})",
        f"branch `{meta['branch']}` · triggered by `{meta['event']}` · {meta['run_conclusion'] or meta['run_status']}",
        "",
    ]
    for job in report["jobs"]:
        out.append(f"## {job['job_name']} — `{job['outcome']}`")
        ts = job["test_summary"]
        if ts:
            out.append(
                f"- tests: **{ts['passed']} passed, {ts['failed']} failed**, "
                f"{ts['ignored']} ignored, {ts['filtered_out']} filtered out, "
                f"{ts['duration_seconds']}s"
            )
        for note in job["notes"]:
            out.append(f"- note: {note}")
        for issue in job["infra_issues"]:
            out.append(f"- infra issue: {issue}")
        if job["failures"]:
            out.append("")
            out.append("### Failures")
            for f in job["failures"]:
                out.append(f"- **{f['test_name']}** (`{f['kind']}`)")
                if f.get("location"):
                    out.append(f"  - at `{f['location']}`")
                if f.get("message"):
                    out.append(f"  - {f['message']}")
                sd = f.get("snapshot_diff")
                if sd:
                    out.append(f"  - snapshot `{sd.get('snapshot_name')}` (+{sd.get('added_lines')}/-{sd.get('removed_lines')})")
                    out.append("    ```diff")
                    for dl in sd.get("diff", "").splitlines():
                        out.append(f"    {dl}")
                    out.append("    ```")
        out.append("")
    return "\n".join(out)


EXIT_CODES = {
    "success": 0,
    "tests_passed_job_hung": 0,
    "test_failures": 1,
    "test_failures_then_hung": 1,
    "infra_failure": 2,
    "unknown_failure": 2,
    "cancelled_before_tests_completed": 3,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", help="OWNER/REPO (default: repo of the current directory)")
    parser.add_argument("--workflow", default="integration_search.yml", help="workflow file name")
    parser.add_argument("--branch", default="trunk", help="branch to find the latest run on (ignored if --run-id given)")
    parser.add_argument("--run-id", type=int, help="specific run ID instead of the latest on --branch")
    parser.add_argument("--job-id", type=int, help="specific job ID within the run")
    parser.add_argument("--job-name", help="regex to select job(s) by name (default: 'test|integration')")
    parser.add_argument("--all-jobs", action="store_true", help="process every job in the run")
    parser.add_argument("--format", choices=["json", "markdown"], default="json")
    parser.add_argument("--max-diff-lines", type=int, default=60, help="cap on snapshot diff lines kept per failure")
    parser.add_argument("--pretty", action="store_true", default=True)
    args = parser.parse_args()

    repo = resolve_repo(args.repo)
    run_id = args.run_id or resolve_run_id(repo, args.workflow, args.branch)
    run_meta = get_run_meta(repo, run_id)
    jobs = select_jobs(run_meta["jobs"], args.job_id, args.job_name, args.all_jobs)

    report = build_report(repo, run_meta, jobs, args.max_diff_lines)

    if args.format == "markdown":
        print(render_markdown(report))
    else:
        print(json.dumps(report, indent=2))

    outcomes = [j["outcome"] for j in report["jobs"]]
    worst = max((EXIT_CODES.get(o, 2) for o in outcomes), default=0)
    raise SystemExit(worst)


if __name__ == "__main__":
    main()
