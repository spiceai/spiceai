#!/usr/bin/env python3
"""Attempt to resolve merge conflicts using OpenAI Codex CLI and emit a report."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class ResolutionResult:
    path: Path
    status: str  # "resolved", "failed", "skipped"
    comment: str


def git_conflict_files() -> List[Path]:
    result = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=U"],
        check=True,
        capture_output=True,
        text=True,
    )
    files = [Path(line) for line in result.stdout.strip().splitlines() if line.strip()]
    return files


def append_output(**items: str) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    with open(output_path, "a", encoding="utf-8") as handle:
        for key, value in items.items():
            handle.write(f"{key}={value}\n")


def write_report(report_path: Path, header: str, results: List[ResolutionResult], model: str, notes: Optional[str] = None) -> None:
    lines: List[str] = ["# Automated conflict resolution report", ""]
    lines.append(f"- Model: {model if model else 'n/a'}")
    lines.append(f"- Outcome: {header}")
    if notes:
        lines.extend(["", notes])
    if results:
        lines.extend(["", "## File details", ""])
        for result in results:
            status_symbol = {
                "resolved": "✅ Resolved",
                "failed": "❌ Manual follow-up required",
                "skipped": "⚪ Skipped",
            }.get(result.status, result.status)
            lines.append(f"### `{result.path}`")
            lines.append("")
            lines.append(f"- Status: {status_symbol}")
            if result.comment.strip():
                lines.extend(["", "```", result.comment.strip(), "```"])
            lines.append("")
    report_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def codex_available() -> bool:
    """Check if the codex CLI is available."""
    return shutil.which("codex") is not None


def has_api_key() -> bool:
    """Check if OPENAI_API_KEY is set."""
    return bool(os.environ.get("OPENAI_API_KEY"))


def resolve_file(model: str, path: Path) -> ResolutionResult:
    original = path.read_text(encoding="utf-8")
    prompt = (
        f"Resolve the merge conflicts in file '{path}'. "
        "The file contains git conflict markers (<<<<<<< HEAD, =======, >>>>>>>). "
        "Analyze both versions and produce a clean merged result that preserves the intent of both changes. "
        "Write the resolved content directly to the file, removing all conflict markers."
    )

    try:
        result = subprocess.run(
            [
                "codex",
                "--model", model,
                "--approval-mode", "full-auto",
                "--full-auto-error-mode", "ignore-and-continue",
                "--quiet",
                prompt,
            ],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout per file
        )
    except subprocess.TimeoutExpired:
        return ResolutionResult(
            path=path,
            status="failed",
            comment="Codex CLI timed out after 5 minutes.",
        )
    except Exception as exc:  # pragma: no cover - runtime path
        return ResolutionResult(path=path, status="failed", comment=f"Codex CLI failed: {exc}")

    if result.returncode != 0:
        error_msg = result.stderr.strip() or result.stdout.strip() or "Unknown error"
        return ResolutionResult(
            path=path,
            status="failed",
            comment=f"Codex CLI returned non-zero exit code: {error_msg}",
        )

    # Read the potentially modified file
    try:
        new_content = path.read_text(encoding="utf-8")
    except Exception as exc:
        return ResolutionResult(
            path=path,
            status="failed",
            comment=f"Failed to read file after resolution: {exc}",
        )

    # Check if conflict markers are still present
    if "<<<<<<< HEAD" in new_content or "=======" in new_content or ">>>>>>>" in new_content:
        # Restore original and report failure
        path.write_text(original, encoding="utf-8")
        return ResolutionResult(
            path=path,
            status="failed",
            comment="Conflict markers still present after resolution attempt.",
        )

    # Stage the resolved file
    add_result = subprocess.run(["git", "add", str(path)], capture_output=True, text=True)
    if add_result.returncode != 0:
        return ResolutionResult(
            path=path,
            status="failed",
            comment=f"Failed to stage resolved file: {add_result.stderr.strip()}",
        )

    return ResolutionResult(path=path, status="resolved", comment=new_content[-500:].strip())


def main() -> int:
    report_path = Path(os.environ.get("LLM_REPORT_PATH", "merge_conflict_report.md"))
    model = os.environ.get("LLM_MODEL", "o4-mini")

    try:
        files = git_conflict_files()
    except subprocess.CalledProcessError as exc:
        print(f"Failed to inspect conflicts: {exc}", file=sys.stderr)
        append_output(status="error", report_file=str(report_path))
        return 0

    if not files:
        write_report(report_path, "No conflicts detected", [], model)
        append_output(status="no_conflicts", report_file=str(report_path))
        return 0

    if not codex_available():
        notes = "Codex CLI is not installed or not in PATH; skipping automatic resolution."
        results = [ResolutionResult(path=file, status="skipped", comment="") for file in files]
        write_report(report_path, "Skipped due to missing Codex CLI", results, model, notes=notes)
        append_output(status="skipped", report_file=str(report_path), reason="missing_codex_cli")
        return 0

    if not has_api_key():
        notes = "OPENAI_API_KEY is not set; skipping automatic resolution."
        results = [ResolutionResult(path=file, status="skipped", comment="") for file in files]
        write_report(report_path, "Skipped due to missing API key", results, model, notes=notes)
        append_output(status="skipped", report_file=str(report_path), reason="missing_api_key")
        return 0

    results: List[ResolutionResult] = []
    for file in files:
        result = resolve_file(model, file)
        results.append(result)

    unresolved = [r for r in results if r.status != "resolved"]
    if unresolved:
        header = "Conflicts remain after attempted resolution"
        status = "partial"
    else:
        header = "All conflicts resolved"
        status = "resolved"

    write_report(report_path, header, results, model)
    append_output(status=status, report_file=str(report_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
