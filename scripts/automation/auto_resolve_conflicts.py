#!/usr/bin/env python3
"""Attempt to resolve merge conflicts using an LLM and emit a report."""
from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from openai import OpenAI


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


def llm_client() -> Optional[OpenAI]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return None
    return OpenAI(api_key=api_key)


def resolve_file(client: OpenAI, model: str, path: Path) -> ResolutionResult:
    original = path.read_text(encoding="utf-8")
    system_prompt = (
        "You are a code merge assistant. Resolve merge conflicts in source files. "
        "Only produce the final merged file content without conflict markers."
    )
    user_prompt = (
        "Resolve the merge conflicts in the following file. "
        "Return ONLY the full file contents that should replace the file, with no explanations.\n\n"
        f"File path: {path}\n\n"
        "File content (including conflict markers):\n\n"
        f"{original}"
    )
    try:
        response = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
    except Exception as exc:  # pragma: no cover - runtime path
        return ResolutionResult(path=path, status="failed", comment=f"LLM request failed: {exc}")

    resolved_content = getattr(response, "output_text", "").strip()
    if not resolved_content:
        return ResolutionResult(
            path=path,
            status="failed",
            comment="LLM response did not contain replacement content.",
        )

    path.write_text(resolved_content + ("\n" if not resolved_content.endswith("\n") else ""), encoding="utf-8")
    add_result = subprocess.run(["git", "add", str(path)], capture_output=True, text=True)
    if add_result.returncode != 0:
        return ResolutionResult(
            path=path,
            status="failed",
            comment=f"Failed to stage resolved file: {add_result.stderr.strip()}",
        )
    return ResolutionResult(path=path, status="resolved", comment=resolved_content[-500:].strip())


def main() -> int:
    report_path = Path(os.environ.get("LLM_REPORT_PATH", "merge_conflict_report.md"))
    model = os.environ.get("LLM_MODEL", "gpt-5.1-codex-mini")

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

    client = llm_client()
    if client is None:
        notes = "OPENAI_API_KEY is not set; skipping automatic resolution."
        results = [ResolutionResult(path=file, status="skipped", comment="") for file in files]
        write_report(report_path, "Skipped due to missing API key", results, model, notes=notes)
        append_output(status="skipped", report_file=str(report_path), reason="missing_api_key")
        return 0

    results: List[ResolutionResult] = []
    for file in files:
        result = resolve_file(client, model, file)
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
