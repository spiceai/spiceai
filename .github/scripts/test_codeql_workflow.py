#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
"""CodeQL runs on the merge queue and trunk, and publishes SARIF for the uploader."""

from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "codeql-analysis.yml"
UPLOAD = ROOT / ".github" / "workflows" / "codeql-upload.yml"


def _on(doc):
    # PyYAML 1.1 parses an unquoted `on` key as boolean true.
    return doc[True] if True in doc else doc["on"]


class CodeQlWorkflowTest(unittest.TestCase):
    def test_codeql_does_not_scan_pull_requests(self):
        import yaml

        triggers = _on(yaml.safe_load(WORKFLOW.read_text()))
        self.assertEqual(set(triggers), {"merge_group", "push"})
        self.assertEqual(
            triggers["merge_group"]["branches"],
            ["trunk", "release-*", "release/*"],
        )
        self.assertEqual(
            triggers["push"]["branches"],
            ["trunk", "release-*", "release/*"],
        )

        upload = yaml.safe_load(UPLOAD.read_text())
        script = upload["jobs"]["upload"]["steps"][0]["run"]
        self.assertIn("pull_request|pull_request_target)", script)
        self.assertNotIn("refs/pull/", script)

    def test_analyze_runs_on_spiceai_macos(self):
        import yaml

        doc = yaml.safe_load(WORKFLOW.read_text())
        # Extraction and query evaluation share one job. The spiceai-macos
        # runners are persistent and all run merge-queue extractions, so a
        # second job on that pool would add no isolation.
        self.assertEqual(set(doc["jobs"]), {"analyze"})
        analyze = doc["jobs"]["analyze"]
        self.assertEqual(analyze["runs-on"], "spiceai-macos")
        self.assertEqual(
            analyze["env"]["HAS_SPICEIO_SECRET"],
            "${{ secrets.UNAS_SMB_PASS != '' }}",
        )
        spiceio = next(step for step in analyze["steps"] if step["name"] == "Set up spiceio")
        self.assertIn("runner.os == 'macOS'", spiceio["if"])
        self.assertIn("HAS_SPICEIO_SECRET", spiceio["if"])
        self.assertIn("spiceio/.github/actions/setup@", spiceio["uses"])
        self.assertEqual(spiceio["with"]["bucket"], "sccache")
        self.assertEqual(spiceio["with"]["immutable-objects"], "true")
        sccache = next(step for step in analyze["steps"] if step["name"] == "Set up sccache")
        self.assertEqual(
            sccache["with"]["spiceio_endpoint"],
            "${{ steps.setup-spiceio.outputs.endpoint }}",
        )
        self.assertIn("HAS_TEST_MINIO_SECRET", sccache["with"]["minio_endpoint"])
        kill = next(step for step in analyze["steps"] if step["name"] == "Kill spiceio")
        self.assertIn("always()", kill["if"])

    def test_analyze_publishes_sarif_for_the_uploader(self):
        import yaml

        doc = yaml.safe_load(WORKFLOW.read_text())
        analyze = doc["jobs"]["analyze"]
        steps = analyze["steps"]

        # No query override, so init resolves the default code-scanning suite.
        init = next(
            step for step in steps if step.get("uses", "").startswith("github/codeql-action/init@")
        )
        self.assertFalse({"queries", "packs", "config", "config-file"} & set(init["with"]))

        run = next(
            step
            for step in steps
            if step.get("uses", "").startswith("github/codeql-action/analyze@")
        )
        self.assertNotIn("skip-queries", run["with"])
        self.assertEqual(run["with"]["upload"], "never")
        self.assertIs(run["with"]["upload-database"], False)
        # The action passes `--threads` and `--ram` from init's CODEQL_THREADS
        # and CODEQL_RAM. A bare `codeql database analyze` defaults to one
        # thread and a heap this database runs out of.
        self.assertFalse(any("database analyze" in step.get("run", "") for step in steps))

        sarif = next(step for step in steps if step["name"] == "Upload SARIF artifact")
        self.assertEqual(sarif["with"]["path"], run["with"]["output"])
        self.assertEqual(sarif["with"]["name"], "codeql-sarif-${{ matrix.language }}")
        self.assertEqual(sarif["with"]["if-no-files-found"], "error")

        upload = yaml.safe_load(UPLOAD.read_text())
        download = next(
            step
            for step in upload["jobs"]["upload"]["steps"]
            if step["name"] == "Download SARIF artifact"
        )
        self.assertEqual(download["with"]["pattern"], "codeql-sarif-*")

        # The token stays read-only. codeql-upload.yml is what publishes.
        self.assertNotIn("security-events", doc["permissions"])
        self.assertNotIn("security-events", analyze["permissions"])

    def test_upload_excuses_only_a_deleted_merge_queue_ref(self):
        import yaml

        steps = yaml.safe_load(UPLOAD.read_text())["jobs"]["upload"]["steps"]
        upload = next(step for step in steps if step["name"] == "Upload SARIF to code scanning")
        self.assertIs(upload["continue-on-error"], True)
        guard = next(
            step for step in steps if step["name"] == "Fail unless the merge-queue ref is gone"
        )
        self.assertEqual(guard["if"], f"steps.{upload['id']}.outcome == 'failure'")

        queue_ref = "refs/heads/gh-readonly-queue/trunk/pr-1-" + "a" * 40
        cases = [
            # (upload ref, stub `gh api` exit code, stub stderr, expected exit)
            (queue_ref, 1, "gh: Not Found (HTTP 404)", 0),
            (queue_ref, 0, "", 1),
            (queue_ref, 1, "gh: Server Error (HTTP 502)", 1),
            (queue_ref, 1, "error connecting to api.github.com", 1),
            ("refs/heads/trunk", 1, "gh: Not Found (HTTP 404)", 1),
            ("refs/heads/release/2.3", 1, "gh: Not Found (HTTP 404)", 1),
        ]
        for ref, gh_exit, gh_stderr, expected in cases:
            with self.subTest(ref=ref, gh_stderr=gh_stderr):
                self.assertEqual(_run_guard(guard["run"], ref, gh_exit, gh_stderr), expected)


def _run_guard(script, upload_ref, gh_exit, gh_stderr):
    import os
    import subprocess
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        stub = Path(tmp) / "gh"
        stub.write_text(
            f"#!/usr/bin/env bash\nprintf '%s\\n' {gh_stderr!r} >&2\nexit {gh_exit}\n",
            newline="\n",
        )
        stub.chmod(0o755)
        env = dict(
            os.environ,
            PATH=f"{tmp}{os.pathsep}{os.environ['PATH']}",
            GITHUB_REPOSITORY="spiceai/spiceai",
            UPLOAD_REF=upload_ref,
        )
        return subprocess.run(
            ["bash", "-c", script], env=env, capture_output=True, check=False
        ).returncode


if __name__ == "__main__":
    unittest.main()
