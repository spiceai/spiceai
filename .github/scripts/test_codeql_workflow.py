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


if __name__ == "__main__":
    unittest.main()
