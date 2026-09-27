#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
"""CodeQL runs on the merge queue and trunk, and analyzes a trusted suite."""

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

    def test_produce_sarif_uses_the_cli_code_scanning_suite(self):
        import yaml

        doc = yaml.safe_load(WORKFLOW.read_text())
        steps = doc["jobs"]["sarif"]["steps"]
        script = next(step["run"] for step in steps if step["name"] == "Analyze imported database")
        self.assertIn(
            'suite="codeql/${lang}-queries:codeql-suites/${lang}-code-scanning.qls"',
            script,
        )
        self.assertNotIn("config-queries.qls", script)
        self.assertNotIn("relocate_database_tree", script)


if __name__ == "__main__":
    unittest.main()
