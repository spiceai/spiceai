#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
"""Produce SARIF relocates query paths recorded on the Analyze runner.

Run 35748658952 failed because `temp/config-queries.qls` named
`/home/runner/_work/_tool/CodeQL/.../DisabledCertificateCheck.ql`, the
spiceai-dev-runners toolcache. Produce SARIF runs on a hosted VM whose
CLI lives under `/opt/hostedtoolcache/CodeQL`. These tests execute the
script embedded in `.github/workflows/codeql-analysis.yml`.
"""

from __future__ import annotations

import io
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "codeql-analysis.yml"

# The path from the failed Produce SARIF log, job 106819039780.
DEV_QUERY = (
    "/home/runner/_work/_tool/CodeQL/2.27.0/x64/codeql/qlpacks/"
    "codeql/rust-queries/0.1.42/queries/security/CWE-295/DisabledCertificateCheck.ql"
)
DEV_QUERY_2 = (
    "/home/runner/_work/_tool/CodeQL/2.27.0/x64/codeql/qlpacks/"
    "codeql/rust-queries/0.1.42/queries/security/CWE-020/RegexInjection.ql"
)

SUITE = f"""---
 -
  query: {DEV_QUERY}
 -
  query: {DEV_QUERY_2}
 -
  exclude:
    tags:
     - exclude-from-incremental
"""


def load_relocator():
    text = WORKFLOW.read_text()
    start_marker = "# relocate-codeql-query-paths"
    end_marker = "# end-relocate-codeql-query-paths"
    start = text.rfind("\n", 0, text.index(start_marker)) + 1
    end_at = text.index(end_marker, start)
    end = text.find("\n", end_at)
    if end == -1:
        end = len(text)
    else:
        end += 1
    namespace = {"__name__": "relocate_codeql_query_paths"}
    exec(compile(textwrap.dedent(text[start:end]), str(WORKFLOW), "exec"), namespace)
    return namespace


def write_pack_file(local_packs: Path, relative: str) -> None:
    path = local_packs / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("// query\n")


class RelocateCodeQlQueryPathsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_relocator()

    def test_dev_runner_prefix_moves_onto_the_local_cli(self):
        local = Path("/opt/hostedtoolcache/CodeQL/2.27.0/x64/codeql/qlpacks")
        updated = self.mod["relocate_suite_text"](SUITE, local)
        self.assertNotIn("/home/runner/_work/_tool/CodeQL", updated)
        self.assertIn(
            "/opt/hostedtoolcache/CodeQL/2.27.0/x64/codeql/qlpacks/"
            "codeql/rust-queries/0.1.42/queries/security/CWE-295/DisabledCertificateCheck.ql",
            updated,
        )
        self.assertEqual(
            self.mod["relocate_suite_text"](updated, local),
            updated,
        )

    def test_imported_database_suite_is_rewritten_when_the_pack_files_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "imported"
            db = root / "rust"
            (db / "temp").mkdir(parents=True)
            suite = db / "temp" / "config-queries.qls"
            suite.write_text(SUITE)
            (db / "codeql-database.yml").write_text(
                "sourceLocationPrefix: /home/runner/_work/spiceai/spiceai\n"
            )
            local = Path(tmp) / "qlpacks"
            for query in (DEV_QUERY, DEV_QUERY_2):
                relative = query.split("/codeql/qlpacks/", 1)[1]
                write_pack_file(local, relative)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                self.mod["relocate_database_tree"](root, local)

            rewritten = suite.read_text()
            self.assertNotIn("/home/runner/_work/_tool", rewritten)
            self.assertTrue(
                (local / DEV_QUERY.split("/codeql/qlpacks/", 1)[1]).is_file()
            )
            self.assertIn(str(local), rewritten)
            self.assertIn("Relocated 2 CodeQL query paths", stdout.getvalue())
            self.assertIn(
                "sourceLocationPrefix: /home/runner/_work/spiceai/spiceai",
                (db / "codeql-database.yml").read_text(),
            )

            stdout2 = io.StringIO()
            with redirect_stdout(stdout2):
                self.mod["relocate_database_tree"](root, local)
            self.assertIn("Relocated 0 CodeQL query paths", stdout2.getvalue())

    def test_missing_pack_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "imported"
            db = root / "rust" / "temp"
            db.mkdir(parents=True)
            (db / "config-queries.qls").write_text(SUITE)
            local = Path(tmp) / "qlpacks"
            relative = DEV_QUERY.split("/codeql/qlpacks/", 1)[1]
            write_pack_file(local, relative)
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as raised:
                    self.mod["relocate_database_tree"](root, local)
            self.assertEqual(raised.exception.code, 1)
            self.assertIn("does not have", stderr.getvalue())
            self.assertIn("RegexInjection.ql", stderr.getvalue())
            self.assertIn("/home/runner/_work/_tool", (db / "config-queries.qls").read_text())

    def test_path_escape_outside_the_cli_packs_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "imported"
            db = root / "rust" / "temp"
            db.mkdir(parents=True)
            local = Path(tmp) / "qlpacks"
            local.mkdir()
            escaped = (
                "/home/runner/_work/_tool/CodeQL/2.27.0/x64/codeql/qlpacks/"
                "../../outside.ql"
            )
            (db / "config-queries.qls").write_text(f" -\n  query: {escaped}\n")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as raised:
                    self.mod["relocate_database_tree"](root, local)
            self.assertEqual(raised.exception.code, 1)
            self.assertIn("..", stderr.getvalue())
            self.assertIn("/home/runner/_work/_tool", (db / "config-queries.qls").read_text())

    def test_database_without_a_suite_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "imported"
            root.mkdir()
            local = Path(tmp) / "qlpacks"
            local.mkdir()
            with self.assertRaises(SystemExit) as raised:
                self.mod["relocate_database_tree"](root, local)
            self.assertEqual(raised.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
