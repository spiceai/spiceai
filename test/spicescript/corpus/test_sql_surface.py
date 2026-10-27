# Copyright 2024-2026 The Spice.ai OSS Authors
# Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0
"""Guards for SQL documentation shapes that caused missing or malformed cases."""
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import sql_surface


class DocumentationInventoryTest(unittest.TestCase):
    def test_all_inline_and_bulleted_aliases_are_inventoried(self):
        with tempfile.TemporaryDirectory() as directory:
            docs = Path(directory)
            for name in ["aggregate_functions", "json", "ai", "search"]:
                (docs / f"{name}.md").write_text("")
            (docs / "scalar_functions.md").write_text(
                "### `array_position`\n"
                "Aliases: `list_position`, `array_indexof`, `list_indexof`.\n"
                "### `foo` and `bar`\n"
                "#### Aliases\n- first_alias\n- `second_alias`\n"
            )
            names = {entry["name"] for entry in sql_surface.documented_functions(docs)}
            self.assertEqual(names, {"array_position", "list_position", "array_indexof",
                                     "list_indexof", "foo", "bar", "first_alias", "second_alias"})

    def test_operands_with_shared_prefixes_keep_their_types(self):
        expression = sql_surface.substitute("date_add($dt, $i)", literals=True)
        self.assertEqual(expression, "date_add(DATE '2024-02-29', CAST(2 AS BIGINT))")

    def test_escape_case_uses_the_escape_clause(self):
        self.assertIn(" ESCAPE ", sql_surface.OPERATORS["LIKE ESCAPE"])

    def test_last_known_failure_removes_script_and_check_rejects_leftovers(self):
        for ledger, fixture in [("sql-surface-known-failures.json", "sql-surface.yaml"),
                                ("sql-surface-known-values.json", "operator-values.yaml")]:
            with self.subTest(ledger=ledger):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    suite = root / "test/spicescript"
                    shutil.copytree(sql_surface.HERE.parent, suite)
                    (root / "crates").symlink_to(sql_surface.HERE.parents[2] / "crates",
                                                 target_is_directory=True)
                    corpus = suite / "corpus"
                    script = suite / f"known-failing/sql-protocols/{fixture}"
                    stale_script = script.read_text() if script.exists() else "steps: []\n"
                    script.write_text(stale_script)
                    (corpus / ledger).write_text("{}\n")
                    command = [sys.executable, str(corpus / "sql_surface.py")]

                    def run(*args):
                        return subprocess.run(command + list(args), capture_output=True, text=True)

                    generated = run()
                    self.assertEqual(generated.returncode, 0, generated.stderr)
                    self.assertFalse(script.exists())
                    checked = run("--check")
                    self.assertEqual(checked.returncode, 0, checked.stderr)

                    script.write_text(stale_script)
                    checked = run("--check")
                    self.assertNotEqual(checked.returncode, 0)
                    self.assertIn(f"{script}: stale;", checked.stderr)
                    self.assertEqual(script.read_text(), stale_script)

                    regenerated = run()
                    self.assertEqual(regenerated.returncode, 0, regenerated.stderr)
                    self.assertFalse(script.exists())


if __name__ == "__main__":
    unittest.main()
