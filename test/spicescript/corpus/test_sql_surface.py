# Copyright 2024-2026 The Spice.ai OSS Authors
# Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0
"""Guards for SQL documentation shapes that caused missing or malformed cases."""
from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()
