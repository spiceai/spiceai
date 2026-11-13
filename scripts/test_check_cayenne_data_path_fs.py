#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for check_cayenne_data_path_fs.py: each rule fails on the input it guards."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_cayenne_data_path_fs as guard  # noqa: E402


class CountTest(unittest.TestCase):
    def test_counts_std_and_tokio_fs(self):
        self.assertEqual(guard.count_calls("std::fs::read(p);\ntokio::fs::write(p, b).await;\n"), 2)

    def test_ignores_comments(self):
        self.assertEqual(guard.count_calls("// std::fs::read is not used here\nlet x = 1;\n"), 0)

    def test_ignores_inline_test_module(self):
        text = "fn a() {}\n#[cfg(test)]\nmod tests {\n    fn b() { std::fs::read(p); }\n}\n"
        self.assertEqual(guard.count_calls(text), 0)

    def test_counts_before_test_module(self):
        text = "fn a() { std::fs::remove_file(p); }\n#[cfg(test)]\nmod tests {\n std::fs::read(p);\n}\n"
        self.assertEqual(guard.count_calls(text), 1)

    def test_does_not_match_other_paths(self):
        self.assertEqual(guard.count_calls("let fs = my::fs::thing(); object_store::local::LocalFileSystem::new();"), 0)


class CompareTest(unittest.TestCase):
    def test_a_new_call_fails(self):
        problems = guard.compare({"a.rs": 3}, {"a.rs": 2})
        self.assertEqual(len(problems), 1)
        self.assertIn("3 filesystem call(s), 2 allowed", problems[0])

    def test_a_new_file_fails(self):
        problems = guard.compare({"new.rs": 1}, {})
        self.assertIn("1 filesystem call(s), 0 allowed", problems[0])

    def test_a_removed_call_asks_to_lower_the_entry(self):
        problems = guard.compare({"a.rs": 1}, {"a.rs": 2})
        self.assertIn("Lower its entry", problems[0])

    def test_a_cleared_file_asks_to_remove_the_entry(self):
        problems = guard.compare({}, {"a.rs": 2})
        self.assertIn("Remove its entry", problems[0])

    def test_matching_counts_pass(self):
        self.assertEqual(guard.compare({"a.rs": 2}, {"a.rs": 2}), [])


class FunctionTest(unittest.TestCase):
    def test_attributes_each_call_to_its_function(self):
        text = "fn probe() {\n    std::fs::read(p);\n}\nasync fn write() {\n    tokio::fs::write(p, b).await;\n    tokio::fs::rename(a, b).await;\n}\n"
        self.assertEqual(guard.calls_by_function(text), {"probe": 1, "write": 2})


class ScanTest(unittest.TestCase):
    def scan(self, files, host_keys=frozenset()):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, text in files.items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text(text)
            original_root = guard.ROOT
            guard.ROOT = root
            try:
                return guard.scan(root, host_keys)
            finally:
                guard.ROOT = original_root

    def test_skips_files_under_tests(self):
        found = self.scan({"tests/a.rs": "std::fs::read(p);\n", "b.rs": "tokio::fs::read(p).await;\n"})
        self.assertEqual(found, {"host_features": {}, "local_data_path": {"b.rs": 1}})

    def test_a_host_feature_file_is_counted_apart(self):
        found = self.scan({"probe.rs": "fn a() { std::fs::read(p); }\n"}, {"probe.rs"})
        self.assertEqual(found, {"host_features": {"probe.rs": 1}, "local_data_path": {}})

    def test_a_host_feature_function_leaves_the_rest_of_its_file_data_path(self):
        text = "fn measure() {\n    std::fs::read_dir(p);\n}\nfn write() {\n    std::fs::write(p, b);\n}\n"
        found = self.scan({"table.rs": text}, {"table.rs::measure"})
        self.assertEqual(
            found, {"host_features": {"table.rs::measure": 1}, "local_data_path": {"table.rs": 1}}
        )


if __name__ == "__main__":
    unittest.main()
