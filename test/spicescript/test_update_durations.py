#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0
"""Tests for update_durations.py: which CI samples a script's recorded duration comes from."""
import json
from pathlib import Path
import tempfile
import unittest

import update_durations

SCRIPT = "test/spicescript/full-refresh-chaos.yaml"


def results(directory, n, outcome, seconds):
    """Writes a `--results` file holding one run of SCRIPT."""
    path = Path(directory) / f"r{n}.json"
    path.write_text(json.dumps({
        "partition": "1/4",
        "complete": True,
        "seconds": seconds,
        "scripts": [{
            "file": SCRIPT,
            "name": "full-refresh-chaos",
            "outcome": outcome,
            "seconds": seconds,
            "checks_passed": 0,
            "failures": [],
            "problems": [] if outcome == "passed" else ["setup: Failed to start source 'src'"],
            "expected_failures": 0,
            "no_longer_failing": [],
            "notes": [],
        }],
    }))
    return path


class Medians(unittest.TestCase):
    def test_failed_runs_do_not_count(self):
        # One full run and two that failed starting a source: the failures'
        # partial times would make the median 1.
        with tempfile.TemporaryDirectory() as d:
            files = [results(d, 1, "passed", 458.0), results(d, 2, "failed", 0.7), results(d, 3, "failed", 0.7)]
            self.assertEqual(update_durations.medians(files), {SCRIPT: 458})

    def test_a_script_with_only_failed_runs_is_not_measured(self):
        with tempfile.TemporaryDirectory() as d:
            files = [results(d, 1, "failed", 0.7)]
            self.assertEqual(update_durations.medians(files), {})

    def test_the_median_of_passed_runs_rounds_up(self):
        with tempfile.TemporaryDirectory() as d:
            files = [results(d, 1, "passed", 100.2), results(d, 2, "passed", 300.0), results(d, 3, "passed", 200.1)]
            self.assertEqual(update_durations.medians(files), {SCRIPT: 201})


if __name__ == "__main__":
    unittest.main()
