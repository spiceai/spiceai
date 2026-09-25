"""Unit tests for check_nextest_config.py."""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_nextest_config  # noqa: E402

# The config as it stood before #12336 was fixed: the convergence binaries carry
# `retries = 0` but no ceiling of their own, so they inherit the global 360s.
PRE_FIX_CONFIG = """\
[profile.default]
retries = { backoff = "exponential", count = 5, delay = "2s", max-delay = "30s", jitter = true }
slow-timeout = { period = "120s", terminate-after = 3 }

[test-groups]
cayenne-property-tests = { max-threads = 2 }

[[profile.default.overrides]]
filter = 'binary(=mutation_property_test) | binary(=cdc_compaction_delete_race_test) | binary(=maintained_aggregate_filter_test)'
test-group = 'cayenne-property-tests'
retries = 0
"""

# The fix: the same overrides, plus an explicit ceiling for the slow binaries.
# `mutation_model_test` is split out at a lower ceiling, mirroring the real
# config: its baseline is a third of the others', so 960s already clears the
# required factor and the retries it keeps would pay for anything above that.
FIXED_CONFIG = PRE_FIX_CONFIG + """\
slow-timeout = { period = "120s", terminate-after = 12 }

[[profile.default.overrides]]
filter = 'binary(=partition_chunking_test) | binary(=layout_pruning_ab_test)'
slow-timeout = { period = "120s", terminate-after = 12 }

[[profile.default.overrides]]
filter = 'binary(=mutation_model_test)'
slow-timeout = { period = "120s", terminate-after = 8 }

[[profile.default.overrides]]
filter = 'binary(=result_correctness_vs_duckdb_test)'
retries = 0
slow-timeout = { period = "120s", terminate-after = 12 }
"""

# The ceiling sized against the 3.7x contention factor measured for #12336: high
# enough to clear every baseline at that factor, and still reached once the pool
# got busier (#12811). Only the 12s drop to 8 — `mutation_model_test` is already
# there, which is the point of it being separate.
UNDERSIZED_CONFIG = FIXED_CONFIG.replace("terminate-after = 12", "terminate-after = 8")


# The override that gives `retention_oom` a budget its step can hold: three
# attempts at the global 360s ceiling, plus startup, inside a 25-minute step.
RETENTION_OVERRIDE = """\

[[profile.default.overrides]]
filter = 'binary(=retention_oom)'
retries = { backoff = "exponential", count = 2, delay = "2s", max-delay = "30s", jitter = true }
"""

# The config as #12336's fix left it, before #13512: every ceiling right, and
# `retention_oom` still on the default six attempts.
PRE_13512_CONFIG = FIXED_CONFIG
FIXED_CONFIG += RETENTION_OVERRIDE
UNDERSIZED_CONFIG += RETENTION_OVERRIDE


def workflow_with_step_bound(minutes: int | None) -> str:
    """A cut-down integration.yml whose retention step carries the given bound."""
    bound = f"        timeout-minutes: {minutes}\n" if minutes is not None else ""
    return (
        "jobs:\n"
        "  test-partitions:\n"
        "    timeout-minutes: 90\n"
        "    steps:\n"
        "      - name: Run integration test\n"
        "        timeout-minutes: 40\n"
        "        run: cargo nextest run\n"
        "      - name: Run retention OOM regression test\n"
        "        if: matrix.partition == 'count:1/3'\n"
        f"{bound}"
        "        run: cargo nextest run -E 'binary_id(=runtime::retention_oom)'\n"
        "      - name: Upload integration snapshots artifact\n"
        "        timeout-minutes: 10\n"
    )


def write(tmp: str, text: str, step_bound: int | None = 25, workflow: str | None = None) -> Path:
    """Write the config, and the workflow STEP_BUDGETS reads, under `tmp`.

    The config goes to `<tmp>/.config/nextest.toml` so the default repo-root
    derivation in `check_config` finds the workflow beside it.
    """
    root = Path(tmp)
    (root / ".config").mkdir(exist_ok=True)
    path = root / ".config" / "nextest.toml"
    path.write_text(text, encoding="utf-8")
    workflow_path = root / ".github" / "workflows" / "integration.yml"
    workflow_path.parent.mkdir(parents=True, exist_ok=True)
    workflow_path.write_text(
        workflow if workflow is not None else workflow_with_step_bound(step_bound),
        encoding="utf-8",
    )
    return path


class DurationTest(unittest.TestCase):
    def test_parses_each_supported_unit(self):
        self.assertEqual(check_nextest_config.parse_duration("120s"), 120.0)
        self.assertEqual(check_nextest_config.parse_duration("2m"), 120.0)
        self.assertEqual(check_nextest_config.parse_duration("1h"), 3600.0)
        self.assertEqual(check_nextest_config.parse_duration("500ms"), 0.5)
        self.assertEqual(check_nextest_config.parse_duration("1.5m"), 90.0)

    def test_rejects_an_unparseable_duration(self):
        for text in ("120", "s", "", "120 s", "2days"):
            with self.assertRaises(ValueError):
                check_nextest_config.parse_duration(text)


class RetryCountTest(unittest.TestCase):
    def test_reads_a_table_and_a_bare_count(self):
        self.assertEqual(check_nextest_config.retry_count({"count": 5, "backoff": "exponential"}), 5)
        self.assertEqual(check_nextest_config.retry_count(3), 3)
        self.assertEqual(check_nextest_config.retry_count(None), 0)

    def test_rejects_a_count_it_cannot_read(self):
        for value in (True, {"backoff": "fixed"}, {"count": "5"}, "5", -1, {"count": -3}):
            with self.assertRaises(ValueError):
                check_nextest_config.retry_count(value)


class CeilingTest(unittest.TestCase):
    def test_period_times_terminate_after(self):
        self.assertEqual(
            check_nextest_config.ceiling_seconds({"period": "120s", "terminate-after": 8}), 960.0
        )

    def test_a_bare_duration_sets_no_ceiling(self):
        """Without `terminate-after` nextest only marks the test SLOW; it never kills it."""
        self.assertIsNone(check_nextest_config.ceiling_seconds("120s"))

    def test_a_period_without_terminate_after_sets_no_ceiling(self):
        self.assertIsNone(check_nextest_config.ceiling_seconds({"period": "120s"}))

    def test_nothing_configured_sets_no_ceiling(self):
        """A config with no slow-timeout at all cannot kill a test."""
        self.assertIsNone(check_nextest_config.ceiling_seconds(None))


class BinaryFilterTest(unittest.TestCase):
    def test_matches_each_disjunct(self):
        self.assertEqual(
            check_nextest_config.binaries_matched("binary(=a_test) | binary(=b_test)"),
            {"a_test", "b_test"},
        )

    def test_a_test_name_filter_matches_no_binary(self):
        """A `test(=…)` filter cannot be resolved to a binary, and must not be guessed at."""
        self.assertEqual(
            check_nextest_config.binaries_matched("test(=mysql::replication_e2e::foo)"), set()
        )

    def test_rejects_binary_filter_spellings_it_cannot_resolve(self):
        for expr in ("binary(~mutation)", "binary(/mutation.*/)", "binary-id(cayenne::foo)"):
            with self.assertRaises(ValueError):
                check_nextest_config.binaries_matched(expr)


class ResolveTest(unittest.TestCase):
    def test_first_matching_override_wins_per_setting(self):
        """nextest evaluates precedence separately for each setting."""
        config = {
            "profile": {
                "default": {
                    "retries": 5,
                    "slow-timeout": {"period": "120s", "terminate-after": 3},
                    "overrides": [
                        {"filter": "binary(=a_test)", "retries": 0},
                        {
                            "filter": "binary(=a_test)",
                            "retries": 9,
                            "slow-timeout": {"period": "60s", "terminate-after": 2},
                        },
                    ],
                }
            }
        }
        self.assertEqual(check_nextest_config.resolve(config, "a_test", "retries"), 0)
        # The second block still supplies the setting the first one omits.
        self.assertEqual(
            check_nextest_config.resolve(config, "a_test", "slow-timeout"),
            {"period": "60s", "terminate-after": 2},
        )

    def test_falls_back_to_the_profile_default(self):
        config = {"profile": {"default": {"retries": 5, "overrides": []}}}
        self.assertEqual(check_nextest_config.resolve(config, "z_test", "retries"), 5)


class CheckConfigTest(unittest.TestCase):
    def test_the_pre_fix_config_is_rejected(self):
        """Regression test for #12336: this is the exact config that failed sign-off."""
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, PRE_FIX_CONFIG))
        self.assertTrue(problems)
        joined = "\n".join(problems)
        # The 360s ceiling is below every recorded baseline x 3.7.
        self.assertIn("mutation_property_test is killed after 360s", joined)
        self.assertIn("partition_chunking_test is killed after 360s", joined)
        # And retries = 0 on top of an inherited ceiling is the unrecoverable case.
        self.assertIn("inherits the global slow-timeout", joined)

    def test_the_fixed_config_is_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(check_nextest_config.check_config(write(tmp, FIXED_CONFIG)), [])

    def test_a_binary_keeps_the_ceiling_its_own_baseline_asks_for(self):
        """960s is enough for mutation_model_test, so the guard must not ask for more.

        Its 107.1s baseline needs 616s at 4.6 x 1.25, which the 960s in
        FIXED_CONFIG clears — while its two block-mates need 978s and 1425s and
        so sit at 1440s. Without this, raising the whole block to match the
        slowest member would read as required rather than as a choice, and this
        binary keeps the default retries: every second above what it needs is
        spent once per attempt when a genuine hang is caught.
        """
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, FIXED_CONFIG))
        self.assertNotIn("mutation_model_test", "\n".join(problems))

    def test_rejects_the_lower_ceiling_for_a_binary_that_needs_more(self):
        """The split is per-baseline, not a licence to move any binary down to 960s.

        Demotes layout_pruning_ab_test into the lower block — it needs 978s, so
        960s no longer clears it. Its name has to leave the 1440s filter as well
        as join the 960s one: nextest takes the first override a binary matches,
        which is what makes the earlier block win otherwise.
        """
        config = FIXED_CONFIG.replace(
            "filter = 'binary(=partition_chunking_test) | binary(=layout_pruning_ab_test)'",
            "filter = 'binary(=partition_chunking_test)'",
        ).replace(
            "filter = 'binary(=mutation_model_test)'",
            "filter = 'binary(=mutation_model_test) | binary(=layout_pruning_ab_test)'",
        )
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, config))
        self.assertIn("layout_pruning_ab_test is killed after 960s", "\n".join(problems))

    def test_rejects_a_ceiling_that_clears_the_baseline_but_not_contention(self):
        """600s clears the 247.9s baseline but not 247.9 x 4.6 x 1.25."""
        config = PRE_FIX_CONFIG.replace(
            'slow-timeout = { period = "120s", terminate-after = 3 }',
            'slow-timeout = { period = "120s", terminate-after = 5 }',
        )
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, config))
        self.assertTrue(any("needs at least 1425s" in p for p in problems))

    def test_rejects_a_ceiling_sized_to_the_contention_measured_for_12336(self):
        """Regression test for #12811: 960s cleared 3.7x, and the pool reached 4.6x.

        A ceiling sized to the worst contention measured at the time is correct
        only until the pool gets busier. This is the config that was in the tree
        when a sign-off was hard-failed by a wall-clock kill on a diff that could
        not reach `crates/cayenne`, so the guard has to reject it — both for the
        binaries whose baseline it no longer clears at all, and because a ceiling
        with no headroom is one busy day from the same failure.
        """
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, UNDERSIZED_CONFIG))
        joined = "\n".join(problems)
        self.assertIn("mutation_property_test is killed after 960s", joined)
        self.assertIn("partition_chunking_test is killed after 960s", joined)
        # `retries = 0` and an explicit ceiling both survive the resizing, so the
        # rejection must be about the size alone and not about either of those.
        self.assertNotIn("no longer has retries = 0", joined)
        self.assertNotIn("inherits the global slow-timeout", joined)

    def test_the_headroom_is_what_rejects_a_ceiling_that_only_matches_the_measurement(self):
        """A ceiling at exactly baseline x factor still fails, by design.

        `CONTENTION_FACTOR` is the worst slowdown seen so far, not a bound, so
        matching it exactly is the sizing that keeps having to be redone (#12811).
        1200s clears every baseline x 4.6 and is still rejected for the widest one.
        """
        config = FIXED_CONFIG.replace("terminate-after = 12", "terminate-after = 10")
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, config))
        widest = max(check_nextest_config.QUIET_BASELINE_SECONDS.values())
        self.assertGreater(1200, widest * check_nextest_config.CONTENTION_FACTOR)
        self.assertTrue(any("partition_chunking_test is killed after 1200s" in p for p in problems))

    def test_rejects_raising_the_global_ceiling_instead(self):
        """The tempting one-line 'fix' hides slowness across the whole workspace."""
        config = FIXED_CONFIG.replace(
            'slow-timeout = { period = "120s", terminate-after = 3 }',
            'slow-timeout = { period = "120s", terminate-after = 12 }',
            1,
        )
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, config))
        self.assertTrue(any("global [profile.default] slow-timeout" in p for p in problems))

    def test_rejects_dropping_retries_zero(self):
        """Buying headroom by allowing retries would mask the races these tests exist to catch."""
        config = FIXED_CONFIG.replace("retries = 0", "retries = 3")
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, config))
        self.assertTrue(any("no longer has retries = 0" in p for p in problems))

    def test_rejects_removing_the_ceiling_from_a_zero_retry_binary(self):
        """A bare period cannot kill a test — including a genuinely hung one.

        Dropping `terminate-after` is the other way to make a timeout stop
        failing, and it satisfies the baseline check by removing the ceiling
        rather than sizing it. The convergence binaries must keep a real one.
        """
        config = FIXED_CONFIG.replace(
            'slow-timeout = { period = "120s", terminate-after = 12 }',
            'slow-timeout = "120s"',
        )
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, config))
        never_terminates = [p for p in problems if "never terminates" in p]
        self.assertEqual(len(never_terminates), len(check_nextest_config.ZERO_RETRY_BINARIES))
        self.assertTrue(all("Set terminate-after" in p for p in never_terminates))

    def test_reports_a_slow_timeout_missing_its_period(self):
        """A `terminate-after` with no `period` is a config problem, not a crash."""
        config = FIXED_CONFIG.replace(
            'slow-timeout = { period = "120s", terminate-after = 12 }',
            "slow-timeout = { terminate-after = 12 }",
        )
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, config))
        self.assertTrue(any("no string period" in p for p in problems), problems)

    def test_reports_an_unresolvable_binary_filter(self):
        config = FIXED_CONFIG + """
[[profile.default.overrides]]
filter = 'binary(~mutation)'
retries = 0
"""
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, config))
        self.assertTrue(any("unsupported binary filter spelling" in p for p in problems))

    def test_reports_malformed_toml(self):
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, "[profile.default\n"))
        self.assertTrue(any("does not parse as TOML" in p for p in problems))

    def test_reports_a_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(Path(tmp) / "absent.toml")
        self.assertTrue(any("not found" in p for p in problems))


class MainTest(unittest.TestCase):
    def test_the_pre_13512_budget_is_rejected(self):
        # The default six attempts at 360s inside a 20-minute step: 36 minutes of
        # budget that the step could never hold, which is what timed the step out
        # fifteen times in the merge queue.
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(
                write(tmp, PRE_13512_CONFIG, step_bound=20)
            )
        self.assertTrue(any("retention_oom" in p and "no verdict" in p for p in problems), problems)
        (problem,) = [p for p in problems if "retention_oom" in p]
        self.assertIn("6 attempt(s) x 360s", problem)
        self.assertIn("20 min (1200s)", problem)

    def test_the_fixed_budget_fits_its_step(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(check_nextest_config.check_config(write(tmp, FIXED_CONFIG, step_bound=25)), [])

    def test_the_fixed_budget_does_not_fit_the_old_step_bound(self):
        # Three attempts x 360s + 240s startup = 1320s, over a 20-minute step: the
        # override alone is not enough, the step bound has to move with it.
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, FIXED_CONFIG, step_bound=20))
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("1320s", problems[0])
        self.assertIn("3 attempt(s)", problems[0])

    def test_a_bare_retry_count_is_read(self):
        config = FIXED_CONFIG.replace(
            'retries = { backoff = "exponential", count = 2, delay = "2s", max-delay = "30s", jitter = true }',
            "retries = 2",
        )
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(check_nextest_config.check_config(write(tmp, config, step_bound=25)), [])

    def test_a_step_without_a_bound_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, FIXED_CONFIG, step_bound=None))
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("has no timeout-minutes", problems[0])

    def test_a_renamed_step_is_reported_rather_than_skipped(self):
        workflow = workflow_with_step_bound(25).replace(
            "Run retention OOM regression test", "Run the retention OOM test"
        )
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, FIXED_CONFIG, workflow=workflow))
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("not found in the workflow", problems[0])

    def test_a_missing_workflow_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write(tmp, FIXED_CONFIG)
            (Path(tmp) / ".github" / "workflows" / "integration.yml").unlink()
            problems = check_nextest_config.check_config(path)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("integration.yml", problems[0])

    def test_a_budget_binary_without_a_ceiling_is_reported(self):
        config = FIXED_CONFIG + """\

[[profile.default.overrides]]
filter = 'binary(=retention_oom)'
slow-timeout = "120s"
"""
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_nextest_config.check_config(write(tmp, config, step_bound=25))
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("no slow-timeout ceiling", problems[0])

    def test_the_step_bound_is_read_from_the_named_step_only(self):
        # The neighbouring steps carry bounds of their own (40 and 10); the
        # scan must stop at the next step rather than read past it.
        self.assertEqual(
            check_nextest_config.step_timeout_minutes(
                workflow_with_step_bound(25), "Run retention OOM regression test"
            ),
            25,
        )
        self.assertIsNone(
            check_nextest_config.step_timeout_minutes(
                workflow_with_step_bound(None), "Run retention OOM regression test"
            )
        )

    def test_the_step_scan_stops_at_a_step_without_a_name(self):
        # The step after the target starts with `- uses:` rather than `- name:`,
        # and carries a bound of its own; the target has none.
        workflow = (
            "jobs:\n"
            "  test:\n"
            "    steps:\n"
            "      - name: Run retention OOM regression test\n"
            "        run: cargo nextest run\n"
            "      - uses: actions/upload-artifact@v4\n"
            "        timeout-minutes: 9\n"
        )
        self.assertIsNone(
            check_nextest_config.step_timeout_minutes(workflow, "Run retention OOM regression test")
        )

    def test_the_step_scan_stops_at_the_end_of_the_job(self):
        # The target is the last step; the next job's own bound is not its.
        workflow = (
            "jobs:\n"
            "  test:\n"
            "    steps:\n"
            "      - name: Run retention OOM regression test\n"
            "        run: cargo nextest run\n"
            "  aggregate:\n"
            "    timeout-minutes: 5\n"
            "    steps:\n"
            "      - run: true\n"
        )
        self.assertIsNone(
            check_nextest_config.step_timeout_minutes(workflow, "Run retention OOM regression test")
        )

    def test_a_nested_timeout_minutes_is_not_the_steps_bound(self):
        workflow = (
            "jobs:\n"
            "  test:\n"
            "    steps:\n"
            "      - name: Run retention OOM regression test\n"
            "        uses: some/action@v1\n"
            "        with:\n"
            "          timeout-minutes: 11\n"
            "        timeout-minutes: 25\n"
        )
        self.assertEqual(
            check_nextest_config.step_timeout_minutes(workflow, "Run retention OOM regression test"),
            25,
        )

    def test_a_duplicated_step_name_is_refused(self):
        workflow = workflow_with_step_bound(25) + (
            "  other:\n"
            "    steps:\n"
            "      - name: Run retention OOM regression test\n"
            "        timeout-minutes: 5\n"
        )
        with self.assertRaises(ValueError):
            check_nextest_config.step_timeout_minutes(workflow, "Run retention OOM regression test")

    def test_exit_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(check_nextest_config.main(["--config", str(write(tmp, FIXED_CONFIG))]), 0)
            self.assertEqual(
                check_nextest_config.main(["--config", str(write(tmp, PRE_FIX_CONFIG))]), 1
            )


class RepositoryTest(unittest.TestCase):
    def test_the_guard_workflow_triggers_on_every_step_budget_workflow(self):
        # The guard reads each STEP_BUDGETS workflow, so a change to one of them
        # alone — a step's timeout-minutes going back down — has to run it. A
        # budget whose workflow is missing from the trigger paths is a check that
        # cannot fire when it matters.
        guard = (
            Path(__file__).resolve().parents[1] / "workflows" / "nextest_config_check.yml"
        ).read_text(encoding="utf-8")
        for binary, (workflow, _step) in check_nextest_config.STEP_BUDGETS.items():
            with self.subTest(binary=binary):
                self.assertEqual(
                    guard.count(f"      - '{workflow}'"),
                    2,
                    f"{workflow} must be in both the pull_request and push path filters "
                    "of nextest_config_check.yml",
                )

    def test_this_repositorys_nextest_config_holds(self):
        """Regression test for #12336 / #12434 against the real .config/nextest.toml."""
        config_path = Path(__file__).resolve().parents[2] / ".config" / "nextest.toml"
        self.assertEqual(check_nextest_config.check_config(config_path), [])


if __name__ == "__main__":
    unittest.main()
