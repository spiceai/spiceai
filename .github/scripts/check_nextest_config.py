#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# nextest wall-clock ceiling guard.
#
# `.config/nextest.toml` gives every test a `slow-timeout` — a period, and a
# `terminate-after` count of periods after which nextest kills the test. That
# ceiling exists to catch hangs. It is not a statement about how long the work
# takes, so when a genuinely slow test's runtime drifts up into it, the ceiling
# silently changes meaning: it stops catching hangs and starts reporting pool
# contention as a test failure.
#
# That is #12336 / #12434 / #12811. The `cayenne::mutation_property_test`
# concurrency property tests need 2-4 minutes of real work, and contention on the
# self-hosted pool stretches them severalfold. Sign-off then fails on unrelated
# PRs: in run 30858006237 one variant passed at 356.1s while its matched sibling
# was killed at 360.015s. Those binaries also carry `retries = 0` (a retry that
# passes would hide a real convergence race), so the kill is an unrecoverable
# merge-queue failure.
#
# This guard keeps the ceiling ahead of the measurements instead of trusting a
# comment to stay true. For each binary with a recorded baseline it resolves the
# ceiling the way nextest does and requires it to clear
# `baseline * CONTENTION_FACTOR * CONTENTION_HEADROOM` — the worst contention
# measured so far, plus room for the next increment, because a ceiling sized to
# exactly the last measurement is one busy day from failing again (#12811). It
# also asserts the two settings stay coupled:
# the convergence binaries must keep `retries = 0` (so a real failure is never
# retried into a pass) *and* keep an explicit ceiling (so a timeout is not a hard
# gate failure).
"""Validate the repository's nextest slow-timeout ceilings against measured runtimes."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path

# Slowest observed duration for each long-running test binary on a QUIET pool.
#
# Source: `Remote Sign-off` run 30833882620 (trunk @ 7b98c234), which ran with no
# other sign-off in flight. Value is the slowest single test in that binary.
QUIET_BASELINE_SECONDS = {
    "mutation_property_test": 239.5,  # prop_concurrent_mixed_position_sqlite
    "partition_chunking_test": 247.9,  # ..._timestamp_partition_with_date_part_impl_sqlite
    "layout_pruning_ab_test": 170.1,  # pruning_ab_inferred_vs_authoritative_sort
    "mutation_model_test": 107.1,  # test_exhaustive_composite_single_row_sequences_impl_sqlite
    # tpcds_and_clickbench_parity_vs_duckdb, measured with fixtures already on
    # disk. CI regenerates them with `dsdgen` on every fresh runner, so the
    # baseline is rounded up from the observed 127.7s to cover that.
    "result_correctness_vs_duckdb_test": 180.0,
}

# Worst same-test slowdown measured between a quiet pool and a saturated one.
#
# Source: sign-off run 31239284344, where
# `mutation_property_test prop_concurrent_upsert_only_key_sqlite` passed at
# 422.1s against a 92.0s quiet baseline in run 30833882620. Contention on this
# pool is shared-machine CPU starvation, so it applies to any binary here.
CONTENTION_FACTOR = 4.6

# Multiplier applied on top of `CONTENTION_FACTOR` when sizing a ceiling.
#
# `CONTENTION_FACTOR` is the worst slowdown observed *so far*. Sizing a ceiling
# to exactly that leaves it correct only until the pool gets busier by any
# margin at all, and each time it does, the miss costs a full sign-off cycle and
# reports a code failure on a diff that never touched these tests (#12811). So a
# ceiling has to clear the measured worst case by enough to absorb the next
# increment, not merely match it. Every additional second of headroom is paid
# for only by a genuine hang, which is a bug being fixed rather than a routine
# cost.
CONTENTION_HEADROOM = 1.25

# Binaries whose failures must never be retried: they assert that the accelerated
# table converges to a reference model, so a retry that happens to pass hides a
# real race rather than working around flakiness. Each must ALSO carry its own
# `slow-timeout` — with `retries = 0`, the ceiling is the difference between a
# recoverable timeout and a hard merge-queue failure.
ZERO_RETRY_BINARIES = frozenset(
    {
        "mutation_property_test",
        "cdc_compaction_delete_race_test",
        "maintained_aggregate_filter_test",
        # Engine-vs-engine parity: a mismatch is deterministic, so a retry buys
        # nothing but five more runs of a ~250s binary before the same failure.
        "result_correctness_vs_duckdb_test",
    }
)

# The global ceiling every other test gets. Pinned so that a future timeout is
# not "fixed" by raising this: it applies to the whole workspace and would hide
# slowness everywhere rather than recording a decision about one test family.
EXPECTED_GLOBAL_SLOW_TIMEOUT = {"period": "120s", "terminate-after": 3}

_DURATION = re.compile(r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>ms|s|m|h)$")
_UNIT_SECONDS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}

# The only binary-filter spelling this guard resolves. `binary(~x)`, `binary(/re/)`
# and `binary-id(...)` would each need different matching, so they are rejected
# rather than silently treated as non-matching.
_BINARY_EQ_TERM = re.compile(r"binary\(=([A-Za-z0-9_]+)\)")
_OTHER_BINARY_TERM = re.compile(r"binary(?:-id)?\((?!=[A-Za-z0-9_]+\))")


def parse_duration(text: str) -> float:
    """Return `text` ('120s', '2m', …) in seconds."""
    match = _DURATION.match(text.strip())
    if not match:
        raise ValueError(f"unparseable duration: {text!r}")
    return float(match.group("value")) * _UNIT_SECONDS[match.group("unit")]


def ceiling_seconds(slow_timeout: object) -> float | None:
    """Return the wall-clock kill ceiling a `slow-timeout` value implies.

    `None` means the value sets no ceiling — either nothing configured one, or a
    bare duration, which only marks the test SLOW in the output and never
    terminates it.
    """
    if slow_timeout is None or isinstance(slow_timeout, str):
        return None
    if not isinstance(slow_timeout, dict):
        raise ValueError(f"unexpected slow-timeout value: {slow_timeout!r}")
    terminate_after = slow_timeout.get("terminate-after")
    if terminate_after is None:
        return None
    # `period` is mandatory once `terminate-after` is set, but a hand-edited
    # table can omit it. Report that as a config problem like every other
    # malformed value here, rather than letting a KeyError escape as a crash.
    period = slow_timeout.get("period")
    if not isinstance(period, str):
        raise ValueError(
            f"slow-timeout sets terminate-after but no string period: {slow_timeout!r}"
        )
    if not isinstance(terminate_after, int) or isinstance(terminate_after, bool):
        raise ValueError(f"slow-timeout has a non-integer terminate-after: {slow_timeout!r}")
    return parse_duration(period) * terminate_after


def binaries_matched(filter_expr: str) -> set[str]:
    """Return the binaries a filter expression matches by exact name.

    Only `binary(=name)` terms are understood. A filter selecting tests by name
    (for example `test(=mysql::…)`) matches no binary here, which is correct: it
    cannot be resolved to a binary without knowing that binary's test names.
    """
    if _OTHER_BINARY_TERM.search(filter_expr):
        raise ValueError(
            f"unsupported binary filter spelling, cannot resolve a ceiling: {filter_expr!r}"
        )
    return set(_BINARY_EQ_TERM.findall(filter_expr))


def resolve(config: dict, binary: str, setting: str) -> object | None:
    """Resolve `setting` for `binary` the way nextest does.

    nextest evaluates precedence per setting: the first override that matches and
    configures that setting wins, and the profile default applies if none do.
    """
    profile = config.get("profile", {}).get("default", {})
    for override in profile.get("overrides", []):
        if setting not in override:
            continue
        if binary in binaries_matched(override.get("filter", "")):
            return override[setting]
    return profile.get(setting)


def check_config(config_path: Path) -> list[str]:
    """Return a list of problems with the nextest config at `config_path`."""
    if not config_path.is_file():
        return [f"{config_path}: not found"]
    try:
        # utf-8-sig so a stray BOM reports as a config problem, not a crash.
        config = tomllib.loads(config_path.read_text(encoding="utf-8-sig"))
    except tomllib.TOMLDecodeError as exc:
        return [f"{config_path}: does not parse as TOML: {exc}"]

    problems: list[str] = []
    name = config_path.name

    profile = config.get("profile", {}).get("default", {})
    try:
        for override in profile.get("overrides", []):
            binaries_matched(override.get("filter", ""))
    except ValueError as exc:
        # Every check below resolves settings per binary, so an unresolvable
        # filter would make the rest of this run meaningless rather than merely
        # incomplete.
        return [f"{name}: {exc}"]

    if profile.get("slow-timeout") != EXPECTED_GLOBAL_SLOW_TIMEOUT:
        problems.append(
            f"{name}: the global [profile.default] slow-timeout is "
            f"{profile.get('slow-timeout')!r}, expected {EXPECTED_GLOBAL_SLOW_TIMEOUT!r}. "
            "Raising the global ceiling hides slowness across the whole workspace; give "
            "the affected binaries their own override and record why."
        )

    for binary, baseline in sorted(QUIET_BASELINE_SECONDS.items()):
        required = baseline * CONTENTION_FACTOR * CONTENTION_HEADROOM
        try:
            ceiling = ceiling_seconds(resolve(config, binary, "slow-timeout"))
        except ValueError as exc:
            problems.append(f"{name}: {binary}: {exc}")
            continue
        if ceiling is None:
            continue  # No ceiling at all cannot be breached by contention.
        if ceiling < required:
            problems.append(
                f"{name}: {binary} is killed after {ceiling:.0f}s, but its slowest test "
                f"needs {baseline:.1f}s on a quiet pool and contention has been measured "
                f"multiplying runtimes by {CONTENTION_FACTOR}x, so with {CONTENTION_HEADROOM}x "
                f"headroom for the next increment it needs at least {required:.0f}s. As "
                "configured, pool load decides whether unrelated PRs pass. See #12336 / #12811."
            )

    for binary in sorted(ZERO_RETRY_BINARIES):
        if resolve(config, binary, "retries") != 0:
            problems.append(
                f"{name}: {binary} no longer has retries = 0. These tests assert the "
                "accelerated table converges to a reference model; a retry that passes "
                "hides a real race instead of working around flakiness."
            )
            continue
        try:
            ceiling = ceiling_seconds(resolve(config, binary, "slow-timeout"))
        except ValueError as exc:
            problems.append(f"{name}: {binary}: {exc}")
            continue
        if not has_override_setting(config, binary, "slow-timeout"):
            problems.append(
                f"{name}: {binary} has retries = 0 but inherits the global slow-timeout. "
                "With no retries a wall-clock kill is an unrecoverable merge-queue "
                "failure, so this binary needs a ceiling sized for it explicitly."
            )
        elif ceiling is None:
            # Deleting `terminate-after` would satisfy the baseline check above
            # by removing the ceiling entirely, which is the wrong way to make a
            # timeout stop failing: nothing then kills a genuine hang.
            problems.append(
                f"{name}: {binary} has retries = 0 and a slow-timeout that never "
                "terminates, so a hung test is never killed. It would run until the "
                "job's own timeout, costing the whole run and reporting as an "
                "infrastructure failure rather than a test one. Set terminate-after."
            )

    return problems


def has_override_setting(config: dict, binary: str, setting: str) -> bool:
    """Return whether an override — not the profile default — supplies `setting`."""
    return any(
        setting in override and binary in binaries_matched(override.get("filter", ""))
        for override in config.get("profile", {}).get("default", {}).get("overrides", [])
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parents[2] / ".config" / "nextest.toml",
        help="path to nextest.toml (default: the repository's .config/nextest.toml)",
    )
    args = parser.parse_args(argv)

    problems = check_config(args.config)
    if problems:
        print(f"FAIL: {len(problems)} problem(s) in {args.config}:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    print(
        f"OK: {args.config.name} keeps every measured binary's kill ceiling above "
        f"its quiet baseline x {CONTENTION_FACTOR} x {CONTENTION_HEADROOM}, and the "
        "convergence binaries keep retries = 0 with an explicit ceiling"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
