#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Tests for scripts/nextest_build_targets.py. They run on synthetic
# `cargo metadata` and need no cargo. The script must never build a subset of
# what the filterset can select: each case below is a way that could happen.

from __future__ import annotations

import contextlib
import io
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import nextest_build_targets as nbt  # noqa: E402


def metadata(packages: dict[str, list[tuple[str, list[str]]]], features: dict[str, list[str]] | None = None) -> dict:
    """`packages` maps a package name to its (test target, required-features)."""
    features = features or {}
    pkgs = []
    for name, tests in packages.items():
        targets = [{"name": name.replace("-", "_"), "kind": ["lib"], "test": True}]
        targets += [{"name": t, "kind": ["test"], "test": True, "required-features": req} for t, req in tests]
        pkgs.append({"id": f"id-{name}", "name": name, "targets": targets})
    return {
        "packages": pkgs,
        "workspace_members": [p["id"] for p in pkgs],
        "resolve": {"nodes": [{"id": f"id-{n}", "features": features.get(n, [])} for n in packages]},
    }


def tests_of(args: list[str]) -> list[str]:
    return [args[i + 1] for i, a in enumerate(args) if a == "--test"]


WORKSPACE = metadata(
    {
        "cayenne": [("acid_test", []), ("odd_name", []), ("vs_duckdb", ["duckdb"]), ("vs_chdb", ["chdb"])],
        "runtime": [("integration", []), ("metrics", [])],
        "spice": [("cli_integration", []), ("cloud_integration", [])],
        "testoperator": [("dispatch", [])],
    },
    features={"cayenne": ["duckdb"]},
)


def run(filterset: str, meta: dict = WORKSPACE, selection: list[str] | None = None) -> list[str]:
    return nbt.target_args(meta, nbt.parse_selection(selection or ["--all"]), nbt.parse_filterset(filterset))


class SelectionTest(unittest.TestCase):
    def test_lib_and_bins_are_always_built(self) -> None:
        self.assertEqual(run("none()")[:2], ["--lib", "--bins"])
        self.assertEqual(tests_of(run("none()")), [])

    def test_package_and_kind_selects_every_test_of_the_package(self) -> None:
        # A new cayenne test file needs no edit, whatever its name.
        self.assertEqual(tests_of(run("package(=cayenne) & kind(=test)")), ["acid_test", "odd_name", "vs_duckdb"])

    def test_unmet_required_features_are_skipped_like_tests_does(self) -> None:
        self.assertNotIn("vs_chdb", tests_of(run("kind(=test)")))

    def test_binary_without_package_matches_in_any_package(self) -> None:
        self.assertEqual(tests_of(run("binary(=metrics)")), ["metrics"])

    def test_package_and_binary(self) -> None:
        self.assertEqual(tests_of(run("package(=spice) & binary(=cli_integration)")), ["cli_integration"])

    def test_test_level_predicate_builds_the_package_conservatively(self) -> None:
        # `test()` is decided only after the build, so the target must be built.
        self.assertEqual(tests_of(run("package(=testoperator) & (test(=a::b) | test(=c))")), ["dispatch"])

    def test_union_operators_and_precedence(self) -> None:
        got = tests_of(run("kind(=lib) + kind(=bin) + (package(=spice) & binary(=cli_integration)) | binary(=metrics)"))
        self.assertEqual(got, ["cli_integration", "metrics"])
        # `&` binds tighter than `|`.
        self.assertEqual(tests_of(run("binary(=metrics) | package(=spice) & binary(=cli_integration)")), ["cli_integration", "metrics"])

    def test_negation_and_difference(self) -> None:
        self.assertEqual(tests_of(run("package(=spice) - binary(=cloud_integration)")), ["cli_integration"])
        self.assertEqual(tests_of(run("package(=spice) & not binary(=cloud_integration)")), ["cli_integration"])
        # not(unknown) stays unknown, so the target is still built.
        self.assertEqual(tests_of(run("package(=testoperator) & not test(=x)")), ["dispatch"])

    def test_matchers(self) -> None:
        self.assertEqual(tests_of(run("binary(~integration) & package(=spice)")), ["cli_integration", "cloud_integration"])
        self.assertEqual(tests_of(run("binary(#cli_*)")), ["cli_integration"])
        self.assertEqual(tests_of(run("binary(/^acid_(test)$/)")), ["acid_test"])
        self.assertEqual(tests_of(run("binary_id(=runtime::metrics)")), ["metrics"])
        self.assertEqual(tests_of(run("package(spic*) & binary(=cli_integration)")), ["cli_integration"])

    def test_extra_filter_intersection_narrows(self) -> None:
        self.assertEqual(tests_of(run("(kind(=test)) & (not package(cayenne))")), sorted(
            ["integration", "metrics", "cli_integration", "cloud_integration", "dispatch"]))

    def test_excluded_and_unselected_packages_are_left_out(self) -> None:
        self.assertEqual(tests_of(run("kind(=test)", selection=["--all", "--exclude", "runtime", "--exclude=spice"])),
                         ["acid_test", "dispatch", "odd_name", "vs_duckdb"])
        self.assertEqual(tests_of(run("kind(=test)", selection=["-p", "spice"])), ["cli_integration", "cloud_integration"])

    def test_shared_name_with_an_unbuildable_target_is_refused(self) -> None:
        meta = metadata({"a": [("same", [])], "b": [("same", ["off"])]})
        with self.assertRaises(ValueError):
            run("package(=a)", meta)

    def test_dependency_feature_requirement_is_refused(self) -> None:
        meta = metadata({"a": [("t", ["dep/feature"])]})
        with self.assertRaises(ValueError):
            run("all()", meta)


class ParserTest(unittest.TestCase):
    def test_rejects_unknown_predicates_and_bad_syntax(self) -> None:
        for bad in ("bogus(=x)", "package(=a", "(package(=a)", "package(=a) &", "package(=a) package(=b)"):
            with self.subTest(bad=bad), self.assertRaises(nbt.FiltersetError):
                nbt.parse_filterset(bad)

    def test_word_operator_is_not_a_predicate_prefix(self) -> None:
        # `not` must not swallow the start of a predicate name like `none()`.
        self.assertEqual(tests_of(run("all() - none()"))[:1], ["acid_test"])


class MainTest(unittest.TestCase):
    def test_unparseable_filter_falls_back_to_every_test_target(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = nbt.main(["--filter", "bogus(=x)", "--", "--all"])
        self.assertEqual(code, 0)
        self.assertEqual(out.getvalue().strip(), "--tests")
        self.assertIn("warning", err.getvalue())

    def test_selection_flags_are_parsed(self) -> None:
        sel = nbt.parse_selection(["--all", "--exclude", "libnfs", "--features", "a/b", "--features=c", "--all-features", "-p", "x"])
        self.assertEqual(sel.excludes, ["libnfs"])
        self.assertEqual(sel.packages, ["x"])
        self.assertEqual(sel.feature_args, ["--features", "a/b", "--features", "c", "--all-features"])


if __name__ == "__main__":
    unittest.main()
