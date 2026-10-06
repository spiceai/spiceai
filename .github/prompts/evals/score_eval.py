#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Scores a writeReleaseNotes eval run against its assertions.
#
# Every assertion here is objectively checkable from the produced file, so a run
# can be graded the same way every time instead of by reading it. Assertions
# come in two families:
#
#   - style: the STE rules, delegated to `check_ste.py` so the skill and the
#     eval score the same thing.
#   - substance: the facts, PR coverage, noise filtering, and section structure
#     that a shorter-sentence rewrite must not quietly drop. These exist because
#     the cheapest way to pass a style check is to delete content, and that is
#     the failure mode worth catching.
#
# Usage:
#   python3 .github/prompts/evals/score_eval.py --eval create-patch-release \
#       --output <dir-or-file> [--json]

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROMPTS = os.path.dirname(HERE)
CHECK_STE = os.path.join(PROMPTS, "scripts", "check_ste.py")
FIXTURES = os.path.join(HERE, "fixtures")

REQUIRED_SECTIONS = [
    "## What's New",
    "## Contributors",
    "## Breaking Changes",
    "## Cookbook Updates",
    "## Upgrading",
    "## What's Changed",
    "### Changelog",
]


def read(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def ste_metrics(path: str) -> dict:
    result = subprocess.run(
        [sys.executable, CHECK_STE, "--json", path],
        capture_output=True,
        text=True,
        check=False,
    )
    payload = json.loads(result.stdout)
    return payload[0]


def changelog(text: str) -> str:
    index = text.find("### Changelog")
    return text[index:] if index != -1 else ""


def expectation(text: str, passed: bool, evidence: str) -> dict:
    return {"text": text, "passed": bool(passed), "evidence": evidence}


def style_expectations(path: str, max_passive_pct: float = 15.0) -> list[dict]:
    report = ste_metrics(path)
    metrics = report["metrics"]
    error_rules = [error["rule"] for error in report["errors"]]

    def rule_count(rule: str) -> int:
        return error_rules.count(rule)

    return [
        expectation(
            "STE checker reports zero errors",
            metrics["errors"] == 0,
            f"{metrics['errors']} errors: "
            + (", ".join(sorted(set(error_rules))) or "none"),
        ),
        expectation(
            "No authored sentence exceeds 25 words",
            metrics["sentences_over_limit"] == 0,
            f"longest sentence {metrics['max_sentence_words']} words, "
            f"{metrics['sentences_over_limit']} over the limit",
        ),
        expectation(
            "No participle clauses (using / enabling / including / by -ing)",
            metrics["participle_constructions"] == 0,
            f"{metrics['participle_constructions']} participle constructions",
        ),
        expectation(
            "No vague or figurative wording",
            metrics["vague_terms"] == 0,
            f"{metrics['vague_terms']} vague terms, {rule_count('vague-wording')} flagged",
        ),
        expectation(
            "No semicolons joining two ideas",
            metrics["semicolons"] == 0,
            f"{metrics['semicolons']} semicolons",
        ),
        expectation(
            f"Passive voice under {max_passive_pct:.0f}% of authored sentences",
            metrics["pct_passive"] < max_passive_pct,
            f"{metrics['pct_passive']}% passive "
            f"({metrics['passive_sentences']}/{metrics['prose_sentences']})",
        ),
        expectation(
            "Average sentence length at or under 20 words",
            metrics["avg_sentence_words"] <= 20,
            f"average {metrics['avg_sentence_words']} words",
        ),
    ]


def score_create(path: str) -> list[dict]:
    text = read(path)
    log = changelog(text)
    checks = style_expectations(path)

    missing = [s for s in REQUIRED_SECTIONS if s not in text]
    checks.append(
        expectation(
            "All required sections present",
            not missing,
            f"missing: {missing}" if missing else "all present",
        )
    )

    # 12027 is a dependency bump, but it moves the vendored DuckDB to a release
    # a user can observe, so it belongs in the changelog with the rest.
    shipped = [
        "12010", "12014", "12019", "12021", "12025",
        "12027", "12031", "12038", "12042",
    ]
    absent = [pr for pr in shipped if f"#{pr}" not in log]
    checks.append(
        expectation(
            "Every user-visible PR appears in the changelog",
            not absent,
            f"missing from changelog: {absent}"
            if absent
            else f"all {len(shipped)} present",
        )
    )

    duplicated = [pr for pr in shipped if log.count(f"pull/{pr}") > 1]
    checks.append(
        expectation(
            "No PR is listed twice in the changelog",
            not duplicated,
            f"duplicated: {duplicated}" if duplicated else "no duplicates",
        )
    )

    noise = {
        "12002": "test snapshot update",
        "12028": "routine serde bump",
        "12035": "Cargo.lock housekeeping",
        "12040": "revert of an unshipped change",
        "12044": "disabled flaky benchmark",
    }
    # The prompt excludes these from the changelog as well as the narrative, so
    # the whole document is searched. Scanning only the narrative would pass an
    # output that listed all five under `### Changelog`.
    leaked = [f"#{pr} ({why})" for pr, why in noise.items() if f"#{pr}" in text]
    checks.append(
        expectation(
            "Noise commits stay out of the notes",
            not leaked,
            f"leaked into the notes: {leaked}"
            if leaked
            else f"all {len(noise)} filtered",
        )
    )

    checks.append(
        expectation(
            "Breaking change documented with before/after migration",
            "runtime.cpu.limit" in text
            and "runtime.cpu.cores" in text
            and re.search(r"##\s+Breaking Changes\s*\n(?!\s*No breaking)", text)
            is not None,
            "Breaking Changes section names both the old and new setting"
            if "runtime.cpu.limit" in text
            else "rename not documented",
        )
    )

    checks.append(
        expectation(
            "User-visible DuckDB v1.6.0 upgrade recorded",
            "1.6.0" in text and "DuckDB" in text,
            "DuckDB 1.6.0 present" if "1.6.0" in text else "DuckDB bump missing",
        )
    )

    humans = ["lukekim", "sgrebnov", "phillipleblanc", "bjchambers", "Jeadie", "karifabri"]
    contributors = text[text.find("## Contributors") : text.find("## Breaking Changes")]
    missing_people = [h for h in humans if h not in contributors]
    bots_present = [b for b in ("dependabot", "github-actions", "claudespice") if b in contributors]
    checks.append(
        expectation(
            "Contributors lists every human author and no bots",
            not missing_people and not bots_present,
            f"missing: {missing_people}; bots: {bots_present}"
            if (missing_people or bots_present)
            else "6 humans, no bots",
        )
    )

    facts = [
        "cayenne_scan_concurrency",
        "on_conflict",
        "55006",
        "runtime.cpu.limit",
        "recency_decay",
        "503",
        "memory_limit",
    ]
    lost = [f for f in facts if f not in text]
    checks.append(
        expectation(
            "Key identifiers from the PR bodies survive into the notes",
            not lost,
            f"missing identifiers: {lost}" if lost else f"all {len(facts)} present",
        )
    )

    return checks


def score_rewrite(path: str) -> list[dict]:
    source = read(os.path.join(FIXTURES, "rewrite", "published-excerpt.md"))
    text = read(path)
    checks = style_expectations(path)

    identifiers = [
        "refresh_mode: changes",
        "pg_replication_slot",
        "max_replication_slots",
        "REPLICA IDENTITY FULL",
        "55006",
        "53300",
        "max_wal_senders",
        "pgoutput",
        "spice_cdc",
        "DataFusion",
        "v54",
        "v53",
        "v58.3",
        "v0.74",
        "Debezium",
    ]
    lost = [i for i in identifiers if i not in text]
    checks.append(
        expectation(
            "Every identifier, error code, and version from the source survives",
            not lost,
            f"lost: {lost}" if lost else f"all {len(identifiers)} preserved",
        )
    )

    def yaml_blocks(doc: str) -> list[str]:
        return [b.strip() for b in re.findall(r"```yaml\n(.*?)```", doc, re.DOTALL)]

    # Compare block to block, not block to document. Searching the raw text
    # passes an output that unfenced the sample or retyped the fence, and the
    # sample is only usable if a reader can copy it out of a YAML block.
    source_yaml = yaml_blocks(source)
    output_yaml = yaml_blocks(text)
    dropped = [b for b in source_yaml if b not in output_yaml]
    checks.append(
        expectation(
            "The YAML sample is reproduced unchanged, still in a YAML block",
            bool(source_yaml) and not dropped and len(output_yaml) == len(source_yaml),
            f"source has {len(source_yaml)} block(s), output has {len(output_yaml)}"
            + (f"; {len(dropped)} not reproduced verbatim" if dropped else ""),
        )
    )

    def prose_words(doc: str) -> int:
        stripped = re.sub(r"```.*?```", "", doc, flags=re.DOTALL)
        return len(re.findall(r"[A-Za-z0-9][A-Za-z0-9'`/._-]*", stripped))

    before, after = prose_words(source), prose_words(text)
    ratio = after / before if before else 0
    checks.append(
        expectation(
            "Content is rewritten, not deleted (word count within 80-160% of source)",
            0.8 <= ratio <= 1.6,
            f"{after} words vs {before} source ({ratio:.0%})",
        )
    )

    checks.append(
        expectation(
            "Section headings are preserved",
            "### Change Data Capture & HTAP" in text,
            "heading present" if "### Change Data Capture & HTAP" in text else "heading lost",
        )
    )

    metaphors = ["headlined by", "significant", "seamless", "powerful"]
    remaining = [m for m in metaphors if m in text.lower()]
    checks.append(
        expectation(
            "Marketing metaphors from the source are gone",
            not remaining,
            f"still present: {remaining}" if remaining else "all removed",
        )
    )

    return checks


def score_update(path: str) -> list[dict]:
    original = read(os.path.join(FIXTURES, "update", "v2.2.0-rc.1.md"))
    text = read(path)
    checks = style_expectations(path)

    def section(doc: str, heading: str) -> str:
        start = doc.find(heading)
        if start == -1:
            return ""
        rest = doc[start + len(heading) :]
        end = re.search(r"\n#{2,3} ", rest)
        return rest[: end.start()] if end else rest

    preserved = []
    for heading in ("### Bounded Cayenne Scan Concurrency", "### MongoDB Change Streams"):
        before = section(original, heading).strip()
        after = section(text, heading).strip()
        preserved.append((heading, before == after, len(before), len(after)))
    unchanged = all(ok for _, ok, _, _ in preserved)
    checks.append(
        expectation(
            "Pre-existing subsections are left byte-identical",
            unchanged,
            "; ".join(
                f"{h}: {'unchanged' if ok else f'changed ({b} -> {a} chars)'}"
                for h, ok, b, a in preserved
            ),
        )
    )

    new_prs = {"12070": "prompt caching", "12073": "rrf tie-break", "12077": "resume token"}
    log = changelog(text)
    missing = [f"#{pr} ({why})" for pr, why in new_prs.items() if f"#{pr}" not in log]
    checks.append(
        expectation(
            "New PRs added to the changelog",
            not missing,
            f"missing: {missing}" if missing else "all three added",
        )
    )

    kept = [pr for pr in ("12014", "12061") if f"#{pr}" in log]
    checks.append(
        expectation(
            "Original changelog entries are retained",
            len(kept) == 2,
            f"retained {kept}",
        )
    )

    # Excluded from the changelog as well as the narrative, so search the whole
    # document rather than the part above `## Contributors`.
    leaked = [pr for pr in ("12074", "12079") if f"#{pr}" in text]
    checks.append(
        expectation(
            "Snapshot and dependency noise stays out of the notes",
            not leaked,
            f"leaked: {leaked}" if leaked else "both filtered",
        )
    )

    contributors = text[text.find("## Contributors") : text.find("## Breaking Changes")]
    expected_people = ["lukekim", "sgrebnov", "Jeadie", "karifabri"]
    missing_people = [h for h in expected_people if h not in contributors]
    bots = [b for b in ("dependabot", "github-actions") if b in contributors]
    checks.append(
        expectation(
            "New contributors added, bots excluded",
            not missing_people and not bots,
            f"missing: {missing_people}; bots: {bots}"
            if (missing_people or bots)
            else "4 humans, no bots",
        )
    )

    substance = ["prompt_cache", "12070", "resume token", "tie"]
    thin = [s for s in substance if s.lower() not in text.lower()]
    checks.append(
        expectation(
            "New entries carry the substance from the PR bodies",
            not thin,
            f"missing: {thin}" if thin else "all present",
        )
    )

    return checks


SCORERS = {
    "create-patch-release": score_create,
    "rewrite-to-ste": score_rewrite,
    "update-in-progress-notes": score_update,
}


def resolve_output(path: str) -> str:
    if os.path.isfile(path):
        return path
    candidates = [
        os.path.join(root, name)
        for root, _, files in os.walk(path)
        for name in files
        if name.endswith(".md")
    ]
    if not candidates:
        raise SystemExit(f"No .md output found under {path}")
    # The largest Markdown file is the release notes; anything else is a note.
    return max(candidates, key=os.path.getsize)


def main() -> int:
    parser = argparse.ArgumentParser(description="Score a writeReleaseNotes eval run.")
    parser.add_argument("--eval", required=True, choices=sorted(SCORERS))
    parser.add_argument("--output", required=True, help="Output file or directory")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    path = resolve_output(args.output)
    expectations = SCORERS[args.eval](path)
    passed = sum(1 for e in expectations if e["passed"])
    result = {
        "eval": args.eval,
        "output": path,
        "passed": passed,
        "total": len(expectations),
        "pass_rate": round(passed / len(expectations), 3),
        "expectations": expectations,
    }

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"\n{args.eval}: {passed}/{len(expectations)} passed  ({path})")
        for item in expectations:
            mark = "PASS" if item["passed"] else "FAIL"
            print(f"  [{mark}] {item['text']}")
            print(f"         {item['evidence']}")

    return 0 if passed == len(expectations) else 1


if __name__ == "__main__":
    sys.exit(main())
