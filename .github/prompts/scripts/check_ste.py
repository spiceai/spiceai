#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Simplified Technical English (STE) checker for release notes.
#
# Reads a release notes Markdown file, extracts the author-written prose
# (summary, highlights, `## What's New` subsections, breaking changes, bug
# fixes) and reports where that prose breaks the STE writing rules described in
# `.github/prompts/references/simplified_technical_english.md`.
#
# Mechanical sections are excluded because their wording is dictated by the
# release process, not by an author: `## Contributors`, `## Upgrading`,
# `## What's Changed` (the changelog quotes PR titles verbatim so they stay
# greppable against git history), and `## Cookbook Updates`. Fenced code,
# tables, and inline code spans are excluded for the same reason - an
# identifier is not a sentence.
#
# Findings are split into errors (a rule the release notes must follow) and
# warnings (a rule with defensible exceptions, reported so an author can judge).
# Exit code is 1 when there is at least one error, so the check can gate a
# commit. `--json` prints machine-readable metrics for the eval harness.
#
# Usage:
#   python3 .github/prompts/scripts/check_ste.py docs/release_notes/v2.1.4.md
#   python3 .github/prompts/scripts/check_ste.py --json docs/release_notes/*.md

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field

# A descriptive sentence longer than this is doing more than one job.
MAX_SENTENCE_WORDS = 25

# A paragraph longer than this stops being skimmable.
MAX_PARAGRAPH_SENTENCES = 6

# Sections whose wording the release process owns, not the author.
MECHANICAL_SECTIONS = re.compile(
    r"^#{2,3}\s+(Contributors|Upgrading|What's Changed|Changelog|Cookbook Updates)\s*$",
    re.IGNORECASE,
)

# Participles that join two ideas into one sentence. STE asks for a finite verb
# in a new sentence instead, because "X, enabling Y" hides who does Y.
PARTICIPLE_CONNECTIVES = {
    "adding", "allowing", "avoiding", "being", "bringing", "causing",
    "changing", "choosing", "collapsing", "containing", "cutting",
    "delivering", "distinguishing", "dropping", "enabling", "ensuring",
    "expanding", "featuring", "folding", "freeing", "giving", "helping",
    "improving", "including", "increasing", "introducing", "keeping",
    "leading", "letting", "lowering", "making", "preventing", "providing",
    "raising", "reducing", "removing", "replacing", "requiring", "resulting",
    "returning", "staying", "supporting", "unlocking", "using",
}

# Vague or figurative wording. Each entry maps to what to write instead.
BANNED_TERMS = {
    "leverage": "use",
    "leverages": "uses",
    "leveraging": "uses",
    "seamless": "name the actual behaviour",
    "seamlessly": "name the actual behaviour",
    "effortless": "name the actual behaviour",
    "effortlessly": "name the actual behaviour",
    "powerful": "state what it does",
    "blazing": "state the measured speed",
    "blazingly": "state the measured speed",
    "supercharge": "state what it does",
    "supercharges": "state what it does",
    "unlock": "state what becomes possible",
    "unlocks": "state what becomes possible",
    "cutting-edge": "state the version or capability",
    "best-in-class": "state the measurement",
    "battle-tested": "state the measurement",
    "game-changing": "state what changed",
    "delightful": "state what changed",
    "magic": "state the mechanism",
    "magical": "state the mechanism",
    "under the hood": "name the component",
    "out of the box": "say 'by default'",
    "first-class": "say what is supported",
    "heavy lifting": "name the work",
    "in the wild": "say 'in production'",
    "on the fly": "say 'at runtime'",
    "significantly": "give the number",
    "dramatically": "give the number",
    "drastically": "give the number",
    "greatly": "give the number",
    "vastly": "give the number",
    "massively": "give the number",
    "headlined by": "say 'The main change is'",
    "a number of": "give the number",
    "various": "name them or give the number",
    "myriad": "give the number",
    "plethora": "give the number",
    "robust": "state the failure it survives",
    "rich set": "list them",
}

# Pairs that must not both appear: the same concept written two ways forces the
# reader to decide whether the difference is meaningful.
TERM_VARIANTS = [
    ("change-data-capture", "change data capture"),
    ("data source", "datasource"),
    ("time zone", "timezone"),
    ("run time", "runtime"),
    ("file system", "filesystem"),
    ("start up", "startup"),
]

BE_VERBS = r"(?:is|are|was|were|be|been|being)"
PASSIVE_RE = re.compile(
    rf"\b{BE_VERBS}\s+(?:not\s+|now\s+|also\s+|already\s+|\w+ly\s+)*"
    r"(\w+ed|built|written|shown|given|sent|held|kept|made|set|read|run|drawn"
    r"|taken|seen|known|thrown|brought|found|lost|left|met|paid|put|sold|told"
    r"|thrown|rewritten|overridden|driven|chosen)\b",
    re.IGNORECASE,
)
PROGRESSIVE_RE = re.compile(rf"\b{BE_VERBS}\s+(\w+ing)\b", re.IGNORECASE)

# Past participles that work as plain adjectives. "The setting is deprecated"
# describes a state; it does not hide an actor, so it is not the passive voice
# the rule is aimed at.
ADJECTIVAL_PARTICIPLES = {
    "advanced", "based", "called", "dedicated", "deprecated", "detailed",
    "disabled", "distributed", "documented", "enabled", "experimental",
    "federated", "fixed", "included", "limited", "licensed", "located",
    "named", "nested", "related", "required", "shared", "supported",
    "unaffected", "unchanged", "undefined", "unsupported", "unused",
}
BY_GERUND_RE = re.compile(r"\bby\s+(\w+ing)\b", re.IGNORECASE)

# Words that cannot head a noun stack, used to bound the stack detector.
STACK_STOPWORDS = {
    "a", "an", "the", "and", "or", "but", "for", "nor", "so", "yet", "of",
    "in", "on", "at", "to", "from", "by", "with", "as", "into", "over",
    "under", "per", "via", "that", "this", "these", "those", "it", "its",
    "they", "them", "their", "we", "you", "your", "is", "are", "was", "were",
    "be", "been", "being", "has", "have", "had", "do", "does", "did", "can",
    "will", "would", "should", "may", "might", "must", "now", "not", "no",
    "when", "where", "which", "who", "what", "how", "if", "than", "then",
    "also", "only", "each", "every", "all", "both", "new", "CODEREF",
    "full", "small", "large", "other", "same", "single", "more", "less",
    "many", "much", "several", "own", "such", "still", "just", "first",
    "within", "without", "against", "across", "between", "before", "after",
    "during", "through", "around", "above", "below", "beyond", "upon",
    "toward", "instead", "rather", "while", "because", "since", "until",
}

# A finite verb ends a noun stack. There is no part-of-speech tagger here, so
# this list covers the verbs that actually appear in release notes prose.
STACK_VERBS = {
    "accept", "add", "allow", "apply", "call", "carry", "check", "close",
    "come", "control", "cover", "create", "cut", "derive", "detect", "drop",
    "enable", "end", "expose", "extend", "fail", "fill", "filter", "find",
    "fix", "fold", "get", "give", "go", "hold", "improve", "include",
    "increase", "join", "keep", "let", "link", "list", "load", "log", "make",
    "match", "mean", "name", "need", "open", "pass", "provide", "push",
    "read", "reduce", "release", "remove", "replace", "report", "require",
    "restore", "return", "run", "save", "say", "select", "send", "set",
    "show", "size", "split", "start", "stay", "stop", "support", "take",
    "target", "tell", "treat", "use", "win", "work", "write",
    "accelerate", "bound", "classify", "compact", "decode", "emit", "encode",
    "federate", "forward", "hold", "invalidate", "map", "propagate",
    "publish", "recover", "refresh", "replicate", "resolve", "retry",
    "scale", "span", "surface", "sustain", "terminate", "wrap",
}
STACK_VERBS |= {f"{verb}s" for verb in STACK_VERBS}
STACK_VERBS |= {"does", "has", "goes", "exports", "sizes", "wins", "applies",
                "carries", "matches", "passes", "pushes", "closes", "fixes"}

PLACEHOLDER = "CODEREF"


@dataclass
class Finding:
    line: int
    rule: str
    message: str
    excerpt: str


@dataclass
class Report:
    path: str
    errors: list[Finding] = field(default_factory=list)
    warnings: list[Finding] = field(default_factory=list)
    metrics: dict[str, float] = field(default_factory=dict)


def strip_markdown(text: str) -> str:
    """Reduce a Markdown line to the words a reader actually reads."""
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", text)  # images
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)  # links keep their text
    text = re.sub(r"<https?://[^>]+>", "", text)  # autolinks
    text = re.sub(r"https?://\S+", "", text)  # bare URLs
    text = re.sub(r"`[^`]*`", PLACEHOLDER, text)  # inline code is one token
    text = re.sub(r"[*_]{1,3}", "", text)  # bold / italic markers
    text = re.sub(r"<[^>]+>", "", text)  # stray HTML
    return text.strip()


def prose_lines(lines: list[str]) -> list[tuple[int, str, bool]]:
    """Yield (line_number, prose, is_list_item) for author-written prose only.

    The list flag matters for the paragraph-length rule: a run of bullets is
    already the structure that rule asks for, so bullets must not be counted
    together as one long paragraph.
    """
    out: list[tuple[int, str, bool]] = []
    in_code = False
    in_mechanical = False

    for idx, raw in enumerate(lines, start=1):
        line = raw.rstrip("\n")

        if line.lstrip().startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue

        if line.startswith("#"):
            # A new H2 always re-opens prose; an H3 inside a mechanical section
            # (`### Changelog`) keeps it closed.
            if MECHANICAL_SECTIONS.match(line):
                in_mechanical = True
            elif line.startswith("## "):
                in_mechanical = False
            continue

        if in_mechanical:
            continue
        if line.strip().startswith("|"):  # table row
            continue
        if line.strip().startswith("<!--"):
            continue
        if re.match(r"^\s*\*\*Full Changelog\*\*", line):
            continue

        body = line.lstrip()
        body = re.sub(r"^>\s?", "", body)  # blockquote callout is still prose
        is_list_item = bool(re.match(r"^([-*+]|\d+\.)\s+", body))
        body = re.sub(r"^[-*+]\s+", "", body)  # bullet marker
        body = re.sub(r"^\d+\.\s+", "", body)  # ordered list marker
        # A bold lead-in label ("**Shared Replication Slot**:") is a heading in
        # disguise, so drop it rather than fold it into the first sentence.
        body = re.sub(r"^\*\*[^*]{1,60}\*\*:\s*", "", body)

        text = strip_markdown(body)
        if not text or text in {":", "-"}:
            continue
        out.append((idx, text, is_list_item))

    return out


def protect(text: str) -> str:
    """Hide periods that do not end a sentence."""
    text = re.sub(r"\b(e\.g|i\.e|etc|vs|cf|approx|Inc|Ltd|Fig|Ref)\.", r"\1<DOT>", text)
    text = re.sub(r"(\d)\.(\d)", r"\1<DOT>\2", text)
    text = re.sub(r"\bv(\d)", r"v\1", text)
    return text


def restore(text: str) -> str:
    return text.replace("<DOT>", ".")


def split_sentences(text: str) -> list[str]:
    protected = protect(text)
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z\"'(\[])", protected)
    return [restore(p).strip() for p in parts if restore(p).strip()]


def word_count(sentence: str) -> int:
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9'`/._-]*", sentence)
    return len(tokens)


def find_noun_stack(sentence: str) -> str | None:
    """Return the first run of 4+ stacked nouns, which forces re-reading.

    Punctuation ends a run: nouns on opposite sides of a comma, colon, or
    bracket are not stacked, they are separate phrases.
    """
    run: list[str] = []
    for token in re.findall(r"[A-Za-z][A-Za-z-]*|[^A-Za-z\s]", sentence):
        if not token[0].isalpha():
            run = []
            continue
        lowered = token.lower()
        is_stackable = (
            lowered not in STACK_STOPWORDS
            and lowered not in STACK_VERBS
            and token != PLACEHOLDER
            and not lowered.endswith(("ing", "ly", "ed", "able", "ible", "ous", "ful"))
        )
        if is_stackable:
            run.append(token)
            if len(run) >= 4:
                return " ".join(run)
        else:
            run = []
    return None


def check_file(path: str) -> Report:
    report = Report(path=path)
    with open(path, encoding="utf-8") as handle:
        lines = handle.readlines()

    entries = prose_lines(lines)
    lowered_doc = " ".join(text.lower() for _, text, _ in entries)

    sentences: list[tuple[int, str]] = []
    for line_no, text, _ in entries:
        for sentence in split_sentences(text):
            sentences.append((line_no, sentence))

    over_limit = 0
    passive = 0
    gerunds = 0
    banned = 0
    semicolons = 0
    stacks = 0
    lengths: list[int] = []

    for line_no, sentence in sentences:
        words = word_count(sentence)
        lengths.append(words)
        excerpt = sentence if len(sentence) <= 120 else sentence[:117] + "..."

        if words > MAX_SENTENCE_WORDS:
            over_limit += 1
            report.errors.append(
                Finding(
                    line_no,
                    "sentence-length",
                    f"{words} words (limit {MAX_SENTENCE_WORDS}). Split it into "
                    "one sentence per idea.",
                    excerpt,
                )
            )

        if ";" in sentence:
            semicolons += 1
            report.errors.append(
                Finding(
                    line_no,
                    "semicolon",
                    "A semicolon joins two ideas. Write two sentences.",
                    excerpt,
                )
            )

        lowered = sentence.lower()
        for term, fix in BANNED_TERMS.items():
            if re.search(rf"(?<![\w-]){re.escape(term)}(?![\w-])", lowered):
                banned += 1
                report.errors.append(
                    Finding(
                        line_no,
                        "vague-wording",
                        f"'{term}' is vague or figurative. Instead: {fix}.",
                        excerpt,
                    )
                )

        first_word = re.match(r"([A-Za-z]+)", sentence)
        gerund_hits: list[str] = []
        if first_word and first_word.group(1).lower() in PARTICIPLE_CONNECTIVES:
            gerund_hits.append(first_word.group(1))
        for match in re.finditer(r",\s+(\w+ing)\b", sentence, re.IGNORECASE):
            if match.group(1).lower() in PARTICIPLE_CONNECTIVES:
                gerund_hits.append(match.group(1))
        for match in BY_GERUND_RE.finditer(sentence):
            gerund_hits.append(f"by {match.group(1)}")
        for match in PROGRESSIVE_RE.finditer(sentence):
            gerund_hits.append(match.group(0))
        if re.search(r"\b(while|when|after|before)\s+\w+ing\b", sentence, re.IGNORECASE):
            gerund_hits.append("subordinate -ing clause")

        if gerund_hits:
            gerunds += len(gerund_hits)
            report.errors.append(
                Finding(
                    line_no,
                    "participle",
                    f"Participle construction ({', '.join(sorted(set(gerund_hits)))}). "
                    "Use a finite verb in its own sentence.",
                    excerpt,
                )
            )

        passive_match = PASSIVE_RE.search(sentence)
        if passive_match and passive_match.group(1).lower() in ADJECTIVAL_PARTICIPLES:
            passive_match = None
        if passive_match:
            passive += 1
            report.warnings.append(
                Finding(
                    line_no,
                    "passive-voice",
                    "Passive voice. Name the actor unless it is genuinely unknown.",
                    excerpt,
                )
            )

        stack = find_noun_stack(sentence)
        if stack:
            stacks += 1
            report.warnings.append(
                Finding(
                    line_no,
                    "noun-stack",
                    f"Four or more stacked nouns ('{stack}'). Break them with a "
                    "preposition or a hyphen.",
                    excerpt,
                )
            )

    long_paragraphs = 0
    paragraph: list[str] = []
    paragraph_line = 0
    previous_line = -10
    for line_no, text, is_list_item in entries + [(-1, "", False)]:
        # A bullet starts its own unit, and so does any line that is not
        # directly continuing the line above it.
        if line_no != previous_line + 1 or line_no == -1 or is_list_item:
            if len(paragraph) > 0:
                count = sum(len(split_sentences(p)) for p in paragraph)
                if count > MAX_PARAGRAPH_SENTENCES:
                    long_paragraphs += 1
                    report.warnings.append(
                        Finding(
                            paragraph_line,
                            "paragraph-length",
                            f"{count} sentences in one paragraph (limit "
                            f"{MAX_PARAGRAPH_SENTENCES}). Split it or use a list.",
                            paragraph[0][:117],
                        )
                    )
            paragraph = []
            paragraph_line = line_no
        if line_no != -1:
            paragraph.append(text)
        previous_line = line_no

    inconsistencies = 0
    for canonical, variant in TERM_VARIANTS:
        if canonical in lowered_doc and variant in lowered_doc:
            inconsistencies += 1
            report.warnings.append(
                Finding(
                    0,
                    "term-consistency",
                    f"Both '{canonical}' and '{variant}' appear. Pick one and "
                    "use it everywhere.",
                    "",
                )
            )

    total = len(sentences) or 1
    report.metrics = {
        "prose_sentences": len(sentences),
        "max_sentence_words": max(lengths) if lengths else 0,
        "avg_sentence_words": round(sum(lengths) / total, 1),
        "sentences_over_limit": over_limit,
        "pct_sentences_over_limit": round(100 * over_limit / total, 1),
        "participle_constructions": gerunds,
        "vague_terms": banned,
        "semicolons": semicolons,
        "passive_sentences": passive,
        "pct_passive": round(100 * passive / total, 1),
        "noun_stacks": stacks,
        "long_paragraphs": long_paragraphs,
        "term_inconsistencies": inconsistencies,
        "errors": len(report.errors),
        "warnings": len(report.warnings),
    }
    return report


def print_report(report: Report) -> None:
    print(f"\n=== {report.path} ===")
    metrics = report.metrics
    print(
        f"prose sentences: {metrics['prose_sentences']} | "
        f"avg {metrics['avg_sentence_words']} words | "
        f"longest {metrics['max_sentence_words']} words"
    )
    print(f"errors: {len(report.errors)}  warnings: {len(report.warnings)}")

    for label, findings in (("ERROR", report.errors), ("WARN", report.warnings)):
        for finding in findings:
            print(f"  {label} {report.path}:{finding.line} [{finding.rule}] {finding.message}")
            if finding.excerpt:
                print(f"        > {finding.excerpt}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check release notes prose against STE rules.")
    parser.add_argument("files", nargs="+", help="Markdown files to check")
    parser.add_argument("--json", action="store_true", help="Print metrics as JSON")
    parser.add_argument(
        "--warnings-as-errors",
        action="store_true",
        help="Exit non-zero on warnings too",
    )
    args = parser.parse_args()

    reports = [check_file(path) for path in args.files]

    if args.json:
        print(
            json.dumps(
                [
                    {
                        "file": r.path,
                        "metrics": r.metrics,
                        "errors": [vars(f) for f in r.errors],
                        "warnings": [vars(f) for f in r.warnings],
                    }
                    for r in reports
                ],
                indent=2,
            )
        )
    else:
        for report in reports:
            print_report(report)

    failed = any(r.errors for r in reports)
    if args.warnings_as_errors:
        failed = failed or any(r.warnings for r in reports)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
