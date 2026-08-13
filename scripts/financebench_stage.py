#!/usr/bin/env python3
#
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

"""One-time staging job that builds the FinanceBench PDF search benchmark corpus.

FinanceBench (patronus-ai/financebench) is 150 questions over SEC filings. Each
question's `evidence` entries name the exact filing page that answers it, so the
ground truth is at *page* granularity. Spice's document/`file:` connector
flattens a whole PDF into one `content` string per file, destroying page
boundaries on ingestion, so page identity must exist *before* Spice sees the
data: every filing is pre-split into one PDF per page, with the zero-indexed
page number in the file path (which becomes the corpus row's `location`).

This job runs once, offline (never in CI or by testoperator). It:

  1. Reads `financebench_open_source.jsonl` and the referenced `pdfs/<doc>.pdf`.
  2. Splits every unique evidence doc into `corpus_pages/<doc>/pNNNN.pdf` with
     the committed `pdf-split` binary (identical page extraction to the tool).
  3. Validates every `evidence_page_num < page_count` for its doc — a Qrel past
     end-of-doc means the corpus is wrong, so it fails loudly (data correctness).
  4. Builds `queries.parquet` (`_id`, `text`) and `relevance_data.parquet`
     (`query-id`, `corpus-id`, `score`) matching the harness contract (#12935),
     expanding multi-page questions to one relevance row per evidence page.
  5. Writes `corpus_pages/`, `queries.parquet`, `relevance_data.parquet` to a
     `--dest` that is either a local directory or an `s3://` URI.

The `corpus-id` in `relevance_data.parquet` MUST equal the `location` string
Spice reports for the matching page-PDF. A single normalization rule
(`corpus_id_for`) is shared by the parquet builder and the upload layout so the
two never drift; set `--corpus-id-prefix` to whatever the `location` probe
reports (see the README / issue #12858 Verification step 2).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

DEFAULT_SOURCE = "https://raw.githubusercontent.com/patronus-ai/financebench/main"
JSONL_RELATIVE = "data/financebench_open_source.jsonl"


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr, flush=True)


def page_file_name(idx: int) -> str:
    """Zero-indexed, zero-padded page file name, matching `pdf_split::page_file_name`."""
    return f"p{idx:04d}.pdf"


def corpus_id_for(prefix: str, doc_name: str, page_idx: int) -> str:
    """The single source of truth for the `location`/`corpus-id` string of a page.

    Both the on-disk / S3 layout (`<dest>/corpus_pages/<doc>/pNNNN.pdf`) and the
    `corpus-id` column derive from this, so a Qrel always names the same string
    Spice reports as `location`. `prefix` adapts the relative key to whatever the
    connector reports for a given `--dest` (see the module docstring).
    """
    key = f"{doc_name}/{page_file_name(page_idx)}"
    return f"{prefix}{key}" if prefix else key


# ---------------------------------------------------------------------------
# Destination writers — dispatch on the `--dest` scheme so the same layout and
# `corpus-id`/`location` normalization apply to a local dir and an s3:// URI.
# ---------------------------------------------------------------------------


class Dest:
    """Write files to a staging destination under a stable relative layout."""

    def put_file(self, rel_path: str, local_path: Path) -> None:
        raise NotImplementedError

    def describe(self) -> str:
        raise NotImplementedError


@dataclass
class LocalDest(Dest):
    root: Path

    def put_file(self, rel_path: str, local_path: Path) -> None:
        target = self.root / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target)

    def describe(self) -> str:
        return str(self.root)


@dataclass
class S3Dest(Dest):
    bucket: str
    prefix: str
    client: object

    def put_file(self, rel_path: str, local_path: Path) -> None:
        key = f"{self.prefix}{rel_path}" if self.prefix else rel_path
        self.client.upload_file(str(local_path), self.bucket, key)

    def describe(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"


def make_dest(dest: str) -> Dest:
    """Build a `Dest` for a local directory path or an `s3://bucket/prefix` URI."""
    parsed = urlparse(dest)
    if parsed.scheme in ("", "file"):
        root = Path(parsed.path if parsed.scheme == "file" else dest)
        root.mkdir(parents=True, exist_ok=True)
        return LocalDest(root=root)

    if parsed.scheme == "s3":
        try:
            import boto3  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - env dependent
            raise SystemExit(
                "boto3 is required to upload to s3://. Install it (pip install boto3) "
                "or write to a local --dest directory instead."
            ) from exc

        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        # Reuse the same secret pattern the S3 spicepods use: S3_ENDPOINT /
        # S3_KEY / S3_SECRET (MinIO bench bucket).
        endpoint = os.environ.get("S3_ENDPOINT")
        client = boto3.client(
            "s3",
            endpoint_url=f"https://{endpoint}" if endpoint and "://" not in endpoint else endpoint,
            aws_access_key_id=os.environ.get("S3_KEY"),
            aws_secret_access_key=os.environ.get("S3_SECRET"),
        )
        return S3Dest(bucket=bucket, prefix=prefix, client=client)

    raise SystemExit(f"Unsupported --dest scheme: {dest!r} (use a local path or s3://…)")


# ---------------------------------------------------------------------------
# Source access — read the FinanceBench jsonl and PDFs from a local clone or
# over HTTPS from raw.githubusercontent.
# ---------------------------------------------------------------------------


def read_source_bytes(source: str, rel_path: str) -> bytes:
    """Read `rel_path` from a local FinanceBench checkout or an HTTP(S) base URL."""
    parsed = urlparse(source)
    if parsed.scheme in ("http", "https"):
        url = f"{source.rstrip('/')}/{rel_path}"
        try:
            with urllib.request.urlopen(url) as resp:  # noqa: S310 - fixed https host
                return resp.read()
        except urllib.error.HTTPError as exc:
            raise SystemExit(f"Failed to download {url}: HTTP {exc.code}") from exc

    local = Path(source) / rel_path
    if not local.is_file():
        raise SystemExit(f"Source file not found: {local}")
    return local.read_bytes()


def load_records(source: str) -> list[dict]:
    raw = read_source_bytes(source, JSONL_RELATIVE)
    records = []
    for line in raw.decode("utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def evidence_pages(record: dict) -> list[tuple[str, int]]:
    """Return `(doc_name, page_idx)` pairs from a record's evidence entries."""
    pairs: list[tuple[str, int]] = []
    for ev in record.get("evidence") or []:
        doc_name = ev.get("evidence_doc_name") or ev.get("doc_name") or record.get("doc_name")
        page = ev.get("evidence_page_num")
        if doc_name is None or page is None:
            continue
        pairs.append((str(doc_name), int(page)))
    return pairs


# ---------------------------------------------------------------------------
# Staging
# ---------------------------------------------------------------------------


def split_doc(
    pdf_split_bin: Path,
    source: str,
    work_dir: Path,
    doc_name: str,
) -> list[Path]:
    """Fetch `pdfs/<doc>.pdf` into `work_dir` and split it into single-page PDFs.

    Returns the produced page paths in page order.
    """
    pdf_bytes = read_source_bytes(source, f"pdfs/{doc_name}.pdf")
    src_pdf = work_dir / f"{doc_name}.pdf"
    src_pdf.parent.mkdir(parents=True, exist_ok=True)
    src_pdf.write_bytes(pdf_bytes)

    out_dir = work_dir / "corpus_pages" / doc_name
    subprocess.run(
        [str(pdf_split_bin), str(src_pdf), "--out", str(out_dir)],
        check=True,
        capture_output=True,
    )
    return sorted(out_dir.glob("p*.pdf"))


def write_parquet(rows: list[dict], schema, path: Path) -> None:
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    columns = {field.name: [row[field.name] for row in rows] for field in schema}
    table = pa.table(columns, schema=schema)
    pq.write_table(table, path)


def stage(args: argparse.Namespace) -> int:
    try:
        import pyarrow as pa  # noqa: PLC0415
    except ImportError as exc:
        raise SystemExit("pyarrow is required (pip install pyarrow).") from exc

    pdf_split_bin = Path(args.pdf_split_bin)
    if not pdf_split_bin.is_file():
        raise SystemExit(
            f"pdf-split binary not found at {pdf_split_bin}. Build it with "
            "`cargo build --release -p pdf-split` and pass --pdf-split-bin, or set it explicitly."
        )

    records = load_records(args.source)
    if args.limit_docs:
        # Deterministically keep the first N unique docs for dry-runs.
        wanted: set[str] = set()
        kept = []
        for record in records:
            docs = {doc for doc, _ in evidence_pages(record)}
            if wanted or docs:
                wanted |= docs
            kept.append(record)
            if len(wanted) >= args.limit_docs:
                break
        keep_docs = set(list(wanted)[: args.limit_docs])
        records = [
            r for r in kept if any(doc in keep_docs for doc, _ in evidence_pages(r))
        ]
        eprint(f"Limiting to {len(keep_docs)} docs ({len(records)} questions) for dry-run.")

    # Every unique evidence doc must be split and validated.
    doc_names = sorted({doc for r in records for doc, _ in evidence_pages(r)})
    eprint(f"{len(records)} questions reference {len(doc_names)} unique evidence docs.")

    dest = make_dest(args.dest)
    eprint(f"Staging to {dest.describe()}")

    with tempfile.TemporaryDirectory(prefix="financebench-stage-") as tmp:
        work_dir = Path(tmp)
        page_counts: dict[str, int] = {}

        for i, doc_name in enumerate(doc_names, start=1):
            eprint(f"[{i}/{len(doc_names)}] splitting {doc_name}")
            pages = split_doc(pdf_split_bin, args.source, work_dir, doc_name)
            if not pages:
                raise SystemExit(f"Splitting produced no pages for {doc_name}.")
            page_counts[doc_name] = len(pages)
            for page_idx, page_path in enumerate(pages):
                # The uploaded relative path IS the corpus-id key (minus prefix),
                # so the layout and the Qrel `corpus-id` derive from one rule.
                dest.put_file(f"corpus_pages/{doc_name}/{page_file_name(page_idx)}", page_path)

        # Validate every evidence page is in range BEFORE emitting Qrels.
        errors: list[str] = []
        for record in records:
            fb_id = record.get("financebench_id")
            for doc_name, page in evidence_pages(record):
                count = page_counts.get(doc_name)
                if count is None:
                    errors.append(f"{fb_id}: evidence doc {doc_name!r} was not staged")
                elif not 0 <= page < count:
                    errors.append(
                        f"{fb_id}: evidence_page_num {page} out of range for {doc_name} "
                        f"(0..{count})"
                    )
        if errors:
            eprint("Qrel validation FAILED — the corpus is inconsistent with the evidence:")
            for err in errors:
                eprint(f"  {err}")
            return 1

        # queries.parquet: _id, text.
        query_rows = [
            {"_id": str(r["financebench_id"]), "text": str(r["question"])}
            for r in records
            if r.get("financebench_id") is not None and r.get("question") is not None
        ]
        query_schema = pa.schema([("_id", pa.string()), ("text", pa.string())])

        # relevance_data.parquet: query-id, corpus-id, score. One row per evidence
        # page; dedup identical (query, page) pairs a multi-evidence question may repeat.
        seen: set[tuple[str, str]] = set()
        rel_rows = []
        for record in records:
            fb_id = str(record.get("financebench_id"))
            for doc_name, page in evidence_pages(record):
                corpus_id = corpus_id_for(args.corpus_id_prefix, doc_name, page)
                key = (fb_id, corpus_id)
                if key in seen:
                    continue
                seen.add(key)
                rel_rows.append({"query-id": fb_id, "corpus-id": corpus_id, "score": 1})
        rel_schema = pa.schema(
            [("query-id", pa.string()), ("corpus-id", pa.string()), ("score", pa.int64())]
        )

        queries_path = work_dir / "queries.parquet"
        relevance_path = work_dir / "relevance_data.parquet"
        write_parquet(query_rows, query_schema, queries_path)
        write_parquet(rel_rows, rel_schema, relevance_path)
        dest.put_file("queries.parquet", queries_path)
        dest.put_file("relevance_data.parquet", relevance_path)

    eprint(
        f"Done. {len(query_rows)} queries, {len(rel_rows)} relevance rows, "
        f"{sum(page_counts.values())} corpus pages across {len(doc_names)} docs."
    )
    return 0


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--dest",
        required=True,
        help="Output destination: a local directory or an s3://bucket/prefix URI.",
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=(
            "FinanceBench source: a local checkout path or an HTTP(S) base URL "
            f"(default: {DEFAULT_SOURCE})."
        ),
    )
    parser.add_argument(
        "--pdf-split-bin",
        default="target/release/pdf-split",
        help="Path to the built pdf-split binary (default: target/release/pdf-split).",
    )
    parser.add_argument(
        "--corpus-id-prefix",
        default="",
        help=(
            "Prefix prepended to the '<doc>/pNNNN.pdf' relative key to form the "
            "corpus-id, so it matches the connector-reported `location`. Set from the "
            "location probe (issue #12858 Verification step 2). Default: empty."
        ),
    )
    parser.add_argument(
        "--limit-docs",
        type=int,
        default=0,
        help="For dry-runs: stage only the first N unique evidence docs (0 = all).",
    )
    return parser.parse_args(list(argv))


def main(argv: Iterable[str]) -> int:
    return stage(parse_args(argv))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
