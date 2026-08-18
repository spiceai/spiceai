# FinanceBench PDF Search Benchmark

FinanceBench ([patronus-ai/financebench](https://github.com/patronus-ai/financebench)) is an
information-retrieval benchmark of 150 questions over SEC filings (10-K, 10-Q, 8-K). Each question
names the evidence pages that answer it (`evidence_doc_name` + `evidence_page_num`). These pages are
the ground-truth relevance labels (qrels).

This benchmark adds **PDF, page-level search quality** to `testoperator run search`. The corpus is one
PDF per filing page. Spice ingests each page-PDF as one corpus row and runs its full PDFium parse →
chunk → embed → search pipeline, while the page number stays the retrieval unit.

The spicepods here run as **custom** search tests (no `--benchmark-dataset`). See
[../README.md](../README.md) for the custom-run contract.

## How page identity is kept

Spice's document connector flattens a whole PDF into one `content` string per file. Page boundaries
are lost on ingestion. So each filing is split into one PDF per page **before** Spice sees it, and the
page number is encoded in the file path.

The object-store text connector reports each row's primary key as `location` — the full in-bucket key,
for example `financebench/corpus_pages/3M_2018_10K/p0000.pdf`. The qrels use the portable key
`3M_2018_10K/p0000.pdf`, which does not name any bucket or prefix.

To match the two, `corpus` is a **view** that strips the prefix from `location`:

```sql
SELECT regexp_replace(location, '^.*corpus_pages/', '') AS _id, content AS text FROM corpus_pdfs
```

The view's `_id` is the search primary key. It equals the qrel `corpus-id` for any bucket or prefix,
so the corpus data and the qrels stay portable.

## Variants

| File | Search |
|---|---|
| `full_text_search-cayenne[file].yaml` | BM25 full-text |
| `model2vec[potion-multilingual-128M]-cayenne[file].yaml` | Vector (chunked) |
| `hybrid[model2vec[potion-multilingual-128M]]-cayenne[file].yaml` | Hybrid (BM25 + vector, RRF) |

The vector and hybrid variants enable chunking, because a filing page often exceeds the embedding
token limit. Each chunk keeps its page's `_id`, so page-level relevance still scores.

## Run

The corpus, queries, and qrels must first be staged to the bench bucket (see below). Then run:

```bash
testoperator run search \
  --concurrency 10 \
  --ready-wait 3600 \
  --metrics \
  -p test/spicepods/search/financebench/full_text_search-cayenne[file].yaml
```

The spicepod reads `S3_ENDPOINT`, `S3_KEY`, and `S3_SECRET` from the environment. In CI, dispatch
`.github/workflows/testoperator_run_search.yml` with `benchmark_dataset: custom` and the spicepod
path; the workflow maps these from the bench MinIO secrets.

## One-time staging

Run this once to build and upload the corpus. It is not run in CI or by `testoperator`.

1. Build the page splitter:

   ```bash
   cargo build --release -p pdf-split
   ```

2. Stage the corpus and the two parquet files to the bench bucket. Pin `--source` to a commit so the
   corpus cannot change under a staged run:

   ```bash
   S3_ENDPOINT=<bench-minio-endpoint> S3_KEY=<key> S3_SECRET=<secret> \
   ./scripts/financebench_stage.py \
     --source https://raw.githubusercontent.com/patronus-ai/financebench/<commit-sha> \
     --dest s3://benchmarks/financebench/
   ```

   Add `--limit-docs 3` for a dry run over the first three documents.

The staging job splits every unique evidence document with the same `pdf-split` binary, validates that
every `evidence_page_num` is in range, and writes:

```
s3://benchmarks/financebench/
  corpus_pages/<doc>/pNNNN.pdf   # one PDF per page, zero-indexed
  queries.parquet                # _id, text
  relevance_data.parquet         # query-id, corpus-id, score
```

`corpus-id` is the portable `<doc>/pNNNN.pdf` key. A question with more than one evidence page emits
one relevance row per page.

## Known limitations

- A scanned or image-only filing page extracts to empty `content`, because PDFium OCR is off. Evidence
  on such a page cannot be retrieved. This is a recall ceiling of the data, not a defect.
- `corpus` is a view, so the run's OpenTelemetry attributes for engine and embedding model are empty
  (`search_dataset_attributes` reads them from a dataset named `corpus`). The retrieval metrics are
  not affected.
