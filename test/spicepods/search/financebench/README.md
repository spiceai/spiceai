# FinanceBench PDF Search Benchmark

## How page identity is kept

Spice's document connector flattens a whole PDF into one `content` string per file. Page boundaries
are lost on ingestion. Chunking does not preserve page boundaries either. Page boundaries are required because the benchmark answers are a tuple `(filename, page_num)`. To enforce this, we split into one PDF per page **before** Spice sees it, and the
page number is encoded in the file path. 

## One-time staging
To setup the per-page PDFs, we require one time setup. 

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
