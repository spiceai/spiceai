# Vector Search Test Spicepods

## Naming

Test spicepod names should be formatted according to the following template:

```console
{embedding-model-provider[variant]}-{accelerator/indexer[variant]}-{test variant}
```

`[variant]` refers to the specific information about the embedding model or indexer setup. For example:

* `model2vec[potion-multilingual-128M]` - a Model2Vec `potion-multilingual-128M` embedding model.
* `duckdb[file]` - a DuckDB accelerator using file-mode acceleration

Variants can be nested, up to 2 levels. For example, `model2vec[potion-multilingual-128M[chunking]]` is an embedding model with enabled chunking configuration.

`{test variant}` refers to additional configuration information, for example, `hybrid` indicating that full text search is enabled

Do not include test dataset information in the `{test variant}`. This information is supplied as a query metric dimension/attribute.

Examples of full spicepod names:

* `huggingface[all-minilm-l6-v2]-arrow` - a HuggingFace `all-MiniLM-L6-v2` embedding model with Arrow acceleration.
* `model2vec[potion-multilingual-128M[chunking]]-duckdb[file]` - a Model2Vec `potion-multilingual-128M` embedding model with chunking enabled, using DuckDB file-mode acceleration.
* `huggingface[all-minilm-l6-v2]-arrow-hybrid_limit_2000` - a HuggingFace `all-MiniLM-L6-v2` embedding model with Arrow acceleration and hybrid search (vector + full-text search) enabled, with a test corpus data limit of 2000 records.

## Datasets

Each search spicepod loads its own data. `testoperator run search` does not download anything: it
starts `spiced` with the spicepod, then queries three fixed tables (see
[Required tables](#required-tables)).

```console
testoperator run search --spicepod-path test/spicepods/search/mteb/fiqa/full_text_search-cayenne[file].yaml
```

| Directory | Source | Primary key |
|---|---|---|
| `mteb/<dataset>/` | MTEB retrieval datasets on Hugging Face (`mteb/*`) | `_id` |
| `conteb/<dataset>/` | ConTEB datasets on Hugging Face (`illuin-conteb/*`) | `chunk_id` |
| `financebench/` | FinanceBench PDFs staged to the benchmark bucket (see [its README](./financebench/README.md)) | `_id` |
| `perf/<dataset>/` | Latency benchmarks (see [Latency benchmarks](#latency-benchmarks)) | `_id` |

### Dataset label

The spicepod names its dataset in the top-level `metadata`. The test sends this value as the
`benchmark_dataset` metric attribute, so runs of different spicepod configurations over the same
data share one series:

```yaml
metadata:
  benchmark_dataset: fiqa_retrieval
```

If the spicepod does not set `metadata.benchmark_dataset`, the attribute is `custom`. Do not change
the label of an existing dataset, because dashboards use it to group runs.

### Reading parquet files from Hugging Face

Use the `https` connector with `params: { file_format: parquet }`. Use one of these URL forms:

* Parquet that Hugging Face converts from the source files (the `refs/convert/parquet` branch):
  `https://huggingface.co/api/datasets/<org>/<dataset>/parquet/<config>/<split>/<n>.parquet`.
  `<n>` is the shard number without zero padding (`0`, not `0000`). To list the shards of a
  dataset, open `https://huggingface.co/api/datasets/<org>/<dataset>/parquet`.
* Parquet files on a branch without a `/` in its name, such as `main`:
  `https://huggingface.co/datasets/<org>/<dataset>/resolve/main/<path>`.

Do not use a `resolve/refs%2Fconvert%2Fparquet/...` URL. `spiced` decodes `%2F` to `/`, and the
request fails.

The `https` connector cannot list a directory on Hugging Face, so one dataset reads one file. When a
corpus has more than one shard (for example `mteb/touche2020` and `mteb/msmarco`), load each shard
as its own dataset (`corpus_0`, `corpus_1`, ...) and make `corpus` an accelerated view that joins
them with `UNION ALL`. Put the acceleration and the search columns on the view, so that one refresh
of the view reads every shard and builds the search indexes.

### ConTEB

[ConTEB](https://huggingface.co/blog/manu/conteb) has a `documents` config (`chunk_id`, `chunk`)
and a `queries` config (`query`, `chunk_id`) for each dataset. The spicepods search the `chunk`
column, and views map the queries to `test_queries` and `relevance_data`. Each query row is one
query with one gold chunk, so the query id is `md5(query || '|' || chunk_id)`. `esg-reports` is
different: each query lists all of its gold chunks in `chunk_ids`, so its query id is `md5(query)`.
The views use `SELECT DISTINCT`, so a row that occurs two times in the source is one query.

The split of each dataset is the split that ConTEB evaluates, from the `DATASETS` table in
[`scripts/evaluation/eval_conteb.py`](https://github.com/illuin-tech/conteb/blob/main/scripts/evaluation/eval_conteb.py)
(the default split is `train`):

| Directory | Hugging Face dataset | Split | Chunks | Queries |
|---|---|---|---|---|
| `covid_qa` | `illuin-conteb/covid-qa` | `train` | 3,351 | 1,106 |
| `esg_reports` | `illuin-conteb/esg-reports` | `test` | 3,702 | 36 |
| `football` | `illuin-conteb/football` | `train` | 6,259 | 2,682 |
| `geography` | `illuin-conteb/geography` | `train` | 2,291 | 5,283 |
| `insurance` | `illuin-conteb/insurance` | `train` | 60 | 120 |
| `mldr` | `illuin-conteb/mldr-conteb-eval` | `test` | 1,536 | 100 |
| `narrative_qa` | `illuin-conteb/narrative-qa` | `test` | 1,750 | 8,073 |
| `squad` | `illuin-conteb/squad-conteb-eval` | `validation` | 17,562 | 2,067 |

`esg_reports` (36 queries), `mldr` (100 queries), and `insurance` (60 chunks) are too small for a
stable nDCG@10. Use them to find large changes only.

## Required tables

Every search spicepod must define `corpus` and `test_queries`. It can also define
`relevance_data`. Without `relevance_data`, the run measures latency only and does not report
retrieval quality. Each table may be a dataset or a view.

| Name | Kind | Required columns | Notes |
|---|---|---|---|
| `corpus` | dataset or view | the column(s) set up for `embeddings:` and/or `full_text_search:` | Must declare a one-column `row_id:` (a source-system primary key is not discovered automatically). The test reads the corpus id of each search result from this column, and matches it to `"corpus-id"`. |
| `test_queries` | dataset or view | `_id` (query id), `text` (query text) | Read as `SELECT * FROM test_queries`. An optional `query_type` column groups the queries for per-type latency percentiles. |
| `relevance_data` (optional) | dataset or view | `"query-id"`, `"corpus-id"`, `score` | Read as `SELECT "query-id", "corpus-id", CAST(score AS BIGINT) AS score FROM relevance_data`. `score` must be a whole-number grade (0, 1, 2, ...); NDCG uses it as a weight. |

Id columns may be `Utf8`, `LargeUtf8`, or an integer type. Use `views:` to map real column names
onto these names. See [`custom/example.yaml`](./custom/example.yaml) for a template.

## Latency benchmarks

`testoperator run search` measures the latency of each search request. These flags make the
measurement a benchmark:

| Flag | Default | Effect |
|---|---|---|
| `--warmup-passes <n>` | `0` | Send all test queries `n` times before measurement starts. |
| `--passes <n>` | `1` | Send all test queries `n` times and measure each request. Retrieval quality uses the first measured pass. |
| `--limit <n>` | `10` | The maximum number of results of each request. |
| `--concurrency <n>` | `1` | The number of clients. Each client sends one request, waits for the response, and then sends the next request. |

The run prints the P50, P95, P99, and P99.9 latency and the sample count, for all queries and for
each `query_type`. It does not report a percentile that has fewer than 10 samples beyond it. For
example, P99.9 needs at least 10,000 samples. The run records the same values as the
`search_latency_ms` (with `percentile` and `query_type` attributes) and `search_latency_samples`
metrics.

When the run sends each query more than one time (`--warmup-passes` + `--passes` > 1), the
spicepod must turn off both results caches. Otherwise the run fails, because a repeated query
would come from the cache:

```yaml
runtime:
  caching:
    search_results:
      enabled: false
    sql_results:
      enabled: false
```

Compare two runs only when they use the same runner type, spicepod, `--concurrency`, `--limit`,
and pass counts.

| Directory | Data | Queries | `relevance_data` |
|---|---|---|---|
| `perf/msmarco/` | MS MARCO passages (8.8M) from `mteb/msmarco` | The 6,980 dev queries that have judgments | MS MARCO dev qrels |
| `perf/search_benchmark_game/` | English Wikipedia (about 5M articles) from [`spiceai/search-benchmark-game`](https://huggingface.co/datasets/spiceai/search-benchmark-game) | 962 keyword queries from [search-benchmark-game](https://github.com/quickwit-oss/search-benchmark-game), with `query_type` (`union`, `intersection`, `phrase`, ...) | None (latency only) |

For example, to get P99.9 for each query type of search-benchmark-game (about 300 queries for
each type, so 34 or more passes):

```console
testoperator run search \
  -p "test/spicepods/search/perf/search_benchmark_game/full_text_search-cayenne[file].yaml" \
  --warmup-passes 2 --passes 35 --concurrency 1
```

The search-benchmark-game queries use the Tantivy query syntax: `+term` is a required term, and
`"a b"` is a phrase. Spice indexes text with an English stemmer, and the benchmark does not.
Thus, Spice results are not the same as the published search-benchmark-game results.
