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

Every search spicepod must define three tables with a fixed schema. Each table may be a dataset or
a view.

| Name | Kind | Required columns | Notes |
|---|---|---|---|
| `corpus` | dataset or view | the column(s) set up for `embeddings:` and/or `full_text_search:` | Must declare a one-column `row_id:` (a source-system primary key is not discovered automatically). The test reads the corpus id of each search result from this column, and matches it to `"corpus-id"`. |
| `test_queries` | dataset or view | `_id` (query id), `text` (query text) | Read as `SELECT _id as id, text FROM test_queries`. |
| `relevance_data` | dataset or view | `"query-id"`, `"corpus-id"`, `score` | Read as `SELECT "query-id", "corpus-id", CAST(score AS BIGINT) AS score FROM relevance_data`. `score` must be a whole-number grade (0, 1, 2, ...); NDCG uses it as a weight. |

Id columns may be `Utf8`, `LargeUtf8`, or an integer type. Use `views:` to map real column names
onto these names. See [`custom/example.yaml`](./custom/example.yaml) for a template.
