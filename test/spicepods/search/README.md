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

## Custom datasets

`testoperator run search` also runs against your own search-configured spicepod. Omit
`--benchmark-dataset` and the tool skips all MTEB data preparation, testing the spicepod at
`--spicepod-path` as-is:

```console
testoperator run search --spicepod-path ./my-spicepod.yaml
```

A custom spicepod must define three tables with a fixed schema:

Each table may be a dataset or a view.

| Name | Kind | Required columns | Notes |
|---|---|---|---|
| `corpus` | dataset or view | the column(s) set up for `embeddings:` and/or `full_text_search:` | Must declare `row_id:` (a source-system primary key is not discovered automatically). |
| `test_queries` | dataset or view | `_id` (query id), `text` (query text) | Read as `SELECT _id as id, text FROM test_queries`. |
| `relevance_data` | dataset or view | `"query-id"`, `"corpus-id"`, `score` | Read as `SELECT "query-id", "corpus-id", CAST(score AS BIGINT) AS score FROM relevance_data`. `score` must be a whole-number grade (0, 1, 2, ...); NDCG uses it as a weight. |

Id columns may be `Utf8`, `LargeUtf8`, or an integer type. Use `views:` to map real column names
onto these names. See [`custom/example.yaml`](./custom/example.yaml) for a template.
