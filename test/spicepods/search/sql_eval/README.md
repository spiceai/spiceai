# Search Evaluation in SQL

These spicepods do what `testoperator run search` does, but in SQL. `spiced` runs a search for each
test query, then calculates nDCG, recall, MRR and precision at k = 1..10 in views. You do not need
`testoperator`.

`testoperator` does not use these files. Do not put them in `mteb/`, `conteb/` or `financebench/`:
the spicepod validator in `testoperator_dispatch_search.yml` validates every file in those
directories as a standalone spicepod, and the spicepods in `spicepods/` are not standalone.

## Layout

```
sql_eval/
├── scifact-full_text_search.yaml           <- one root spicepod for each benchmark and search
└── spicepods/                              <- dependencies (resolved from the root's directory)
    ├── search/offline-evals/spicepod.yaml  <- the metric views, for every benchmark
    └── mteb/scifact/spicepod.yaml          <- the benchmark data: corpus, test_queries, relevance_data
```

| Spicepod | Contains |
|---|---|
| Root | `runtime.functions.enabled: true`, `dependencies`, `metadata.benchmark_dataset`, and the search to evaluate: the table function `search_pipeline(query)` |
| `search/offline-evals` | The views `relevance_judgments`, `search_results`, `query_metrics` and `retrieval_metrics` |
| `mteb/<dataset>` | The datasets `corpus`, `test_queries` and `relevance_data`, with the [required schema](../README.md#required-tables) |

`spiced` loads a dependency from `<directory of the root spicepod>/spicepods/<dependency>/spicepod.yaml`.
It does not load the dependencies of a dependency, and it ignores the `runtime` section of a
dependency. Thus each root spicepod is in this directory, lists all of its dependencies, and sets
`runtime.functions.enabled: true`.

## Run

```console
spiced test/spicepods/search/sql_eval/scifact-full_text_search.yaml
```

```sql
SELECT * FROM retrieval_metrics ORDER BY k;   -- mean metrics at k = 1..10
SELECT * FROM query_metrics WHERE k = 10;     -- one row for each judged query
```

`search_results` is a view, not an accelerated table. Each query of the metric views runs the
searches again.

## Add a search or a benchmark

- **A different search on the same benchmark:** copy the root spicepod and change only the body of
  `search_pipeline`. It must return `_id` and `_score` for the top 10 rows of `corpus`, best first.
  A search that needs embeddings on `corpus` needs a benchmark spicepod that configures them.
- **A different benchmark:** add `spicepods/mteb/<dataset>/spicepod.yaml` with the three datasets,
  and a root spicepod that depends on it.
