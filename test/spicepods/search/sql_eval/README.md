# Search Evaluation in SQL

These spicepods calculate the retrieval metrics of `testoperator run search` in SQL. `spiced` runs
a search for each test query, then calculates nDCG, recall, MRR and precision at k = 1..10 in views.

## Layout

```
test/spicepods/search/
├── mteb/scifact/full_text_search-cayenne[file].yaml  <- existing benchmark spicepod: corpus, test_queries, relevance_data
└── sql_eval/
    ├── spicepod.yaml                    <- the metric views, for every benchmark
    └── scifact-full_text_search.yaml    <- root: the search to evaluate, and its dependencies
```

| Spicepod | Contains |
|---|---|
| Root (`sql_eval/<benchmark>-<search>.yaml`) | `runtime.functions.enabled: true`, `metadata.benchmark_dataset`, `dependencies`, and the search to evaluate: the table function `search_pipeline(query)` |
| `sql_eval/spicepod.yaml` | The views `relevance_judgments`, `search_results`, `query_metrics` and `retrieval_metrics` |
| A benchmark spicepod in `mteb/` | The tables `corpus`, `test_queries` and `relevance_data`, with the [required schema](../README.md#required-tables) |

The root spicepod lists its dependencies relative to `test/spicepods/search/`:

```yaml
dependencies:
  - sql_eval
  - mteb/scifact/full_text_search-cayenne[file].yaml
```

A dependency is a directory that contains `spicepod.yaml`, or a spicepod file. The runtime ignores
the `runtime` section of a dependency, and it does not load the dependencies of a dependency.
Thus each root spicepod sets `runtime.functions.enabled: true` and lists all of its dependencies.

## Run

Give `test/spicepods/search` to `testoperator` as the dependency directory:

```console
testoperator run search \
  --spicepod-path test/spicepods/search/sql_eval/scifact-full_text_search.yaml \
  --spicepod-dependencies test/spicepods/search \
  --sql-pipeline "SELECT _id, _score FROM search_pipeline(:query)"
```

`testoperator` loads the dependencies, merges them with the root spicepod, and starts `spiced` with
the result. It sends each test query to `search_pipeline` and calculates the metrics in Rust. The
`retrieval_metrics` view calculates the same metrics in SQL.

`spiced` alone cannot load these dependencies, because it looks for them in the `spicepods/`
directory next to the root spicepod. To run the merged spicepod in `spiced` without a test,
export it, then start `spiced` in the directory that the command prints:

```console
testoperator export search \
  --spicepod-path test/spicepods/search/sql_eval/scifact-full_text_search.yaml \
  --spicepod-dependencies test/spicepods/search
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
  A search that needs embeddings on `corpus` depends on a benchmark spicepod that configures them,
  for example `mteb/scifact/model2vec[potion-multilingual-128M]-cayenne[file].yaml`.
- **A different benchmark:** add a root spicepod that depends on `sql_eval` and on a spicepod of
  that benchmark in `mteb/`, `conteb/` or `financebench/`.

## Notes

- The root `.gitignore` ignores every `spicepod.yaml` file, but an ignore rule does not apply to a
  file that git already tracks. Changes to `sql_eval/spicepod.yaml` are tracked as usual.
- `testoperator` does not run these files unless you give the root spicepod with
  `--spicepod-path`. Do not put a root spicepod in `mteb/`, `conteb/` or `financebench/`: the
  spicepod validator in `testoperator_dispatch_search.yml` checks every file there as a standalone
  spicepod.
