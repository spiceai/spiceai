# Distributed Full-Text Search

This document describes how full-text search works over a multi-node accelerated
table in Spice. It matches the code on this branch. Read it with the source files
it names.

## 1. The problem

A full-text search index is a local `Tantivy` index. When a table is accelerated
across many nodes, each executor holds only its own partition. Each executor
builds its own `Tantivy` index over its own documents.

`Tantivy` scores a query with BM25. BM25 uses collection statistics:

- `N` — the number of documents in the collection.
- `df(term)` — the number of documents that contain the term.
- the total number of tokens in the search field (BM25 divides this by `N` to get
  the average document length).

Each executor knows only its own local statistics. The local statistics differ
between executors. A BM25 score from one executor is therefore not comparable
with a score from another executor. A naive top-N merge across executors is only
approximate.

## 2. The approach

`N`, `df(term)`, and the total token count are additive over disjoint partitions.
The global value of each statistic is the sum of the per-partition values. Sum the
per-partition statistics to get the global statistics. Then score every executor
with the same global statistics. The scores are then comparable, and the merge is
exact.

`GlobalBm25Stats` (in `crates/search/src/generation/text_search/bm25_stats.rs`)
carries the statistics. `GlobalBm25Stats::add` sums two partitions. The same type
holds one partition's local statistics and the summed global statistics, because
the statistics are additive.

## 3. The plan shape

The `DistributedSearchRewrite` analyzer rule rewrites the `text_search` scan into
this logical plan:

```text
Projection: <original scan columns>
  Extension: DistributedSearch{ table, query, column, fetch, executors }
    Aggregate: group_by=[term], aggr=[SUM(doc_freq), SUM(total_num_docs), SUM(total_num_tokens)]
      Union[ TableScan(text_search_stats(...) @ executor-0), ..., @ executor-N ]
```

The `Union` collects one statistics leg per executor. The `Aggregate` sums the
legs, grouped by term, into the global statistics. The `DistributedSearch`
extension node drains that child, then scores each executor with the global
statistics and merges the results. The outer `Projection` returns the scan's
advertised columns.

## 4. Components

### The statistics UDTF

File: `crates/runtime-search/src/full_text_stats_udtf.rs`.

`text_search_stats(tbl, query, column?)` gathers one executor's local BM25
statistics. It resolves the table's `FullTextDatabaseIndex`, selects the index for
the requested column (or the sole index), and calls `local_bm25_stats` on the
field index. It returns one row per analyzed query term:

- `term` (Utf8) — the tokenized and stemmed query term.
- `doc_freq` (UInt64) — the term's local document frequency.
- `total_num_docs` (UInt64) — the partition's document count `N`.
- `total_num_tokens` (UInt64) — the partition's total token count.

The collection totals repeat on every row, so a downstream `SUM ... GROUP BY term`
sums each of `N`, the token count, and per-term `df` across partitions.
`GlobalBm25Stats::stats_schema` and `to_record_batch` define the output.

### `local_bm25_stats`

File: `crates/search/src/generation/text_search/mod.rs`.

`FullTextSearchFieldIndex::local_bm25_stats` parses the query, collects the terms,
and keeps only terms on the search field. A term on another field is scored
locally and is not summed. For each search-field term, it reads the local document
frequency. It reads `total_num_docs` and `total_num_tokens` from the local reader.
It returns a `GlobalBm25Stats`.

### The `global_stats` argument and the `GlobalBm25Provider` injection

Files: `crates/runtime-search/src/full_text_udtf.rs`,
`crates/runtime-search/src/udtf.rs`,
`crates/search/src/generation/text_search/mod.rs`.

`text_search` takes a runtime-only named argument `global_stats`. The constant is
`TEXT_SEARCH_GLOBAL_STATS_ARG` (`"global_stats"`). The scheduler sets it on each
executor's query. Users do not pass it.

`TextSearchTableFunc::parse_args` reads `global_stats` as an encoded string.
`call` decodes it with `GlobalBm25Stats::decode` and attaches it with
`with_global_stats`. When the argument is absent, scoring stays local.

`search_query_literal` branches on the attached statistics. When present, it wraps
the statistics in a `GlobalBm25Provider` and calls
`Searcher::search_with_statistics_provider`. `GlobalBm25Provider` implements the
`Tantivy` `Bm25StatisticsProvider` trait. It returns the global `total_num_docs`,
the global `total_num_tokens` for the search field, and the global `doc_freq` for a
search-field term. It delegates any other field or non-text term to the local
searcher. The search still runs over the local segments; only the collection
statistics change.

### The analyzer rule

File: `crates/runtime/src/cluster/datafusion/distributed_search_rewrite.rs`.

`DistributedSearchRewrite` runs on the scheduler only, because it needs the
executor registry. It transforms each `TableScan` down the plan. It rewrites the
scan only when the scan is a `SearchQueryProvider` with a `TextSearch` UDTF source
over an accelerated table. Otherwise it leaves the scan unchanged.

The rule resolves the covering executor set with `resolve_search_executors`. When
no live executor covers the table, it returns an empty relation rather than score
against an empty index. It builds the statistics plan (`build_stats_plan`): one
`text_search_stats(...)` Flight SQL leg per executor, joined by `Union`, then a
`SUM ... GROUP BY term` aggregate. It builds a `DistributedSearchNode` with the
statistics plan, the merge schema, the search parameters, and the executor list.
It wraps the node in a `Projection` back to the scan's advertised columns. The
rewrite jumps recursion so federation handles the injected subtree afterward.

### The operator

File: `crates/runtime/src/cluster/datafusion/distributed_search.rs`.

The physical operator implements the execution strategy produced by the analyzer rule.

`DistributedSearchNode` is the logical extension node. Its parameters
(`DistributedSearchParams`) are the source table SQL, the query, the column, the
primary key, the fetch limit, and a skip. Its executors are `DistributedExecutor`
values, each an id and a Flight SQL client.

`DistributedSearchExec` is the physical operator. It first drains the statistics
child (the aggregate). It reconstructs the global statistics with
`GlobalBm25Stats::from_aggregated_batches`, then encodes them with
`GlobalBm25Stats::encode`. It runs `text_search(..., global_stats => '<encoded>')`
on each executor over Flight SQL. Each executor scores its own partition with the
global statistics. The operator merges the comparable results and applies the
fetch limit.

### The Flight SQL UDTF-in-FROM leg

File: `crates/runtime/src/cluster/datafusion/distributed_search_rewrite.rs`
(`stats_leg`, `stats_from_function`).

Each statistics leg is a `FlightSQLTable` whose FROM source is a
`text_search_stats(<table>, '<query>'[, "<column>"])` UDTF call. The scheduler
sends this SQL to the executor over the intra-cluster Flight SQL channel. The
executor runs the UDTF against its local index and returns its local statistics.
`stats_from_function` renders the SQL. It quotes the query as a string literal and
the column as an identifier, and it doubles embedded quotes.

### The proto and codec field

File: `crates/runtime-proto/proto/spice.proto` (`TextSearchArgs.global_stats`).

`TextSearchArgs` has an optional `global_stats` field (field 6). It carries the
encoded `GlobalBm25Stats` JSON that a distributed search scores against. When
absent, the executor scores with local statistics. The Spice logical codec
(`crates/runtime/src/cluster/datafusion/codec/spice_logical_codec.rs`) carries the
field through serialization, so the argument survives distribution to the executor.

## 5. Correctness notes

- **Disjoint covering executor set.** `resolve_search_executors` (in
  `crates/runtime-cluster/src/executor_registry.rs`) returns a disjoint covering
  set. `covering_executor_ids` selects the minimal set of executors that together
  cover every partition. A replica is never counted twice. A double count would
  return duplicate rows and skew the ranking and the summed statistics. This is the
  same minimal-cover selection a partitioned table scan uses.
- **The collection size matches what `Tantivy` scores against.** `total_num_docs`
  is the sum of each segment's `max_doc`. This includes documents that a later
  update superseded, until a merge removes them. The gathered `N` is therefore the
  same collection size `Tantivy` scores against locally.
- **Analyzer parity across executors.** Every executor shares the index schema and
  the tokenizer. The analyzed term text is the same on every executor. The
  `doc_freq` key of a query term therefore matches the term `Tantivy` scores
  against. The `BTreeMap` key type keeps the encoding stable.
- **`_score` is kept for the merge, then projected away.** The merge schema always
  includes the `_score` column, so the operator can rank by score. When the caller
  passed `include_score => false`, the outer `Projection` drops `_score` again.
- **Empty and reordered batches are safe.**
  `GlobalBm25Stats::from_aggregated_batches` takes the maximum of the repeated
  collection totals, so a reordered or partial batch cannot lower them. The
  `saturating_add` in `add` cannot overflow.

## 6. Not yet implemented / follow-ups

- **`scoring = local` opt-in mode.** An option to skip the statistics gather and
  merge local scores approximately, for lower latency.
- **Statistics caching.** `N` and the average document length do not depend on the
  query. Cache them so only per-term `df` is gathered per query.
- **`vector_search` fan-out.** Extend the same distributed fan-out and merge to
  vector search.
