## Design: make `refresh_mode: full` remove stale entries from external indexes

### Root cause

A `refresh_mode: full` refresh maps `RefreshMode::Full → UpdateType::Overwrite → InsertOp::Overwrite`, which is passed to `TableSink::insert_into` and on to the **accelerator storage** provider (`crates/runtime/src/accelerated_table/sink/table.rs`). `InsertOp::Overwrite` correctly truncates+replaces the accelerator's own storage.

External indexes (S3 Vectors, Elasticsearch) are *not* maintained through `insert_into`. They are maintained through the `Index` trait (`crates/runtime-datafusion-index/src/lib.rs`):

- `compute_index()` — runs per batch inside `IndexTableScanExec::execute` during `collect()`, writing rows to the external store as **upsert-by-primary-key**;
- `on_write_start()` / `on_write_complete()` / `on_write_failed()` — lifecycle hooks fired by the sink around the write.

**None of these hooks carry the `InsertOp`/overwrite signal.** So a full refresh into an external index behaves exactly like an append: rows whose primary keys are re-written are updated in place, but keys that disappeared from the source (`d`, `e` in the issue's example) are never deleted. Because writes are upsert-by-PK, there is no *duplication* bug — purely *stale survivors*.

The abstraction gap the issue names is precisely this: the write lifecycle cannot distinguish "append to the existing set" from "replace the set", so an external index has no signal on which to act.

### Approach

**1. Make the write mode explicit in the `Index` lifecycle (the core abstraction fix).**

Add to `runtime-datafusion-index`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexWriteMode { Append, Overwrite }
```

Change the three lifecycle hooks to take it:

```rust
async fn on_write_start(&self, mode: IndexWriteMode) -> Result<()>;
async fn on_write_failed(&self, mode: IndexWriteMode) -> Result<()>;
async fn on_write_complete(&self, mode: IndexWriteMode) -> Result<()>;
```

`compute_index()` keeps its signature — it stays a per-batch upsert and does not need to branch on the mode (see the generation mechanism below).

I will **remove the default impls** on the three hooks (per the trait-evolution guidance in `CLAUDE.md`), so the compiler forces every wrapper and impl to be revisited rather than silently inheriting a no-op. The sink derives the mode from the `InsertOp` it already has (`Overwrite → Overwrite`, else `Append`) and threads it through `TableSink`, `MultiSink`, and `finalize_indexes`.

**2. Give overwrite "replace the set" semantics via a per-write generation stamp + purge-on-complete.**

The mechanism, entirely within each external index (no query-path changes):

- `on_write_start(Overwrite)` bumps a per-index `AtomicU64` **write generation** and resets any per-run bookkeeping. `on_write_start` is guaranteed to run before `compute_index` (the sink fires it before `collect()`), so the generation is set before any row is written.
- `compute_index` stamps every written document/vector with the current generation (ES: a reserved `_spice.write_generation` field in `_source`; S3 Vectors: tracked via the in-memory written-key set — see below). Upsert-by-PK means a surviving key is re-stamped to the current generation.
- `on_write_complete(Overwrite)` deletes everything **not** carrying the current generation — those are exactly the stale survivors. `on_write_complete(Append)` and `on_write_failed(_)` do **not** purge, so an append never deletes and a failed refresh leaves the previous generation fully intact (failure-safe: the old data keeps serving, the partial new generation is reconciled on the next successful full refresh).

Per backend:

- **Elasticsearch** — add `delete_by_query` to the `Elasticsearch` client trait (`crates/elasticsearch/src/lib.rs`) + its real `Client` impl + the test mock. `on_write_complete(Overwrite)` issues `delete_by_query({ bool: { must_not: { term: { "_spice.write_generation": <gen> } } } })`. This runs **inside the existing `refresh_interval: -1` write window** (the current `ElasticsearchIndexWriteMaintenance` already suspends refresh during the write and calls `_refresh` at the end), so the new writes *and* the stale-purge become visible together at the final `_refresh` — no window in which a query can observe either a partial new set or the stale rows.
- **S3 Vectors** — no metadata-filtered delete exists, so use the primitives that do: `S3Vector` accumulates the written primary keys during `compute_index` (a `Mutex<HashSet<String>>` reset in `on_write_start(Overwrite)`); `on_write_complete(Overwrite)` calls `list_vectors` and issues `delete_vectors` for every key not in the written set. Partitioned indexes purge each resolved partition index. S3 Vectors has no refresh barrier, so there is a brief window in which stale keys may still be visible; this matches S3 Vectors' existing non-transactional write behavior and is documented.

**3. Non-external / wrapper impls.**

- Accelerator-embedded indexes (`DuckDBVectorIndex`, `MemoryVectorIndex`, `NativeVectorIndex`) need no purge — the accelerator's `InsertOp::Overwrite` already replaced their backing storage. They accept the mode and ignore it.
- Wrappers (`CompoundVectorIndex`, `CompoundSearchIndex`, `ChunkedSearchIndex`, `ChunkedVectorIndex`, `FullTextDatabaseIndex`) forward the mode to the index(es) they wrap — the compiler will flag each once the default impls are removed.
- The CDC path does not call these hooks (already the case) and is unaffected.

### Trade-offs / alternatives considered

- **Delete-all-before-write** (the issue's first "naive" option): simplest, but opens an *empty* window where the index returns nothing mid-refresh, and a mid-refresh failure destroys the old data with no replacement — a correctness regression. Rejected.
- **Soft-delete version filtered at query time** (the issue's second option): requires every external-index query path to filter on the version and a separate GC pass. Spreads refresh state into the read path and touches every query. Rejected as too invasive.
- **Atomic alias/index swap** (write a new generation index, flip an alias/pointer on commit): the zero-window ideal, but needs new per-backend primitives (ES write-alias management, S3 index-name indirection) *and* the read path to resolve through the pointer. Much larger; a good future upgrade. Noted, not chosen now.
- **Generation stamp + purge-on-complete** (chosen): failure-safe, no query-path changes, uses primitives each backend already exposes (or a single new ES `delete_by_query`), and for Elasticsearch is genuinely window-free thanks to the existing refresh barrier. The cost is one reserved internal field on ES docs and, for S3 Vectors only, a brief transient-superset window. Best balance for a single change.

### Files touched

- `crates/runtime-datafusion-index/src/lib.rs` — `IndexWriteMode`; hook signatures; drop default impls.
- `crates/runtime/src/accelerated_table/sink/{table.rs,multi.rs,mod.rs}` — derive mode from `InsertOp`; thread it through `on_write_start`/`on_write_failed`/`finalize_indexes`.
- `crates/runtime/src/accelerated_table/refresh_task.rs` — nothing beyond the existing `InsertOp` plumbing (mode is derived in the sink).
- `crates/search/src/index/{s3_vectors,elasticsearch}/mod.rs` (+ `elasticsearch/write.rs` for the generation field) — generation stamp + purge.
- `crates/search/src/index/{compound/vector_index.rs,compound/search_index.rs,chunking.rs,native_vector.rs,memory/mod.rs,duckdb/mod.rs}` and `crates/search/src/generation/text_search/index.rs` — accept/forward the mode.
- `crates/elasticsearch/src/lib.rs` — `delete_by_query` on the trait, real client, and mock.
- Test-only `Index` impls updated for the new signatures.

### Test plan

- Unit (runtime sink): a mock `Index` records the `IndexWriteMode` it received; assert `RefreshMode::Full` yields `Overwrite` and append yields `Append`, through both `TableSink` and `MultiSink`.
- Unit (Elasticsearch, mock client): after a two-generation overwrite where the source shrinks (`[a,b,c,d,e] → [a,b,c,f,g]`), assert `delete_by_query` was issued with a `must_not` on the previous generation, and that append mode issues no delete. Assert the purge runs before the final `_refresh` and after `refresh_interval` suspension.
- Unit (S3 Vectors, mock client): same shrinking-source scenario; assert `delete_vectors` is called for exactly `{d,e}` (listed − written) and never in append mode; assert a failed write purges nothing.
- Regression test tag `#12145` on the core sink test.
- Scoped `cargo check`/`clippy`/`nextest` on the touched crates with a fixed feature set, then `make lint-rust-fix` before pushing.

Feedback welcome on the chosen strategy — in particular whether the S3 Vectors transient-superset window is acceptable or whether that backend should also move to an atomic index swap in this change.
