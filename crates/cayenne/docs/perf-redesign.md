# Cayenne Performance Redesign

Status: Draft for review. No code changes have been made — this document is the structural plan
that the first implementation PR will execute against.

## 1. Goal

Step-function — not incremental — improvements to Cayenne ingestion and query performance under
the existing DataFusion + Vortex + SQLite/Turso stack. Three structural changes are proposed.
They share a common direction (move work off the hot path, remove lock chains, prune earlier)
and reinforce one another, so they are described together.

Non-goals for this redesign:

- Replacing the metastore backend (SQLite/Turso stays).
- Replacing Vortex as the on-disk format.
- Changing the DuckLake-style snapshot/sequence model exposed through `MetadataCatalog`.
- Schema/PK evolution.

## 2. What's structurally limiting today

References anchor every claim to current code so reviewers can verify before approving.

### 2.1 Lock chains on the read path

`PkDeletionStrategyWithCache` (`src/provider/deletion_strategy.rs:50–73`) holds every deletion
and insert cache as `Arc<RwLock<Arc<HashMap<…>>>>`. Each scan acquires a read guard, clones the
inner `Arc`, and drops the guard. Refresh (`copy_cache`, `deletion_strategy.rs:222–240`) takes a
write guard and swaps the inner `Arc`. The outer `RwLock` therefore serialises readers against
any concurrent refresh and adds a guard acquisition to every scan.

A SIMD/bloom-filter alternative already exists in the same crate but is unused by the production
filter execs: `DeletionIndex` (`src/provider/deletion_index.rs:39–260`) uses
`parking_lot::RwLock` plus a bloom filter (`hash_index::BloomFilter`). Tests exercise it
(`deletion_index.rs:410–516`) but `Int64PkDeletionFilterExec` and `KeyBasedDeletionFilterExec`
(`src/provider/delete/filter_exec.rs:137–200`) still consume `Arc<HashMap<i64, i64>>` /
`Arc<HashMap<Box<[u8]>, i64>>` directly.

### 2.2 Per-row probes on the read path

`is_pk_visible_i64` (`src/provider/delete/filter_exec.rs:65–83`) is called once per row inside
the filter exec. It does a `HashMap::get` against `deleted_pks`, then on a hit does a second
`HashMap::get` against `insert_records`. For Int64 PKs this is the entire deletion check; for
composite PKs the same two-probe pattern runs after a `RowConverter::convert_columns` call per
batch (`is_pk_visible_row_key`, `filter_exec.rs:89–103`). There is no bloom-prefilter, no
SIMD probe, and nothing batched: each row scalar-probes two hash maps.

Position-based deletion already does the right thing — bitmaps are pushed into Vortex via
`Selection::ExcludeRoaring` at the scan layer (see header comment at `filter_exec.rs:28–32`).
The PK paths never got the same treatment.

### 2.3 Merge-on-write upsert with `RowConverter` on the write path

`CayenneDataSink::write_all_append` (`src/provider/sink.rs:141–250+`) calls
`prepare_stream_for_insert`, which validates on-conflict constraints — for non-Int64 PKs this
runs `RowConverter` over the full incoming stream to produce byte keys, then probes the
deletion/insert caches. This happens before any data is written, on the same task that is
draining the input stream. For wide composite keys the conversion dominates the write path.

Insert records are persisted one-PK-per-row in `cayenne_insert_record` (`sqlite.rs:233–242`)
with a `UNIQUE(table_id, pk_bytes)` constraint, so each upserted PK is at minimum a separate
SQL statement (or large parameterised batch) inside the per-table write lock.

### 2.4 No per-file pruning beyond Vortex footers

`cayenne_table_statistics` (`sqlite.rs:267–274`) holds one stats blob per table on a
last-write-wins basis (the comment in the DDL acknowledges this is "an optimization hint until
cross-write merging lands"). There is no per-file or per-block min/max/null-count table, so any
non-PK predicate fans out to all files in the snapshot — the planner has nothing to prune
against. Vortex footer-level stats can prune columns within a file once it is opened, but the
planner can't avoid opening the file in the first place.

### 2.5 Per-table write lock + sequential staging move

`write_all` (`sink.rs:141–157`) acquires a single per-table mutex for both append and overwrite.
Inside, the staging-to-snapshot move is a per-file rename loop guarded by a JSON WAL
(`provider/staging_wal.rs`, called from `sink.rs:141` flow). For many small files this is N
syscalls plus a fsync, all under the table lock. There is already a "data inlining fast-path"
for small writes (`sqlite.rs:276+`, `cayenne_inlined_data`); it is the right shape but only
covers the smallest end of the distribution.

## 3. The redesign — three bets

Bets are independent enough to land sequentially but reinforce one another. They share three
themes: **make caches lock-free for readers**, **push pruning earlier**, and **move
correctness work out of the hot path**.

### Bet A — Lock-free deletion caches and PK Selection pushdown

**Problem:** §2.1 + §2.2.

**Change:**

1. Replace the `Arc<RwLock<Arc<HashMap<…>>>>` fields in `PkDeletionStrategyWithCache` with a
   single `arc-swap`-style cell (`ArcSwap<DeletionIndex>` / `ArcSwap<KeyDeletionIndex>`).
   Readers do a wait-free load; refresh publishes a fully-built new index. The existing
   `DeletionIndex` / `KeyDeletionIndex` types already encapsulate the bloom filter and are a
   drop-in fit; their internal `parking_lot::RwLock` becomes immutable post-publish (a
   builder-then-frozen pattern).
2. Remove `is_pk_visible_*` row-by-row probing from the filter execs. Replace it with two
   batch operations:
   - **Build a per-batch tombstone bitmap** by vectorised probing of the deletion index over
     the PK column(s). For Int64 PK, this is a tight `i64` loop with bloom prefilter; for
     composite PK, `RowConverter::convert_columns` is called once per batch and the resulting
     row bytes are probed in bulk against `KeyDeletionIndex`.
   - **Apply the bitmap via `arrow::compute::filter_record_batch`** in one shot. This is what
     position-based deletion already does at the Vortex layer; PK paths now do the same at
     the Arrow batch layer.
3. Where the underlying file is Vortex and we have a stable mapping from PK to file, bypass
   the filter exec entirely and push the deletion bitmap into `Selection::ExcludeRoaring`,
   matching the position-based path. (The mapping comes "for free" from Bet C; until C lands
   we keep the filter-exec path but with the vectorised probe.)

**Expected impact:** read-path lock contention removed; per-row probe replaced by per-batch
SIMD-friendly probe with bloom prefilter; the existing `DeletionIndex` is finally on the hot
path.

**Blast radius:** Internal to `provider/delete/` and `provider/deletion_strategy.rs`. No
metastore migration. No on-disk format change. Public catalog API unchanged.

### Bet B — Per-file zone maps in the metastore

**Problem:** §2.4.

**Change:**

1. Add a new metastore table (SQLite + Turso):

   ```sql
   CREATE TABLE cayenne_file_stats (
       table_id        TEXT NOT NULL,
       snapshot_id     TEXT NOT NULL,
       file_path       TEXT NOT NULL,
       row_count       BIGINT NOT NULL,
       column_stats    BLOB   NOT NULL,  -- columnar min/max/null_count, packed
       PRIMARY KEY (table_id, snapshot_id, file_path),
       FOREIGN KEY (table_id) REFERENCES cayenne_table(table_id) ON DELETE CASCADE
   );
   CREATE INDEX idx_cayenne_file_stats_table_snapshot
       ON cayenne_file_stats(table_id, snapshot_id);
   ```

   Encoding for `column_stats`: a single Arrow IPC RecordBatch with one row per column
   (`column_name`, `min`, `max`, `null_count`, `distinct_estimate`). Reuses the
   `ColumnStatsAccumulator` already populated during write
   (`provider/sink.rs` write pipeline).

2. Wire `CayenneTableProvider::scan` to call a new
   `MetadataCatalog::list_files_with_stats(table_id, snapshot_id)` that returns
   `(file_path, ColumnStats)` pairs. Translate predicates to a min/max prune in
   `provider/optimizer_rules.rs` (existing module) and drop pruned files from the
   `ListingTable` config before constructing the scan.

3. Keep `cayenne_table_statistics` for table-level summary (it powers DataFusion table-level
   `Statistics`); the new table is the per-file truth.

**Expected impact:** non-PK range/equality predicates prune files at planning time, before any
file is opened. Largest wins on selective filters and on tables that have grown to many files
within a snapshot.

**Blast radius:** Metastore schema migration (additive, no destructive change). Both
SQLite and Turso backends touched. Read path adds a single catalog query per scan; we already
do one to list files.

### Bet C — Merge-on-read upsert with sorted runs and compaction

**Problem:** §2.3 + §2.5 + tail of §2.1.

This is the largest bet and the one the other two de-risk. It moves Cayenne's upsert handling
from "merge-on-write" (validate the incoming stream against deletion/insert caches before any
file is written) to "merge-on-read" (write straight through, resolve duplicates at scan time).

**Change:**

1. **Drop `RowConverter` from the write path.** `prepare_stream_for_insert` becomes a
   pass-through for the on-conflict case; new data goes to a fresh file with the next sequence
   number, full stop.

2. **Sort each written file by PK** when a PK is configured. `VortexConfig.sort_columns`
   already exists; we make PK-sort the default for PK tables. This gives every file a
   `(min_pk, max_pk)` and a sorted layout, which Bet B's zone maps already record.

3. **Resolve PK duplicates at scan time** with a sequence-aware merge:
   - Files with disjoint `(min_pk, max_pk)` ranges — the common case under
     monotonically-growing keys — need no merge work; the planner sees disjoint ranges from
     Bet B and skips merge entirely.
   - Files with overlapping ranges go through a sort-merge node that keeps the row with the
     highest sequence number per PK. This is built on DataFusion's `SortPreservingMergeExec`
     plus a small "last-writer-wins by sequence" aggregate; both PK columns and the sequence
     are already present on disk.
   - Tombstones from the existing deletion vectors continue to apply via Bet A's pushdown.

4. **Background compaction** merges overlapping runs into non-overlapping ones, dropping
   superseded versions and tombstoned rows. Compaction is per-table, runs under a separate
   lock (not the write lock), and produces a single new snapshot; the existing snapshot
   protection mechanism keeps in-flight reads consistent.

5. **Drop or shrink `cayenne_insert_record`.** With sequence-on-disk and merge-on-read, the
   insert-records cache is no longer the source of truth for upsert visibility — the file
   metadata is. The table can be retained for backwards compatibility and gradually deprecated,
   or migrated away in a follow-up.

**Expected impact:** the write path no longer pays `RowConverter` cost or per-PK SQL inserts;
write throughput becomes bound by Vortex encode + I/O. Read path pays a merge cost only on
overlapping ranges, which compaction keeps small.

**Blast radius:** Largest of the three. Affects sink, filter execs, scan planning, and adds a
new compaction loop. Bet A and Bet B should land first because they're prerequisites for the
read-path pieces (vectorised pushdown for tombstones; per-file ranges for prune-then-merge).

## 4. Sequencing

```
Bet A  ──►  Bet B  ──►  Bet C
(read locks    (per-file       (merge-on-read +
 + pushdown)    pruning)        compaction)
```

Each bet is independently shippable and independently measurable. Bet C depends on B's per-file
ranges and A's vectorised tombstone application; landing them in order means Bet C is mostly
new write/compaction code rather than a giant cross-cutting rewrite.

## 5. First focused PR

Per the direction chosen for this redesign: **Bet A, in one PR**, scoped as follows.

In scope:

- Replace the `Arc<RwLock<Arc<HashMap<…>>>>` fields in
  `src/provider/deletion_strategy.rs` with `ArcSwap` of the existing `DeletionIndex` /
  `KeyDeletionIndex` types from `src/provider/deletion_index.rs`.
- Update `Int64PkDeletionFilterExec` and `KeyBasedDeletionFilterExec`
  (`src/provider/delete/filter_exec.rs`) to consume the swap cell directly and apply the
  vectorised batch-probe + `arrow::compute::filter_record_batch` path described in Bet A.
- Update `refresh_from` semantics to publish a freshly-built index via the swap cell instead
  of write-locking the existing one.
- Update call sites in `src/provider/delete/sink.rs`,
  `src/provider/delete/sink/position_based.rs`, and `src/provider/table.rs` for the new
  cache type. The `position_based` strategy is unchanged on the hot path (it already pushes
  down to Vortex); only its cache wrapper changes type.
- Keep the existing `Arc<HashMap<…>>` types as a `From`/`Into` boundary so the change is a
  pure refactor of internal storage, not a public API change.

Out of scope for this PR (each is its own PR):

- Bet B's metastore migration and planner-side pruning.
- Bet C's merge-on-read pipeline and compaction.
- Removing the per-table write lock.
- Any change to the inlined-data fast-path (`cayenne_inlined_data`).

Acceptance for this PR:

- Existing test suite in `crates/cayenne/tests/` passes unchanged.
- Both new benches (§6) demonstrate measurable improvement on the targeted workloads with no
  regression on the others.

## 6. Validation plan

Both micro-benches and an end-to-end harness, as agreed.

### 6.1 Criterion micro-benches (extend existing)

In `crates/cayenne/benches/deletion_strategies.rs`:

- `query_with_concurrent_refresh` — N reader tasks + 1 refresh task. Measures effect of
  removing reader/refresher contention. Pre-PR: contention with `RwLock`; post-PR: wait-free.
- `vectorised_probe` — single-batch probe across deletion ratios `{0%, 0.1%, 1%, 10%, 50%}`
  and PK widths `{1×i64, 4×col composite, 8×col composite}`. Measures bloom prefilter and
  batched probe vs. the per-row baseline.
- `refresh_publish_latency` — end-to-end "rebuild and publish" of a deletion cache at sizes
  `{10k, 100k, 1M}`. Used to confirm refresh stays in-budget under the new build-then-swap
  pattern.

In `crates/cayenne/benches/metastore_operations.rs` (relevant for Bets B and C, lighter
expansion for Bet A):

- `list_files_with_stats` — added when Bet B lands, but the bench skeleton goes in now so the
  Bet B PR is just a body change.

### 6.2 End-to-end harness (new bench)

Add `crates/cayenne/benches/end_to_end.rs` driving real `CayenneTableProvider` writes and
scans. One bench module per bet so we can A/B against the same workload.

Workloads:

- **`ingest_throughput`** — append a stream of `N` rows in batches of `B`, measure
  rows/sec and end-to-end wall time. Variants: PK off / Int64 PK / 4-col composite PK; sizes
  `{100k, 1M, 10M}` rows; batch sizes `{1k, 10k, 100k}`.
- **`upsert_throughput`** — same shape as ingest, but 50% of inserts collide with existing
  PKs. Most directly exposes the `RowConverter` write-path cost (Bet C target).
- **`scan_with_deletions`** — populate `N` rows, delete `D` of them, then scan with
  predicates `{full scan, point lookup by PK, range filter on non-PK column}`. Variants:
  `D/N ∈ {0.001, 0.01, 0.1, 0.5}`. Targets Bets A and B.
- **`concurrent_read_under_write`** — a writer task appending in a loop while readers run
  scans. Reports reader p50/p95 latency and writer throughput. Targets Bet A.

The harness reuses the existing in-process metastore (SQLite file in tmpdir) so it measures
realistic catalog interaction, not just in-memory paths.

### 6.3 Reporting

Each bench prints (a) absolute numbers and (b) ratio vs. a baseline tag committed alongside
the PR (`cayenne-perf-baseline-2026-05`). The PR description includes a before/after table for
the relevant subset; the rest stays in CI artifacts.

## 7. Risks and open questions

1. **`arc-swap` dependency** — pulling it in is a single small dep. Acceptable, but worth
   confirming there is no policy reason to avoid it. Alternative: a hand-rolled
   `AtomicPtr<Arc<T>>` cell with `triomphe` or `arcsync`, but `arc-swap` is the standard.
2. **`DeletionIndex` builder freeze** — today `DeletionIndex` is interior-mutable. To make it
   safe behind `ArcSwap` (so readers can rely on the snapshot being immutable for the
   lifetime of an `Arc::clone`) we either (a) gate `insert*` to a builder phase via a typestate
   `DeletionIndexBuilder → DeletionIndex`, or (b) document that all mutation goes through
   "build new, swap" and remove the public `insert*` methods on the published index. Option
   (a) is preferred — type-system guarantee beats convention.
3. **`is_pk_visible_*` callers outside `filter_exec`** — need to grep before deletion. If any
   slow-path code depends on per-row visibility checks, it gets converted to the batch helper
   or kept as a debug-only utility.
4. **Sequence ordering on the merge-on-read path (Bet C)** — sequence numbers today live in
   the catalog (`cayenne_snapshot_sequence`) and per insert record. Bet C requires sequence on
   disk per row or per file. Per-file is enough if files are written atomically per
   `(snapshot, sequence)`; per-row would mean a sidecar column. To resolve when Bet C is
   designed in detail.
5. **Compaction concurrency model** — Bet C's compaction must not deadlock with the per-table
   write lock. The plan is a separate compaction lock that snapshots the file set, writes new
   files to a fresh snapshot, then atomically swaps via the existing snapshot mechanism. Needs
   spelling out fully when Bet C is detailed.
6. **Turso parity** — Bet B's new table must be created by the Turso backend too. The Turso
   path in `src/metastore/turso.rs` is feature-gated; we keep parity but will need to
   re-verify locally.

## 8. Out of scope for this redesign

- Replacing SQLite/Turso with an LSM key-value store at the metastore layer.
- Distributed/HA metastore.
- Cross-table query optimisations.
- Changes to retention or partitioning semantics — those modules are touched only where their
  call surface intersects the read/write paths above.

## 9. Open decisions for the reviewer

1. Confirm `arc-swap` is acceptable as a new dependency.
2. Confirm Bet A first-PR scope is acceptable, or specify a smaller slice (e.g. Int64 PK only,
   composite PK in a follow-up).
3. Confirm baseline tag naming and bench artifact location.
4. Confirm Bet B's metastore migration policy — additive-only, or are we comfortable doing an
   online schema upgrade for existing deployments?
5. Confirm Bet C's eventual deprecation of `cayenne_insert_record` is on the table, or whether
   it must stay as the source of truth.
