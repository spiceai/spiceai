# Cayenne — Metadata &amp; Data Storage Reference

What Cayenne persists, and where. Cayenne splits storage across **two stores plus a level-0 tier**:

| Store | Holds | Durability |
|-------|-------|------------|
| **Metastore** (SQLite / Turso) | *all* metadata — table rows, snapshot pointers, file manifests, delete-file refs, statistics, sequence numbers, the inline memtable | transactional (`BEGIN…COMMIT`) |
| **Data lake** (local FS / S3 Express One Zone) | immutable **Vortex** data files + Arrow-IPC deletion files, grouped under per-snapshot directories | atomic file rename + snapshot-pointer flip |
| **LSM level-0** | small writes absorbed as Arrow-IPC blobs in the metastore (`cayenne_inlined_*`) and, under the CDC profile, an in-RAM mem-tier | metastore tier is durable; RAM tier is discarded on crash and re-streamed exactly-once |

> **Source of truth:** the DDL in `crates/cayenne/src/metastore/sqlite.rs` (mirrored verbatim in `turso.rs`) and the column lists in `metastore::EXPECTED_TABLES`. `validate_existing_schema` checks **column names and ordering** at startup (types are not compared — SQLite/libSQL type affinity makes exact matching unreliable). This document reflects the 11 tables in `EXPECTED_TABLES`.

---

## 1. Metastore schema (11 tables)

`table_id` is a **UUIDv7 string** and the spine of every dependent table via `FOREIGN KEY … ON DELETE CASCADE`. Tables are grouped below by role.

### Table definition &amp; sequencing

#### `cayenne_table` — the table row

One row per table; the authoritative pointer to current state.

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | TEXT | **PK**, UUIDv7 — stable across catalog dumps and snapshots |
| `table_name` | TEXT NOT NULL | unique index `idx_cayenne_table_name_unique` |
| `path` | TEXT NOT NULL | data root (local or `s3://…--x-s3/…`) |
| `path_is_relative` | BOOLEAN NOT NULL | re-anchored on snapshot import for portability |
| `schema_json` | TEXT NOT NULL | Arrow schema |
| `primary_key_json` | TEXT | PK column list (nullable → no PK) |
| `on_conflict_json` | TEXT | on-conflict / upsert policy |
| `current_snapshot_id` | TEXT NOT NULL DEFAULT `''` | **the visibility pointer** — flipped atomically by every write/compaction |
| `partition_column` | TEXT | composite-partition column spec (nullable) |
| `vortex_config_json` | TEXT | per-dataset Vortex tuning (`VortexConfig`) |
| `current_sequence_number` | BIGINT NOT NULL DEFAULT 0 | high-water sequence allocator |

#### `cayenne_snapshot_sequence` — per-snapshot sequence

Drives Iceberg-style visibility ordering and protected-snapshot filtering: a deletion applies only to snapshots whose `sequence_number ≤` the delete file's.

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | TEXT NOT NULL | **PK** part, FK → `cayenne_table` |
| `snapshot_id` | TEXT NOT NULL | **PK** part |
| `sequence_number` | BIGINT NOT NULL | the snapshot's sequence |

### Data-file manifest &amp; statistics

#### `cayenne_snapshot_file` — authoritative per-snapshot file manifest

The **complete** data-file set for a snapshot (manifest snapshot model). One row per file; a new snapshot can reference an existing file by inserting a row pointing at the same path (no copy). `min_sequence`/`max_sequence` let compaction bake a seq-prefix: a file is bake-eligible when `min_sequence ≤ T` (a file straddling the cutoff, `min_sequence ≤ T < max_sequence`, still holds rows ≤ T and must be baked).

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | TEXT NOT NULL | **PK** part, FK → `cayenne_table` |
| `snapshot_id` | TEXT NOT NULL | **PK** part |
| `file_path` | TEXT NOT NULL | **PK** part |
| `row_count` | BIGINT NOT NULL DEFAULT 0 | |
| `file_size_bytes` | BIGINT NOT NULL DEFAULT 0 | |
| `min_sequence` | BIGINT NOT NULL DEFAULT 0 | commit-seq range — drives seq-prefix bake |
| `max_sequence` | BIGINT NOT NULL DEFAULT 0 | |

#### `cayenne_snapshot_file_statistics` — per-file pruning cache

Best-effort footer statistics so listing-time pruning skips re-reading every object on each scan.

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | TEXT NOT NULL | **PK** part, FK → `cayenne_table` |
| `snapshot_id` | TEXT NOT NULL | **PK** part |
| `file_path` | TEXT NOT NULL | **PK** part |
| `file_size_bytes` | BIGINT NOT NULL | |
| `num_rows` | BIGINT NOT NULL DEFAULT 0 | |
| `statistics_blob` | BLOB NOT NULL | Vortex `FileStatistics` flatbuffer (min/max/null count) |

#### `cayenne_table_statistics` — per-table aggregate

One row per table, upserted on every write and merged into the running aggregate. Treat as optimization hints.

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | TEXT NOT NULL | **PK**, FK → `cayenne_table` |
| `statistics_blob` | BLOB NOT NULL | Vortex `FileStatistics` flatbuffer (min/max/null count) |
| `num_rows` | BIGINT NOT NULL DEFAULT 0 | live row count |
| `ndv_sketches` | BLOB | per-integer-column HyperLogLog NDV sketches (mergeable register-wise max) |

### Deletes &amp; upsert tracking

#### `cayenne_delete_file` — deletion-vector references

Decouples deletes from data (deletes never rewrite data files). The `deletion_type` (position- vs key-based) is **inferred from the file schema at read time**, not persisted.

| Column | Type | Notes |
|--------|------|-------|
| `delete_file_id` | TEXT | **PK**, UUIDv7 |
| `table_id` | TEXT NOT NULL | FK → `cayenne_table` |
| `path` | TEXT NOT NULL | unique index on `(table_id, path)` |
| `path_is_relative` | BOOLEAN NOT NULL | |
| `format` | TEXT NOT NULL | always `'arrow_ipc'` |
| `delete_count` | BIGINT NOT NULL | |
| `file_size_bytes` | BIGINT NOT NULL | |
| `source_data_file_path` | TEXT | non-NULL **only** for position-based deletes |
| `sequence_number` | BIGINT NOT NULL DEFAULT 0 | delete sequence |
| `reinsert_sequence` | BIGINT | per-commit reinsert sequence for metadata-only publish (added last, matches ALTER backfill order) |

#### `cayenne_insert_record` — upsert re-insertion tracking

One row per deleted-then-reinserted PK of a CDC burst (20K–55K rows on hot upsert tables) — a high-volume, hot table.

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | BLOB NOT NULL | **PK** part — stored as the **16 raw UUID bytes**, not the 36-char text, to cut WAL write volume (`table_id_to_key_bytes`) |
| `pk_bytes` | BLOB NOT NULL | **PK** part — PK row key (Arrow `RowConverter` bytes) |
| `sequence_number` | BIGINT NOT NULL | re-insertion sequence |

Declared `WITHOUT ROWID` on SQLite (clustered on `(table_id, pk_bytes)`); a plain rowid table on Turso (no `WITHOUT ROWID` under its MVCC journal mode). The legacy `insert_record_id` UUID column was dropped — it was never read.

#### `cayenne_pk_index` — persisted PK existence checkpoint

Lets restart / snapshot-bootstrap skip the full-table keyset rebuild.

| Column | Type | Notes |
|--------|------|-------|
| `table_id` | TEXT NOT NULL | **PK**, FK → `cayenne_table` |
| `snapshot_id` | TEXT NOT NULL | snapshot the checkpoint covers |
| `index_blob` | BLOB NOT NULL | serialized PK existence bloom checkpoint |

### Partitions

#### `cayenne_partition` — composite partition metadata

| Column | Type | Notes |
|--------|------|-------|
| `partition_id` | TEXT | **PK** |
| `table_id` | TEXT NOT NULL | FK → `cayenne_table` |
| `partition_columns_json` | TEXT NOT NULL | ordered column names |
| `partition_values_json` | TEXT NOT NULL | ordered values |
| `partition_key` | TEXT NOT NULL | slash-separated composite key (Hive-style dir naming); `UNIQUE(table_id, partition_key)` |
| `path` | TEXT NOT NULL | |
| `path_is_relative` | BOOLEAN NOT NULL | |
| `record_count` | BIGINT NOT NULL DEFAULT 0 | |
| `file_size_bytes` | BIGINT NOT NULL DEFAULT 0 | |

### LSM level-0 (inline) tier

#### `cayenne_inlined_data` — inline memtable

Small insert batches stored as Arrow-IPC blobs directly in the metastore, avoiding a Vortex file per small write. Flushed (`CHECKPOINT`) to consolidated Vortex files on memtable pressure.

| Column | Type | Notes |
|--------|------|-------|
| `inlined_id` | TEXT | **PK** |
| `table_id` | TEXT NOT NULL | FK → `cayenne_table` |
| `partition_key` | TEXT | routes inline data per partition (nullable) |
| `data_ipc` | BLOB NOT NULL | Arrow IPC stream |
| `record_count` | BIGINT NOT NULL | |
| `sequence_number` | BIGINT NOT NULL | index `(table_id, sequence_number)` |
| `created_at` | TEXT NOT NULL | default `strftime('%Y-%m-%dT%H:%M:%fZ','now')` |

#### `cayenne_inlined_delete` — inline tombstones

Inline tombstones for upserted/deleted PKs not yet checkpointed to a delete-vector file.

| Column | Type | Notes |
|--------|------|-------|
| `inlined_id` | TEXT | **PK** |
| `table_id` | TEXT NOT NULL | FK → `cayenne_table` |
| `delete_ipc` | BLOB NOT NULL | Arrow IPC stream of PK row keys / row IDs |
| `delete_count` | BIGINT NOT NULL | |
| `sequence_number` | BIGINT NOT NULL | index `(table_id, sequence_number)` |
| `created_at` | TEXT NOT NULL | default timestamp |
| `published` | INTEGER | `NOT NULL DEFAULT 0`. Per-tombstone durable activation flag. A staged inline-conflict upsert writes `published = 0`; the read filter applies the tombstone **only** when `1`, so a tombstone seen by an inline-cache rebuild before its snapshot publishes can't transiently hide the old row. The owning snapshot's finalize flips it to `1`. Must stay last (ALTER backfill order). |

---

## 2. On-disk data layout

Vortex data files and Arrow-IPC deletion files live under the data root (local FS or S3 Express One Zone), grouped by `table_id` then snapshot directory:

```
<data_root>/
└─ <table_id>/
   ├─ <current_snapshot_id>/
   │   ├─ part-001.vortex            ◀ the one authoritative, VISIBLE base state
   │   ├─ part-002.vortex
   │   └─ deletions/
   │       └─ <delete_file_id>.arrow ◀ Arrow-IPC deletion vector
   ├─ <staging_snapshot_id>/         ◀ Stage-A buffer — durable, NOT yet visible
   │   ├─ _wal.json                  ◀ staging WAL marker (tmp+fsync+rename)
   │   └─ part-*.vortex                (no deletions/ until published)
   └─ <protected_snapshot_id>/       ◀ published & VISIBLE, scanned with a PARTIAL filter
       └─ part-001.vortex             DATA FILES ONLY — the replacement rows.
                                      deletion vectors live under current's deletions/;
                                      upsert tombstones are inline (cayenne_inlined_delete)

(For partitioned tables, _partitioned_wal/<commit_id>.json on local FS
 anchors the cross-partition atomic commit.)
```

### The three snapshot kinds

A `<snapshot_id>` directory is a UUIDv7-named set of immutable Vortex files (and their deletion vectors). Three kinds coexist under one table; they differ in **visibility** and **which deletions apply at scan time**, not in on-disk shape:

| Kind | Visible to reads? | Deletion filter at scan | Lifetime · created by |
|------|-------------------|-------------------------|------------------------|
| **Current** | yes — the base (`cayenne_table.current_snapshot_id`) | **full** — every deletion applies | exactly one at a time; replaced by an atomic pointer flip. Genesis + compaction output |
| **Staging** | **no** — pre-visibility | n/a | transient; written in **Stage A** of every burst under `_wal.json`. **Stage B** moves/publishes it; the WAL self-heals on crash |
| **Protected** | yes — **unioned** with current | **partial** — only `delete_seq > threshold` | retained; they accumulate and are folded by maintenance compaction. Created by an on-conflict upsert / pending-PK-delete publish (key-delete path) |

**Why protected snapshots exist:** a publish stages freshly-inserted rows at sequence numbers *above* the deletions already baked into the current snapshot. Those older deletions provably cannot apply to the new rows, so the writer skips re-resolving the entire deletion set against them — it publishes the new data as a protected snapshot with a **threshold = the snapshot's own allocated sequence** (persisted in [`cayenne_snapshot_sequence`](#cayenne_snapshot_sequence), cached as `protected_snapshots: snapshot_id → threshold`). At scan time the read applies the full deletion filter to current and a partial one (`delete_seq > threshold`) to each protected snapshot, then **unions** them. They are never silently cleared — each holds valid data under its own filter; `compact_protected_snapshots_subset` (count/age-triggered) folds the older prefix into one self-contained merged snapshot, keeping the newest `K` unbaked and seq-prefix-baking `delete_seq ≤ T`.

### Layout by workload shape

The same primitives produce visibly different trees depending on whether the workload deletes.

**1 · Append-only** (plain INSERT / batch load):

```text
<table_id>/
└─ <current_snapshot_id>/           ◀ one snapshot, GROWS IN PLACE
   ├─ part-0001.vortex             each append moves its staged files in here —
   ├─ part-0002.vortex             current_snapshot_id does NOT change
   ├─ part-0003.vortex
   └─ (no deletions/)              nothing is ever deleted

       │  compaction (small-file tier → target size)
       ▼
<table_id>/
└─ <NEW current_snapshot_id>/       ◀ pointer FLIPS to a fresh UUIDv7
   └─ part-0001.vortex             consolidated; prior dir retired + swept
```

**2 · CDC append** (`cdc_durability: memory`, no PK conflicts):

```text
 RAM   [ mem-tier: newest bursts — NOT on disk yet ]
 meta  [ cayenne_inlined_data: tiny batches as Arrow-IPC blobs ]

<table_id>/
├─ <current_snapshot_id>/           durable data
│  ├─ part-0001.vortex             ◀ written by a periodic CHECKPOINT
│  └─ part-0002.vortex               (flushes the RAM tier to one Vortex file)
└─ <staging_id>/                    ◀ exists only mid-burst (Stage A)
   ├─ _wal.json                     durable file list (tmp+fsync+rename)
   └─ part-*.vortex                 source LSN acked once this is durable

 (still no deletions/, no protected snapshots — pure append)
```

**3 · Updates / deletes** (upserts + deletes, key-delete path):

```text
 meta  [ cayenne_snapshot_sequence: {A → seqA, B → seqB} ]  ◀ thresholds
       [ cayenne_inlined_delete:    unflushed tombstones    ]

<table_id>/
├─ <current_snapshot_id>/           ◀ base (genesis / compaction output)
│  ├─ part-0001.vortex             scanned with the FULL deletion filter
│  └─ deletions/<del>.arrow        key-based: row_key, deleted_at
├─ <protected_snapshot_A>/          ◀ upsert publish #1  (filter: delete_seq > seqA)
│  └─ part-*.vortex                replacement rows ONLY (seq > seqA); their tombstones
│                                  are inline + under current's deletions/, NOT here
├─ <protected_snapshot_B>/          ◀ upsert publish #2  (filter: delete_seq > seqB)
│  └─ part-*.vortex
└─ <staging_id>/  (mid-burst)       _wal.json + part-*.vortex

 scan = current(FULL)  ∪  A(partial)  ∪  B(partial)  ∪  mem-tier / inline

       │  maintenance compaction (count/age trigger): fold older prefix,
       │  keep newest K unbaked, seq-prefix-bake delete_seq ≤ T
       ▼
<table_id>/
├─ <current_snapshot_id>/           (unchanged)
├─ <merged_snapshot_id>/            ◀ A+B folded into ONE self-contained dir
│  └─ part-*.vortex                seq-prefix baked: tombstones ≤ T applied into the data
└─ <protected_snapshot_C>/          ◀ newest K kept (active delete stream)
```

**Vortex data files** — immutable once written. Target size `cayenne_target_file_size_mb` (default 256 MB); `btrblocks` (default) or `zstd` compression; optional sort columns. In Cayenne a "file" (`DataFile`) is a Vortex `ListingTable` rooted at a unique directory, not necessarily a single object. Footer statistics are cached in `cayenne_snapshot_file_statistics`.

**Deletion files** — Arrow IPC under `<snapshot_id>/deletions/<delete_file_id>.arrow`. The on-disk schema *is* the type discriminator:

| Deletion mode | Schema | When |
|---------------|--------|------|
| **Position-based** | `row_id: UInt64`, `deleted_at: Int64` (µs) | PK-less tables (always), or any table in `deletion_mode: position` |
| **Key-based** | `row_key: Binary`, `deleted_at: Int64` (µs) | PK tables in `deletion_mode: key` (`auto` ⇒ key for PK tables) |

**Staging WAL (`_wal.json`)** — makes a staged file list crash-safe via tmp + fsync + rename. On the next provider open, unreconciled WAL markers self-heal (atomic rename of staged Vortex files into the current snapshot, or rollback).

**Partitioned WAL (`_partitioned_wal/<commit_id>.json`)** — anchors a cross-partition atomic commit on local FS.

**Snapshot slice (`metadata/<dataset>.slice.json`)** — when a dataset is snapshotted, Cayenne exports a **per-dataset metastore slice** (versioned `format_version: 1` JSON of every metastore row for one `cayenne_table`), *not* the raw `cayenne.db`. Path columns are rewritten relative to a writer-side anchor on export and re-anchored on import, so snapshots are portable across nodes with different data directories. `cayenne.db`, `cayenne.db-wal`, and `cayenne.db-shm` are **excluded** from the snapshot tar.

---

## 3. In-memory state — an ontology

`CayenneTableProvider` carries ~57 fields of in-memory state (the `CayenneTableProvider` struct in `crates/cayenne/src/provider/table.rs`). They are *not* all "data" and they are *not* all ephemeral. The organizing question is **durability class** — what happens to each on a crash:

| Class | Meaning | On crash | Examples |
|-------|---------|----------|----------|
| **Derived cache** | a fast-read projection of a durable source, rebuilt on open/restart | no loss — reloaded from the metastore / files | listing table, **protected snapshots**, deletion index, inline cache, PK keyset, statistics, sequence allocator |
| **Ephemeral** | RAM-only payload with no durable copy | discarded, then **re-streamed exactly-once** from the source slot | the CDC mem-tier (and its tombstones) — *the only data-bearing ephemeral state* |
| **Coordination** | locks, fences, atomics, scheduling flags — no payload | irrelevant (reconstructed empty) | `listing_fence`, `write_lock`, generation counters, staging trackers, GC maps |
| **Accounting** | memory-pool reservations | irrelevant | `CayenneMemoryAccount`, the global `MemTierBudget` |

> **Are protected snapshots in memory?** Only as a **derived cache**. The `protected_snapshots: Arc<ArcSwap<HashMap<String, i64>>>` field (the `protected_snapshots` field of `CayenneTableProvider` in `crates/cayenne/src/provider/table.rs`) maps `snapshot_id → minimum_sequence` for wait-free read-path routing, but it is **not the source of truth**: the `load_protected_snapshots` method (in `crates/cayenne/src/provider/table.rs`) rebuilds it from the durable [`cayenne_snapshot_sequence`](#cayenne_snapshot_sequence) table on every open and restart, and writes go to *both* `set_snapshot_sequence` (persist) and `protected_snapshots.rcu(…)` (refresh cache). The protected-snapshot **data** lives in on-disk per-snapshot directories. So protected snapshots are durable; the `ArcSwap` is just the index in front of them.

The same caveat applies to almost every "cache" below — the durable backing is named in each cluster.

### Concurrency vocabulary

- **`ArcSwap<T>`** — wait-free RCU. Readers `load()` an immutable `Arc<T>` and never block; writers build a new `T` and atomically swap it in. Used for everything on the read hot path.
- **`im::HashMap`** — structurally-shared persistent map (HAMT); cloning is an O(1) root-`Arc` bump, so an append shares structure with the prior version instead of deep-copying.
- **`tokio::sync::{Mutex,RwLock}`** — async coordination (held across `await`s — metastore I/O, encode).
- **`parking_lot::Mutex`** (`ParkingMutex`) — short non-async critical sections.
- **`Atomic*` / `OnceLock`** — lock-free counters/flags / one-time init of background tasks.

### A. Visibility &amp; listing — *derived cache* (← `cayenne_snapshot_file`, `current_snapshot_id`, on-disk dirs)

| Field | Type | Role |
|-------|------|------|
| `listing_table` | `Arc<ArcSwap<ListingTable>>` | the Vortex listing table scanned by reads; swapped atomically on publish |
| `current_snapshot_id` | `Arc<RwLock<String>>` | the snapshot a scan resolves (compaction advances it without touching `table_metadata`) |
| `current_sorted_snapshot` | `Arc<ArcSwap<Option<String>>>` | attestation: `Some(id)` iff the current snapshot was produced by sorted compaction — gates sound `output_ordering` |
| `scan_file_statistics` | `Arc<dyn FileStatisticsCache>` | per-file footer stats for the direct scan planner |
| `listing_fence` | `Arc<RwLock<()>>` | the read/write barrier — scans take read, the publish step takes write |
| `scan_state_lock`, `visibility_lock` | `Arc<RwLock<()>>` / `Arc<Mutex<()>>` | make deletion-view + protected-snapshot + inline visibility flip atomically to scans |

### B. Protected-snapshot routing — *derived cache* (← `cayenne_snapshot_sequence`)

`protected_snapshots: Arc<ArcSwap<HashMap<String, i64>>>` — `snapshot_id → min_sequence`. A read of a protected snapshot can skip deletion runs whose `max_delete_seq ≤ min_sequence`. Reloaded by `load_protected_snapshots`; written via `rcu`. (See the callout above.)

### C. Deletion view — *derived cache* (← `cayenne_delete_file` + `cayenne_insert_record` + inline deletes)

The richest structure. One `ArcSwap` per table publishes a frozen, fused index; concurrent scans always see consistent delete/re-insert pairs. The type hierarchy (`provider/deletion_strategy.rs`, `provider/deletion_index.rs`):

```
PkDeletionStrategyWithCache            (enum — one variant per PK shape)
├─ PositionBased  { cached_deleted_row_ids: Arc<ArcSwap<PositionBitmap>> }   ← no PK; RoaringBitmap per file
├─ Int64Pk        { deletion_snapshot:   Arc<ArcSwap<Int64PkDeletionSnapshot>>,
│                   position_deletions:  Arc<ArcSwap<PositionBitmap>> }
└─ RowConverterBased { deletion_snapshot: Arc<ArcSwap<RowConverterDeletionSnapshot>>,
                       position_deletions: Arc<ArcSwap<PositionBitmap>> }

Int64PkDeletionSnapshot       { tombstones: Arc<DeletionIndex> }       DeletionIndex.core    = LayeredRuns<i64,  XxHash3>
RowConverterDeletionSnapshot  { tombstones: Arc<KeyDeletionIndex> }    KeyDeletionIndex.core = LayeredRuns<u128, prehashed>
                                                                       (u128 = XXH3-128 of the RowConverter bytes;
                                                                        bytes not retained → 32 B/entry regardless of PK width)
```

**`LayeredRuns<K,S>`** — an LSM in miniature for tombstones:

| Field | Type | Role |
|-------|------|------|
| `runs` | `Vec<Arc<RunData<K,S>>>` | frozen runs, oldest first; shared by `Arc` clone on publish |
| `active` | persistent `HashMap<K, TombstoneEntry, S>` | the small mutable delta tier (new writes land here) |
| `bloom` | `Arc<SplitBlockBloomFilter>` | global deletion-membership bloom, extended in place, 2× headroom |
| `max_sequence_number`, `min_deleted_key`, `max_deleted_key` | `Option<i64>`/`Option<K>` | protected-snapshot install-skip + i64 PK-range pruning |
| `entry_count` / `delete_count` / `insert_count` | `usize` | fused distinct-key counters |

- **`RunData<K,S>`** = `{ map: Arc<HashMap<K, TombstoneEntry, S>>, max_delete_seq: i64, bloom: Arc<SplitBlockBloomFilter> }` — immutable once frozen.
- **`TombstoneEntry`** = `{ delete_seq: i64, insert_seq: i64 }` (`i64::MIN` = `SEQUENCE_ABSENT`). The **fused** entry: one probe answers both "deleted?" and "re-inserted after the delete?" (visible iff `insert_seq > delete_seq`).
- Freeze threshold `max(DELTA_MERGE_MIN, base/4).min(FREEZE_CAP)`; runs size-tier-fold past `MAX_FROZEN_RUNS` (prod: `DELTA_MERGE_MIN`=16_384, `FREEZE_CAP`=262_144, `MAX_FROZEN_RUNS`=8, `MIN_BLOOM_CAPACITY`=64).
- `DeletionIndex::shared_empty()` / `KeyDeletionIndex::shared_empty()` — a process-wide `LazyLock<Arc<…>>` reused by all tables with no deletions, amortizing the bloom allocation.

### D. Inline / level-0 cache — *derived cache* (← `cayenne_inlined_data` / `cayenne_inlined_delete`)

| Field | Type | Role |
|-------|------|------|
| `inlined_cache` | `Arc<ArcSwap<InlinedCache>>` | decoded inline batches — a generation hit avoids Arrow-IPC decode + two metastore round-trips per scan |
| `inlined_generation` / `inlined_structural_epoch` | `Arc<AtomicU64>` | cache validity (generation) and the strict-subset epoch that licenses the append-only delta path |
| `inlined_row_count` / `durable_inlined_row_count` | `Arc<AtomicI64>` | live vs. durable corpus size — drives checkpoint and zero-corpus fast paths |
| `published_inlined_seq` | `Arc<AtomicI64>` | in-memory visibility watermark (entries invisible until the watermark advances under `scan_state_lock`) |
| `pending_tombstone_deltas`, `inlined_locally_published`, `pending_durable_tombstone_flips`, `pending_inline_tombstones` | `ParkingMutex<…>` / `AtomicU64` | the in-memory tombstone publish/defer machinery behind the per-tombstone `published` flag |

**`InlinedCache`** = `{ generation: u64, structural_epoch: u64, materialized_through_sequence: i64, tombstone_delta_seq: u64, batches: Arc<Vec<RecordBatch>>, view: Arc<Vec<InlinedViewEntry>> }`.

### E. CDC mem-tier — **ephemeral** (RAM-only; re-streamed exactly-once)

The *only* data-bearing state with no durable copy. `mem_tier: Arc<ArcSwap<MemTier>>` (`provider/mem_tier.rs`), empty in `file` mode. Discarded on crash; the source slot holds at most the last-durable LSN, the source re-streams everything past it, and the PK-idempotent apply reconciles exactly-once. The `SlotAdvancer` callback advances the slot **only after** the covering rows/tombstones are durable.

**Ingestion / immutable split (the seal).** The tier is split at `sealed_segments`: `segments[0..sealed_segments]` are the **immutable piece** (already durably *shadowed*), `segments[sealed_segments..]` the **active ingestion piece**. A periodic **seal** (`cdc_mem_tier_seal_age_ms`, default 2 s) durably shadows the active piece into the *unpublished* inline corpus — insert rows as an inline BLOB (`commit_inlined_data_durable`, no `publish_inlined_mutation`), tombstones as delete-vectors (`commit_mem_tier_checkpoint_metadata`, update dropped) — then fires the `SlotAdvancer`. This **decouples the slot ack (replication/freshness lag) from the heavy protected-snapshot bake**: the slot advances every seal cadence, with no Vortex encode, no listing-fence publish, and no read amplification. Reads are unaffected — they still union the *whole* RAM tier (the split is invisible to the scan path); the shadow is invisible in-process (`published_inlined_seq` is not advanced) and is replayed only on restart (the watermark reseeds from the durable `current_sequence_number`; `publish_orphan_inlined_deletes` re-activates shadow tombstones). A later bake re-flushes the same rows to Vortex and clears the shadow (`mem_tier_shadow_present` forces the clear even though the *published* inline view is empty). Seal and bake are serialized by `mem_checkpoint_lock` (single-drainer for `fire_slot_advancer`). Sealing is **all-shards-atomic** at every fan-out N: it takes the same `write_lock` + per-shard publish locks the checkpoint uses (so a mid-fan-out apply is captured all-or-none), shadows the cross-shard union (disjoint keys ⇒ concatenated rows + unioned tombstones) as one inline BLOB, and advances the slot to the **MAX** per-apply epoch across shards — the same axis and MAX rule the checkpoint uses. At N==1 no `write_lock` is taken and the path is byte-identical to the single-shard seal.

**`MemTier`**:

| Field | Type | Role |
|-------|------|------|
| `segments` | `Arc<Vec<MemSegment>>` | append-log; one `MemSegment` per CDC apply |
| `sealed_segments` | `usize` | ingestion/immutable split point — `segments[0..sealed_segments]` are durably shadowed |
| `tombstones` | `InMemTombstones` | accumulated max-delete-seq per key — persistent `im::HashMap` (O(1) clone on append) |
| `bytes` / `rows` / `superseded` | `u64` | budget + observability |
| `epoch` / `version` | `u64` | epoch persists across post-checkpoint clears; version bumps on append *and* retain |
| `oldest_append` | `Option<Instant>` | drives the age cap |

- **`MemSegment`** = `{ batches: Arc<Vec<RecordBatch>>, data_sequence: i64, statistics: Arc<Statistics>, tombstones: SegmentTombstones, bytes, rows, superseded }`.
- **`InMemTombstones`** = `{ int64_pk: im::HashMap<i64,i64>, row_keys: im::HashMap<Box<[u8]>,i64> }` (key → max delete seq); merge-on-read hides a row at `data_sequence` iff a tombstone has `delete_seq ≥ data_sequence`.
- **`SegmentTombstones`** — one apply's deleted-key *sets* (built off the publish lock) + a single reserved `delete_sequence` (stamped under the lock).
- Bounded on four axes: per-table byte cap, age cap, periodic tick, and the process-global **`MemTierBudget`** = `{ total: u64, used: Arc<AtomicU64> }` (`mem_tier_budget.rs`), so a fleet of tables can never OOM.
- Locks: `mem_checkpoint_lock` (serializes spills; `try_lock` detects an in-flight checkpoint), `mem_tier_publish_lock` (serializes the append/seq-reservation, decoupled from `listing_fence`).

### F. PK keyset — *derived cache* (← re-scan of data / `cayenne_pk_index`)

`pk_keyset_cache: Arc<ParkingMutex<Option<CachedPkIndex>>>` — the visible PK set for auto on-conflict detection, so a burst doesn't re-scan Vortex files. Byte-budgeted at `DEFAULT_PK_KEYSET_CACHE_MAX_BYTES` (256 MiB default; `cayenne_pk_keyset_cache_mb` overrides):

```
CachedPkIndex
├─ Exact(CachedPkKeyset { keys: HashMap<OwnedRow, RowLocation>, approx_bytes, captured_files: HashSet<Arc<str>> })
└─ Bloom(PkBloom      { bits: Vec<u64>, bit_mask: u64, inserted_keys })     ← fallback when exact would exceed budget
```

### G. Statistics — *derived cache* (← `cayenne_table_statistics`, `cayenne_snapshot_file_statistics`)

`table_statistics: Arc<RwLock<CachedTableStatistics>>` — the optimizer-facing `Statistics` and the raw blob under one lock; `table_statistics_persistence_lock` serializes the read/merge/upsert cycle.

### H. Sequence allocation — *derived cache* (← `cayenne_table.current_sequence_number`)

`seq_allocator: Arc<Mutex<SeqAllocator>>` — hands out every sequence number; refills in batches from the metastore high-water column to cut round-trips.

### I. Maintained aggregates &amp; schema

`maintained_aggregates: Arc<MaintainedAggregateRegistry>` + `maintained_aggregate_epoch: Arc<AtomicU64>` (served only when the scan's epoch matches); `table_schema: Arc<ArcSwap<Schema>>` (live widening while scans keep their loaded ref); `pk_row_converter`, `pk_column_indices`.

### J. Coordination, staging &amp; GC — *no payload*

Locks (`write_lock`, `compaction_lock`, `seq_allocator` mutex), staging trackers (`staging_wal_present`, `staging_may_have_files`, `inflight_staging_appends`, `last_moved_snapshot_files`), compaction flags (`new_files_since_last_compaction`, `post_write_compaction_scheduled`, `position_compaction_skip_streak`), background tasks (`background_compactor`, `background_mem_tier_checkpointer` — `OnceLock`), and the in-flight-scan GC guards (`snapshot_scan_refs`, `snapshot_last_listed`, `retired_snapshot_dirs`) that keep a file alive until both retirement *and* the last scan that listed it have passed.

### K. Memory accounting

`table_memory: Arc<CayenneMemoryAccount>` = `{ reservation: ParkingMutex<MemoryReservation>, keyset_bytes: AtomicUsize, deletion_bytes: AtomicUsize }` — registers resident keyset + deletion-index bytes against the DataFusion `MemoryPool` (`runtime.query.memory_limit`) so the query path sees the real budget. It *reports*, it does not gate; the real bounds are the PK-keyset byte budget and the global `MemTierBudget`.

---

## 4. Conventions

- **Identifiers** — `table_id`, `snapshot_id`, and most ids are UUIDv7 text. The sole exception is `cayenne_insert_record.table_id`, stored as the 16 raw UUID bytes (a pure re-encoding via `table_id_to_key_bytes` to shrink the hot upsert table's clustered key).
- **Sequence numbers** — monotonically increasing, reserved from the metastore (`reserve_sequence_numbers` batches reservations to cut round-trips). Every insert, delete, and re-insertion carries one; the scan path uses them to resolve "deleted, and re-inserted after that?".
- **Visibility** — the authoritative state is `cayenne_table.current_snapshot_id`; a write or compaction becomes visible by an atomic flip of that pointer under the listing fence, never by mutating files in place.
- **Schema validation** — `validate_existing_schema` compares column **names and ordering** only; a mismatch returns `CatalogError::SchemaMismatch` with an actionable "clear your acceleration data" message.
- **Backends** — SQLite uses `BEGIN IMMEDIATE`; Turso uses `BEGIN CONCURRENT` (MVCC). DDL is identical across both except `WITHOUT ROWID` (SQLite-only).

> The DDL source (`metastore/sqlite.rs`) is authoritative; treat this as a quick reference. See [`README.md`](../README.md) for how these structures are used.
