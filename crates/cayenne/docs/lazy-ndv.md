# Lazy NDV computation

Per-column NDV (distinct-count) is maintained as a `HyperLogLog` sketch and
consumed only as an optimizer **estimate** (`Precision::Inexact(distinct_count)`)
to size distributed joins and group-bys. Because it is a hint, it does not need
to be maintained on the synchronous CDC apply path.

## What this does

NDV is now folded **only when values are first promoted from memory to a
persisted Vortex file** — every `write_to_snapshot` spill: checkpoint spills
(inline memtable / mem-tier → file), staged appends, inline-overflow file
writes, compaction rewrites, and overwrites. The **inline tier0 write** (the
synchronous hot loop that buffers small CDC deltas into a metastore BLOB) no
longer folds NDV; its rows are sketched for free when the memtable next spills
to a file.

Concretely, the only behavioral change from eager folding is at the inline path
(`AppendMutationWriter`): its `ColumnStatsAccumulator` is built with
`new_with_ndv(schema, false)` — lazy is the only path. Min/max/null-count
stats are still maintained there; only the per-batch NDV hashing is removed
from the hot loop.

The table keeps a **single global aggregate sketch**
(`cayenne_table_statistics.ndv_sketches`), maintained exactly as before — no
schema change. Each write merges its sketch into the aggregate (register-wise
union); a skipped inline write contributes an empty sketch, i.e. a **no-op merge**
that preserves the aggregate.

## Why it's safe

The aggregate merge is an idempotent register-wise union, so folding a set of
rows once (at their checkpoint spill) yields the **same** aggregate as folding
them repeatedly (eager). The only observable difference is a **bounded lag**: a
row's distinct-count reaches the aggregate at the inline memtable's next
checkpoint rather than at inline-write time — bounded to roughly one memtable.
NDV is an inexact optimizer hint, so a brief lag does not affect correctness (a
prior experiment validated this at SF100/SF1000 with all data and
analytical-query correctness gates passing). Deletes are unchanged from today:
the union-only aggregate still over-counts between compactions (the safe
direction for join sizing) and self-heals when compaction recomputes it from
live rows.

## Merge performance

`NdvSketches::merge_serialized` folds register bytes **directly from the blob
slice** into the per-column accumulator in one autovectorized `max` pass
(`pmaxub`/`umax`), instead of deserializing each column into a transient
`Vec` + `BTreeMap` and doing a second pass — ~16–20× faster at realistic sizes,
verified register-for-register identical to the prior path. This is the
write-time aggregate-merge path, so it speeds up NDV persistence on every commit.

## Per-value hashing cost

`ColumnStatsAccumulator::add_column_to_hll` folds every non-null value of every
NDV-tracked column through `HyperLogLog::add_i128`/`add_bytes`, each hashing one
value via `hash_index::hash_key_bytes_oneshot` (one-shot XXH3-64) rather than
constructing a fresh streaming `XxHash3_64` hasher per value. Byte-identical to
the prior streaming call (pinned by
`hash_key_bytes_oneshot_matches_streaming` in `hash-index`), so persisted
sketches are unaffected — this only removes per-value hasher setup cost.

`benches/hll_ndv_hashing.rs` measured (Apple Silicon, `bench` profile,
1K/100K values per fold, `cargo bench -p cayenne --bench hll_ndv_hashing`):

| shape       | streaming XXH3 | one-shot XXH3 | FxHash      |
|-------------|---------------:|--------------:|------------:|
| i128 (1K)   |     30.1 ns/val |    1.21 ns/val |  0.341 ns/val |
| i128 (100K) |     30.6 ns/val |    1.21 ns/val |  0.351 ns/val |
| utf8 (1K)   |     26.8 ns/val |    1.23 ns/val |  0.554 ns/val |
| utf8 (100K) |     26.8 ns/val |    1.33 ns/val |  0.630 ns/val |

One-shot XXH3 is **~21–25× faster** than streaming XXH3. `FxHash` is a further
**~2.1–3.5× faster than one-shot XXH3** (~45–90× faster than the original
streaming baseline) but is **not adopted**: it produces different hash values,
which would desync newly folded sketches from already-persisted ones. The
serialized blob already carries a `SKETCH_FORMAT_VERSION` byte, checked by
`parse_columns` — a mismatched version is already treated as "absent" (dropped
on merge, not read), so switching hash functions is *mechanically* a version
bump away. But `persist_table_stats_locked` merges each write's fresh sketch
with the *existing persisted* blob (`merge_serialized`), so a version-mismatched
existing blob would be silently dropped rather than merged — the persisted NDV
would reset to just the current write's rows until the next full compaction
rebuilds it from a live-row rescan (`ColumnStatsAccumulator::new` per
compaction), the same self-healing property already relied on for deletes.
Unlike deletes (which only ever over-count — the safe direction), a version-cut
reset would transiently *under*-count, so adopting a different hash function
would need an explicit rebuild trigger rather than relying on the natural
compaction cadence. Not pursued here since one-shot XXH3 already removes the
dominant cost (hasher construction) while staying value-preserving.

## Follow-up (separate PR): per-file sketches for deletion tolerance

The single global aggregate is union-only, so it over-counts under deletes until
a full compaction rewrite recomputes it. A follow-up will make NDV
**deletion-tolerant** by storing a sketch **per snapshot file** (source of truth,
computed once at file birth — exactly the "compute on spill" point this PR
introduces) and deriving the table estimate as a **union over the current live
files' sketches**. When a file leaves the live set (compaction/overwrite/full
delete) its sketch simply drops out — no subtraction, so re-insertion and
multiset/foreign-key columns are handled correctly. That work needs a metastore
schema addition (`ndv_sketches` on `cayenne_snapshot_file_statistics`) and a
cumulative-rebuild step at compaction, so it is deliberately kept out of this PR.
