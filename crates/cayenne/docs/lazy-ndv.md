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
`new_with_ndv(schema, false)`. Min/max/null-count stats are still maintained
there; only the per-batch NDV hashing is removed from the hot loop.

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
