# Cayenne Cold (Datalake) Tier — Implementation Review (2026-07-11)

Status: review findings + test-gap analysis, branch `sgrebnov/0710-cold-pk-bloom-split`
(includes the row-capped promotion / 32 MiB PK-bloom cap, PR #11812). Four parallel
review passes: correctness/restart, query performance, GC/layout/config wiring,
and test-coverage inventory. Line references are as of this branch.

Priority items P1–P4 (see "Suggested priority order") are implemented on
`sgrebnov/0711-cold-tier-hardening`:

- **P1** — `promote_warm_to_cold_inner` now holds one `listing_fence.write()`
  across the metastore cold commit AND the in-memory snapshot flip
  (`publish_overwrite_snapshot_fenced`); regression test
  `test_cold_tier_concurrent_scan_during_promotion` hammers `COUNT(*)` from
  4 concurrent tasks across three promotions (fresh, dirty rewrite,
  carry-forward).
- **P2** — `test_cold_tier_restart_reopen` reopens the table from a FRESH
  catalog connection after promote + delete: cross-tier reads, tombstone
  survival, a new delete against a cold-resident key, and a further promotion
  all from persisted state.
- **P3** — `validate_datalake_table_options` (runtime accelerator, pure +
  unit-tested) rejects at registration: explicit `cayenne_deletion_mode:
  position` (no longer silently overridden; `auto` still resolves to
  `key`), `cayenne_datalake_promotion_interval_ms: 0`,
  `cayenne_datalake_gc_interval_ms: 0`; WARNS (registers, tier inactive) on
  a PK-less table — relaxed from an error 2026-07-11 so a fleet-wide
  datalake location doesn't block PK-less datasets — and on unknown
  clustering columns.
- **P4** — `run_cold_tier_gc_tick` is `pub`; `test_cold_tier_gc_end_to_end`
  verifies mark-then-sweep grace on a planted orphan, survival of
  manifest-referenced and non-`.vortex` objects, and physical reclamation of
  a superseded generation. `drop_table` comment corrected (F4): physical cold
  objects are intentionally NOT deleted on drop.

---

## 1. Correctness findings (ranked)

### F0 — CONFIRMED + FIXED (2026-07-12): seq-prefix bake resurrects superseded cold rows
Found investigating the CH-benCH `-cold` non-convergence: every PROMOTED table
over-counted by ≈ its update count (local SF100 runs 6/7, e.g. stock
10,000,000 source vs 11,789,715 spiced). Post-mortem on run 7's persisted
metastore: the cold manifest still held all 10M rows at seq ≤ 2870, and all
3.72M key tombstones (seq 2871–42417, all key-scoped) existed in the catalog —
yet the scan masked none of them. Mechanism: the **seq-prefix bake**
(`bake_seq_prefix_protected_snapshots`) physically applies tombstones to the
WARM snapshots it rewrites, then prunes them from the in-memory deletion index
(`prune_deletion_index_at_or_below`) — but the bake never rewrites COLD
objects, so a pruned tombstone that was masking a superseded cold-resident key
silently resurrects the stale cold row. The full-rewrite prune
(`prune_deletion_caches_after_full_rewrite`) had the same blind spot. Unit
tests never triggered a bake, which is why delete/upsert-after-promotion tests
passed while every sustained-update benchmark failed.

**Fix:** cap both prune cutoffs at the cold manifest's max sequence
(`cold_tombstone_prune_cap`) — tombstones above it stay load-bearing for the
cold scan branch until a promotion physically applies them (the promotion
commit then legitimately clears the index). Regression test
`test_cold_tier_bake_preserves_cold_masking_tombstones` drives the real bake
with disjoint-range update rounds; without the cap it fails at 1300 vs 1000
(the 3 pruned rounds' cold rows resurrect), with it the full 9-test cold suite
passes. Memory cost: tombstones accumulate between promotions instead of being
baked away — bounded by the promotion cadence.

### F1 — CONFIRMED correctness bug: transient cross-tier over-count during promotion
`promote_warm_to_cold_inner` publishes visibility at two unsynchronized points:
`commit_overwrite_to_cold` (`table.rs:13970`) makes the NEW cold files visible via
the metastore (`list_cold_tier_files`) with **no listing fence held**, and only the
subsequent `publish_overwrite_snapshot` (`table.rs:13973`) takes
`listing_fence.write()` to flip the **in-memory** `current_snapshot_id`. A scan's
warm branch uses the in-memory snapshot id while its cold branch queries the
metastore directly (`build_cold_tier_scan_plan`, `table.rs:21778`). In the
commit→publish gap a concurrent scan pairs the OLD warm snapshot (whose files
still physically exist — cleanup is deferred) with the NEW cold manifest →
promoted rows counted **twice**. Over-count only, never under-count; window is two
sequential awaits but reachable under concurrent read load (exactly the HTAP
benchmark shape).

**Fix (P1):** build the new listing table first (fallible part), then hold
`listing_fence.write()` across the metastore commit *and* the in-memory flip so
both visibility points publish atomically w.r.t. scans. Lock ordering preserved
(`write_lock → listing_fence`, both already taken in that order by promotion).

### F1b — NEW (2026-07-11, code-reading confidence): promotion vs pipelined Stage-B finalize → lost rows
Found while answering "can promotion conflict with fast protected-snapshot
compaction?" (that pair is safe — see §1a below). The pipelined CDC upsert's
Stage-B finalize (`finish()`, `table.rs:345`) takes only `visibility_lock` +
the listing fence; the table write guard is dropped at the end of Stage-A
(`mutation_writer.rs:636`) and Stage-B runs as a spawned task the CDC loop
joins lazily (`changes.rs:1244`). Promotion holds only `write_lock` and its
commit is a delete-everything-by-`table_id` overwrite, with NO equivalent of
the folded-set bracket the full rewrite uses against exactly this
"concurrent finalize" (`table.rs:12890–12939`). Interleaving: Stage-A commits
(sequence row durable, **source slot acked**) → promotion takes the write
lock and plans its scan before Stage-B publishes (staged rows invisible →
missed by the cold set) → promotion's commit deletes the staged snapshot's
`cayenne_snapshot_sequence` row and clears the in-memory protected map →
batch's rows are in neither warm, nor cold, nor catalog → lost at restart
(slot already advanced). Window is milliseconds (Stage-B is spawned
immediately) but structurally open for `refresh_mode: changes` + datalake
whenever a burst takes the durable pipelined path (`cdc_durability: file`,
or memory-mode durable fallbacks: delete bursts, byte-cap spills).
**Unverified by a repro test.** Candidate fixes: promotion drains/joins the
pending finalize before capture; bracket-and-abort like the full rewrite;
or overwrite-clear by explicit folded snapshot ids instead of `table_id`.

### F1a — Promotion vs fast protected-snapshot subset compaction: catalog
half safe, in-memory half NOT (found 2026-07-11, code-reading confidence)
The two passes overlap (promotion: `write_lock` only; subset pass:
`compaction_lock` only on key-mode tables). The CATALOG side is safe in both
commit orders: the subset commit is a CAS (`swap_protected_snapshots_in_txn`
counts its input ids, `cayenne_catalog.rs:583`) that aborts and discards its
output if promotion's overwrite-clear already removed them; and a merge
whose CAS lands mid-promotion is content-equivalent to its inputs (whose
files stay pinned by promotion's ref-counted scan,
`sweep_retired_snapshot_dirs` defers in-flight dirs, `table.rs:3097`) and
its merged row is wiped by promotion's delete-by-`table_id`.

**BUT the merge's Phase-3 in-memory publish is unguarded**
(`table.rs:14569–14579`): after a successful CAS it acquires
`listing_fence.write()` and RCUs the merged id into the protected map
UNCONDITIONALLY — no revalidation that an overwrite/promotion committed in
between. Interleaving: promotion starts (holds `write_lock`, long cold-write
phase) → merge's CAS commits (inputs still present) → promotion commits +
fenced-publishes (catalog wiped by `table_id`, in-memory protected map
cleared) → merge's queued RCU runs after the fence releases and RE-INSERTS
the merged snapshot into the now-empty map. Its files physically exist, so
scans read the whole pre-promotion warm row set from the merged snapshot
ALONGSIDE its cold copies → **silent double-count** until a restart (the
catalog is consistent, so reload heals) or the next promotion. Same bug
shape as F1: a second publication point missing the atomic-flip guard.
Fixes: revalidate `current_snapshot_id`/epoch under the Phase-3 fence and
skip the RCU + discard output on change (cheapest); guard the swap txn with
`WHERE current_snapshot_id = captured`; or promotion takes
`compaction_lock` so the passes never overlap.

### F2 — Risk: GC can sweep an in-flight promotion's files
GC treats any on-store `.vortex` absent from the manifest as an orphan
(`table.rs:2621–2641`). The background promoter and GC run on one serial per-table
loop (`table.rs:24552`), so they never race intra-process — but
`promote_warm_to_cold` is `pub` (callable outside the loop), and the safety margin
is then only the grace period (= `cayenne_datalake_gc_interval_ms`). A write phase
that outlives the grace can have its uncommitted objects swept → `NotFound` after
commit. Same hazard if two processes ever share one cold location + metastore
(the write lock is in-process only).

### F3 — Documented risk: long-running scan vs GC → `NotFound` (fails loud)
A scan planned against a superseded manifest that runs longer than the grace can
open GC'd cold files. `SnapshotScanRef` pins snapshot dirs, not cold URLs
(`table.rs:23278`); acknowledged at `table.rs:2529–2534`. Fails as an error, never
wrong results. Acceptable for v1; needs a pinning follow-up eventually.

### F4 — Resource leak + stale comment: table drop never deletes cold objects
`drop_table` deletes cold manifest **rows** only (`cayenne_catalog.rs:4131–4140`);
its comment claims "the physical cold objects are swept separately by the
table-drop physical cleanup" — **no such cleanup exists** (DDL drop path
`ddl/operations.rs:278–320` does metadata + deregistration only). After drop the
per-table GC loop is gone and the manifest (GC root) deleted, so every object
under `{location}/{slug}-{table_id}/data/` leaks permanently. Same for a dataset
removed from the spicepod. **Decision: physical cleanup on drop is intentionally
NOT implemented** (operator-managed lifecycle for the shared datalake bucket);
P4 corrects the misleading comment so the code honestly states objects are not
swept on drop.

### F5 — Back-compat: pre-rename bare-`table_id` prefixes orphaned forever
GC and scans only address the current `{slug}-{table_id}` segment
(`table.rs:2551`, `metadata.rs:93`). Objects written by earlier builds under bare
`{table_id}/data/` are invisible to both — leaked and unreadable. Feature is
unreleased, so "recreate tables" is acceptable, but it must be an explicit
decision + release note, not an accident.

### F6 — Silent misconfigurations (should be structured errors / warnings)
- **Datalake on a PK-less table → silently inert.** No registration error; the
  promoter early-returns `Ok(false)` forever (`table.rs:13813`). Docs claim a PK
  is required.
- **Datalake + explicit `cayenne_deletion_mode: position`** → warn-level override
  to `key` (PK present, `mod.rs:1409–1414`) or silent no-op (no PK). Docs say
  position deletes are unsupported; per project config rules this should be a
  structured error for the explicit-conflict case.
- **`cayenne_datalake_promotion_interval_ms: 0`** → background task never spawned
  (`compaction.rs:885`) → tier silently never promotes **and never GCs**, despite
  `datalake_location` being set. No diagnostic.
- **`cayenne_datalake_gc_interval_ms: 0`** → GC grace collapses to zero (the
  interval doubles as the orphan grace), destroying the long-scan safety window.
- **No `config_warnings` coverage for any cold param** (`metadata.rs:1333–1386`
  validates warm knobs only). Invalid `cayenne_datalake_clustering_columns`
  entries are silently dropped (`table.rs:13277`).

**Fix (P3):** structured registration errors for the first four; warnings for
degenerate values.

### Confirmed safe by design
- Crash after cold write, before commit → orphans only; GC re-derives the orphan
  set from (on-store − manifest) each pass and sweeps after one grace. Restart
  resets first-seen marks (in-memory) → one extra grace, never premature.
- Crash after commit, before old-warm cleanup → old warm dir is unreadable
  (not current, not protected); disk + `cayenne_snapshot_file` rows leak
  (resource only). Note: `commit_overwrite_in_txn` intentionally does not clear
  `cayenne_snapshot_file`.
- Tombstones / no resurrection across tiers: delete paths and promotion serialize
  on `write_lock`; cold branch reads use the Ignore-reinserts deletion filter;
  dirty-file classification is conservative in every failure direction; the cold
  PK-existence bloom has no false negatives (bloom dropped on row-count mismatch
  at read-back); keyset caches are cleared under the publish fence.

---

## 2. Query-performance findings

**Strengths** (all verified):
- Manifest-driven cold scans — `list_cold_tier_files` from the metastore, **no
  per-scan S3 LIST** (`table.rs:21778`); listing-time pruning runs from the
  persisted per-file min/max `statistics_blob` with **zero object-store
  round-trips** before pruning (`table.rs:21849–21869`, `file_pruning.rs:75`);
  footers are opened only for surviving files.
- Filter, projection, and limit pushdown wired into the cold `DataSourceExec`
  (`table.rs:21823, 21908, 21909`); limit correctly suppressed under pending
  key-mode deletions.
- PK-selective fan-out suppression (`target_partitions=1`) for point lookups;
  cold reads share the warm Vortex segment cache; physical-plan statistics
  attached (`compute_all_files_statistics`).

**Gaps** (ranked):
1. **Per-file PK blooms are never consulted at query time.** They exist in the
   manifest and serve keyset rebuild + promotion classification, but
   `build_cold_tier_scan_plan` never probes them. A `WHERE pk = K` point lookup
   reads every non-min/max-pruned file; for composite PKs the leading columns
   span the full domain so min/max cannot prune. Biggest read-amplification win
   available.
2. Min/max is the only skip mechanism; no Z-order-aware read beyond generic
   zone maps (Z-order is write-side only).
3. Cold manifest re-queried from the metastore on every scan (no query-path
   cache); `ListingTableUrl::parse` per file in the scan hot loop.
4. `TableProvider::statistics()` returns `None` whenever deletions are pending /
   inline rows exist — the optimizer loses all cold stats in those windows.
5. Zero-rowcount / empty-stats-blob files can never be pruned → scanned by every
   query (correct, but permanent per-file overhead).

---

## 3. GC / layout / config wiring

- Layout `{location}/{slug}-{table_id}/data/{promotion_id}/…` — no collision
  risk (UUIDv7 promotion ids, per-write UUID file prefixes). Empty
  `promotion_id` dirs are never removed (relevant only to `file://` cold tiers;
  S3 prefixes are virtual).
- GC lists the entire `data/` prefix each pass — O(live files), acceptable.
  Only `.vortex` objects are considered; the registration probe object
  (`.spice-datalake-probe-*`, written at the location root) is best-effort
  deleted and otherwise ignored.
- GC cadence is bounded by the promotion tick: GC fires from inside the
  promotion loop with a `due` gate, so `promotion_interval_ms >
  gc_interval_ms` silently coarsens GC — contradicting the docstrings.
- All `cayenne_datalake_*` params verified wired end-to-end (location,
  s3 auth/endpoint/key/secret/allow_http/timeout/unsigned_payload,
  clustering_columns, target_file_size_mb, warm_max_bytes/_files,
  promotion_interval_ms, gc_interval_ms). Both-triggers-unset defaults the byte
  trigger to 16 × target file size. Guards verified: `refresh_mode: full` +
  datalake rejected; non-`s3://` location rejected; `s3_auth: key` without
  key/secret is a structured error.

---

## 4. Test coverage matrix

Rows = review areas; cells = existing coverage → **gaps**.

| Area | Unit | Integration | Bench/e2e | Top gaps |
|---|---|---|---|---|
| Cross-tier query correctness | — | 3 tests in `cold_tier_test.rs` (promotion, carry-forward, delete-after-promotion, **concurrent-scan-during-promotion**; `file://` only) | chbench cold pods (no asserts) | ~~concurrent-scan-during-promotion (F1)~~ *(done)*; upsert vs cold-resident key; multi-chunk split promotion; empty/all-deleted promotion |
| Filter/pruning perf | `cold_partition.rs` classifier (13 tests), `file_pruning.rs` | — | chbench (perf only) | assert files are actually pruned by a selective WHERE; projection pushdown; no re-LIST on repeated scans |
| Storage layout, GC | GC pure-fn tests (mark/sweep/grace/restart/prune), `datalake_dir_segment` | layout assert in cold tests; **`test_cold_tier_gc_end_to_end`** (orphan grace, live+non-vortex survive, superseded gen reclaimed) | — | ~~end-to-end GC~~ *(done)*; drop-table cleanup *(decided: intentionally not implemented)*; GC-vs-slow-promotion |
| Scalability (streaming, PK) | row-cap/chunk tests, bloom round-trip/budget | — | chbench SF1000 | promotion above `cold_file_row_cap` (split path) end-to-end; keyset resolves `Bloom` post-promotion |
| App restarts | GC grace pure-fn only | **`test_cold_tier_restart_reopen`** (fresh catalog connection; manifest, reads/deletes, re-promotion) | — | ~~reopen-after-promotion~~ *(done)*; crash-between-write-and-commit |
| Config params | format/segment unit tests; **`validate_datalake_table_options` negative tests** (PK-less → inactive-tier warning, explicit position → error, interval=0 ×2 → error, unknown clustering column → warning) | plumbed implicitly | chbench pods | byte-trigger (`warm_max_bytes`) path |
| S3-specific | **NONE** | **NONE** (all `file://`) | daily MinIO chbench (perf only) | MinIO-backed correctness variant of the cold integration tests |
| CH-bench e2e | — | — | `sf1000-cold` daily cron; `sf10-cold` dispatch dir **orphaned** (not in cron map) | wire `sf10-cold`; add post-run row-count parity assertions |

---

## 5. Suggested priority order

1. **P1** Fix F1 (fence across commit+publish) + concurrent-scan-during-promotion
   test. *(implemented)*
2. **P2** Restart-with-cold-manifest integration test — zero coverage on a whole
   checklist row. *(implemented)*
3. **P3** Structured errors for the silent-inert configs (F6) + cold
   `config_warnings`. *(implemented)*
4. **P4** End-to-end GC test + comment fix for F4 (physical drop cleanup
   deliberately not implemented). *(implemented)*
5. Read-side PK bloom probing for point lookups (perf, F-perf-1).
6. Cold-URL pinning for long scans (F3); GC cadence decoupled from the promotion
   tick; legacy-prefix decision + release note (F5); MinIO-backed correctness
   test variant; wire `sf10-cold` into the dispatch cron.
