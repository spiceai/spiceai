# Cayenne datalake (cold) tier — test coverage matrix

Where the datalake tier is tested today, and which behaviours had no assertion
before this document existed. Scope is the storage-cascade bottom tier: warm →
cold promotion, the cold manifest, cold-branch scans, and datalake GC.

## Where the tests live

| Layer | Location |
| --- | --- |
| Integration — promotion/scan/delete/GC | `crates/cayenne/tests/cold_tier_test.rs` |
| Integration — statistics conservation | `crates/cayenne/tests/cold_tier_statistics_test.rs` |
| Integration — listing-time pruning | `crates/cayenne/tests/cold_tier_pruning_test.rs` |
| Integration — promotion trigger | `crates/cayenne/tests/cold_tier_trigger_test.rs` |
| Property / fuzz | `crates/cayenne/tests/mutation_property_test.rs` (`prop_sequential_cold`, `prop_concurrent_cold_sqlite`) |
| Unit — promotion classification | `crates/cayenne/src/provider/cold_partition.rs` |
| Unit — GC mark/sweep, file sizing | `crates/cayenne/src/provider/table.rs` |
| Unit — PK bloom round-trip | `crates/cayenne/src/provider/pk_index.rs`, `crates/cayenne/src/cayenne_catalog.rs` |
| Unit — config validation | `crates/runtime/src/dataaccelerator/cayenne/mod.rs` |
| End-to-end on real S3 | `crates/runtime/tests/cayenne/mod.rs` (`test_cayenne_datalake_tier_e2e`) |
| Macro benchmark | `test/spicepods/chbench/accelerated/*-cold-*.yaml` (SF10, SF1000) |

Every `crates/cayenne` test runs in PR CI against both metastore backends
(`sqlite`, `turso`) via `test_with_backends!`. The S3 end-to-end test runs in
`integration.yml`.

## Coverage matrix

| Area | Status | Covered by |
| --- | --- | --- |
| Cross-tier query correctness | Good | `cold_tier_test`: promotion + scan + delete, carry-forward, bake-vs-tombstone, concurrent scan during promotion |
| Randomised correctness | Good | `mutation_property_test`: `prop_sequential_cold`, `prop_concurrent_cold_sqlite` |
| Restart / reopen | Good | `cold_tier_test::…restart_reopen`, `Restart` fuzz op, `cold_gc_restart_resets_grace_never_premature` |
| GC of orphaned cold objects | Good | 3 unit tests (mark/sweep grace, restart, un-orphan) + `…gc_end_to_end` |
| Upsert / CDC after promotion | Good | `…upsert_after_promotion`, `…cdc_memory_upsert_after_promotion`, `…cdc_upserts_concurrent_with_promotion` |
| Cold file layout / rolling | Good | `cold_write_format_rolls_files_at_cold_target_size`, `cold_file_row_cap_stays_within_bloom_budget` |
| Bounded Z-order across runs | Good | `…bounded_zorder_multi_run_promotion` |
| **Table statistics across promotion** | **Was untested** | `cold_tier_statistics_test` |
| **Listing-time file pruning** | **Was untested** | `cold_tier_pruning_test` |
| **Promotion trigger thresholds** | **Was untested** | `cold_tier_trigger_test` |
| **Tier stays inert without a primary key** | **Was untested** | `cold_tier_trigger_test` |

### Why those four mattered

- **Statistics.** `ColdTierFile::row_count` and the maintained table count are
  what let a `COUNT(*)` be answered from metadata instead of a scan. Nothing
  asserted the count survived a warm→cold promotion, so a drift there would have
  produced a wrong query result, not merely a bad plan.
- **Pruning.** `cold_partition.rs` unit-tests the classification arithmetic and
  `cold_tier_test` asserts each cold file carries a statistics blob, but no test
  proved a selective query actually reads fewer cold files — the whole reason
  the blob is persisted.
- **Trigger thresholds.** `cayenne_datalake_warm_max_files` /
  `_warm_max_bytes` had validation tests (reject zero intervals, warn on unknown
  clustering column) but no test that the configured value is what fires a
  promotion.
- **PK-less inert.** Registration only *warns* when a datalake location is set on
  a table with no primary key; the tier then silently does nothing. Nothing
  asserted that "nothing" is correct — that no cold object is written and the
  data stays queryable.

## Defect found by this coverage

`cold_tier_statistics_test::test_cold_tier_statistics_follow_a_folded_delete` is
`#[ignore]`d as a marker for
[#12846](https://github.com/spiceai/spiceai/issues/12846), not a flaky test.

A standalone `DELETE` never decrements the maintained row count and never taints
`num_rows_exact`; the stale value is hidden only while the tombstone is pending.
A promotion folds the tombstone without re-baselining the count, so the table
reports `Exact(110)` while the cold manifest correctly holds 109 rows.

The rule is general, not datalake-specific — compaction and overwrite both
`Set` the count, the seq-prefix bake does not, and an upsert table re-enters the
exact window after every `Set`. See the issue for the full analysis.

## Known remaining gaps

- No micro-benchmark exercises the real datalake path. Every `crates/cayenne`
  bench with `cold` in its name means a cold *cache/session*, not this tier.
  Promotion throughput and cold-scan latency are only visible through chbench.
- One end-to-end test against real object storage, covering the append path on a
  25-row table. No restart, CDC, or GC coverage over real S3.
- `prop_concurrent_cold` is sqlite-only, and drives promotion explicitly rather
  than through the background promoter under load.
