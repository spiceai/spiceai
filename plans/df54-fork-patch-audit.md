# DataFusion fork patch audit — spiceai-53 → sgrebnov/spiceai-54

**Goal:** track every logical spiceai patch to `spiceai/datafusion` and its status in the DF54 fork (`sgrebnov/spiceai-54` @ `b8ad9dc`), which the DF54 upgrade PR (spiceai/spiceai#11360) depends on.

## Root cause (proven)
DF54 fork built via `Merge tag '54.0.0' into spiceai` (`7cbaf7d58`). The merge resolved conflicts in upstream-rewritten files **in favor of upstream**, silently reverting spiceai patches. Verified on `unparser/dialect.rs`: spiceai parent `facf8d01e`=14 occurrences of `supports_subquery_in_join_predicate`; upstream parent `45d943dfb`=0; merge result=0. The post-merge "restore" pass (#159/#162-style) was **incomplete**.

- Inventory: 68 logical patches in `spiceai-53` over upstream `53.1.0` (`eae7bf4fa`), audited by content-presence in the local `b8ad9dc` checkout.
- Upstream-54 base: `45d943dfb8699dc9cb9ef2320e955b73e3e6c03b`.

## ⚠️ CONFIRMED REGRESSIONS — restore on `sgrebnov/spiceai-54` (then bump rev + re-add any spiceai-repo overrides)

| priority | sha (PR) | logical change | status | impact / action |
|---|---|---|---|---|
| **P1** | `65880c7c2` (#121) | Safe `.get()`/defaults for column-statistics access | **PARTIAL — PANIC RISK** | Survived in `datasource/mod.rs` + `aggregates/mod.rs`, but **reverted to panicking `column_statistics[idx]` in `physical-plan/src/filter.rs:416/418` and `joins/utils.rs:479/480`**; 2 regression tests gone. Re-port the safe indexing (panic-on-OOB in filter constant-folding + join cardinality). |
| **P1** | `f9e35b561` (#151) | `supports_subquery_in_join_predicate` dialect flag | **LOST (mechanism replaced)** | q6 federation fix. Merge replaced it with `split_join_on_and_where_filters` (moves inner-join filters to WHERE *unconditionally*, dialect-agnostic; **silently folds outer-join filters into ON instead of erroring**). Helpers `partition_subquery_filters`/`expr_contains_subquery` survive as **dead code**. Restore dialect flag (dialect.rs+plan.rs+utils.rs) + re-add `spiceai.rs` override. **q6 may be approximated — validate empirically.** |
| **P2** | `497da28ec` (#144) | BigQuery `timestamp_with_tz_to_string` override | **LOST** | BigQuery timestamp-with-tz unparsing reverts to RFC3339 default; correct BigQuery literal format (`%Y-%m-%d %H:%M:%S%:z`) gone. `unparser/dialect.rs` `BigQueryDialect`. |
| **P2** | `cf5214db6` (#111) | Make `EarlyStoppingStream` `pub` (+ re-export) | **LOST (latent)** | Reverted to `pub(super)`, no `pub use`. Latent (runtime build passes today). Re-widen + re-export in `datasource-parquet`. |
| **P3** | `cdccd2533` (#149) | Filter-node column-alias rewriting | **PARTIAL (tests)** | Production code present (plan.rs+utils.rs); all **4 regression tests dropped** from `plan_to_sql.rs`. Re-add tests. |
| **verify** | `757c891b9` (#135) | Lazy consumer-side `BatchCoalescer` | **LOST/OBSOLETE** | Upstream rewrote RepartitionExec to producer-side `SharedCoalescer`. Likely obsolete, but the LargeUtf8/Utf8 schema-mismatch it guarded could recur — regression-check. |
| **verify** | `7bbfa5a17` (#143), `f48975e0e` | Nested-metadata-ref panic guard; empty-projection metadata cols | **PARTIAL (no named replacement test)** | Metadata subsystem rewritten (see below); these specific edge-case guards + tests have no named equivalent — verify `upper(_location)` and None-projection paths don't panic / drop cols. |

## Metadata-column projection cluster — SUPERSEDED, validate (not simple restores)
Patches `adea254e2, f32727fbb, 633900a07, 9d4c3e4c6, 0486f2693, 14c2cbeda, f3e15e0d2, 1a49882f0, 19572a274, d599c7272` (ExtendedColumnProjector, `projected_metadata_positions`, FileStream `col_projector`). Upstream DF54 turned `file_scan_config.rs`/`file_stream.rs` into module dirs and **rewrote the metadata subsystem** (`TableSchema::compute_table_schema` + `projection.rs::inject_metadata_columns_into_projection`). The old per-PR markers are gone, but the **feature was reimplemented** (restore `d599c7272`, PRESENT) with surviving tests (`test_eq_properties_metadata_*`, `test_try_pushdown_filters_*`). Public API (`MetadataColumn`, `with_metadata_cols`, `_location/_last_modified/_size`) intact. **Action: validate metadata-column output ordering + nested/empty-projection edge cases under the new model.**

## PRESENT (survived, no action) — spiceai patches
`#87/#88/#89` placeholder-type inference · `#98/#104/#123` UDTF named-args + `spice.parameter_name` · `c703d72e1` no-nested-alias · vortex (`2f3ca2cdb/c562fb2f1/5459c5280`, evolved to 0.74) · `#126` iterative BinaryExpr · `#107` schema-metadata serde · hash-join accumulator seam (`38b6c042c/#117/506dabefd`) · object-versioning (`#113/63f54bf4a/6f725f40f`) · `#160` AT TIME ZONE · `#146/#147/#148` BigQuery dialect overrides · `#52` dangling-identifier strip · `600dd0b1b` Truncate · `#167` LIMIT placeholder typing · `148ae5b75` project_statistics bounds guard · `06e9d850f` UDTF coercion fallback · `e10081577/2cf498548` Sort unparser guard · `bfd27b87f` `_`-prefixed metadata names · `f9443aba3/7fbc52975/2e4b04b4e` PushdownSort+projection fetch.

## UPSTREAMED (in apache 54 base, no action)
`[branch-5x]` backports: `#19855, #19877, #19856/#20539, #20146(#127), #21439, #19633, #19576, #19884`.

## OBSOLETE / intended
`59f0d1d87` (old lockfile), `6930f5200`+`f404e6acb` (table_partition_cols add+revert pair → net no-op).

---

## SEPARATE FINDING — behavioral unit-test failures revealed by the compile fix
CI "Build and Test" *runs* unit tests. Sergei's branch never compiled, so these never ran; fixing the compile revealed pre-existing DF54 behavior failures (NOT compile/my-edit regressions):
- `runtime-datafusion-udfs bucket::tests::*` (×4) — DF54 `create_hashes` output changed → `bucket()` partition assignments differ → expectations stale. **Data-consistency implication for existing bucketed/partitioned data — needs decision, not just test update.**
- `vortex-datafusion persistent::sink::*_write_partitioned` — likely same bucket/hashing change.
- `search index::vector_table::*_vector_scan_*` (×3) — DF54 scan behavior change.
- `cayenne optimizer_rules::native_semi_join_does_not_plant_dynamic_filter…` — asserts "DF does not push join dynamic filters through semi joins"; DF54 changed this.
- `cayenne provider::table::protected_snapshot_subset_compaction…` — "position-mode merge output file count out of bounds: 8".
- `runtime http::v1::nsql::*_snapshot` (×4) — `insta` snapshot mismatches (likely regen, if output change is acceptable).

These are DF54-upgrade test triage (snapshot regen vs expectation update vs real regression), independent of the fork-patch losses above.

## Restore sequencing
1. Restore P1/P2 fork patches onto `sgrebnov/spiceai-54` (spiceai/datafusion) → bump `datafusion`/`datafusion-*` rev in Cargo.toml → re-add `spiceai.rs` `supports_subquery_in_join_predicate` override.
2. Triage the behavioral test failures.
3. THEN benchmarks (so q6 + bucket + others don't pollute the trunk comparison).
