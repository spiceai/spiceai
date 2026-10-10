/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Every Cayenne integration test, linked into one binary.
//!
//! Each `tests/<name>.rs` is a module here rather than its own test target, so
//! the crate's dependencies are linked once instead of once per file. Files that
//! install a `#[global_allocator]` stay separate targets: a process has one
//! allocator, and they measure process-wide live bytes. So does the `chDB`
//! result-correctness lane, because `chDB` and `DuckDB` cannot both run in one
//! process and the `DuckDB` lanes are modules here.
//!
//! nextest runs every test in a process of its own, so a test here may change
//! process-global state as long as it calls `common::require_process_per_test`
//! first; under plain `cargo test` that call fails the test before it changes
//! anything its neighbours would see.

#[macro_use]
mod common;
#[path = "correctness/support/mod.rs"]
mod support;

mod acid_compliance_test;
mod adaptive_layout_test;
mod anti_join_sort_merge_null_aware_test;
mod catalog_concurrency_test;
mod catalog_selector_test;
mod cdc_compaction_delete_race_test;
mod checkpoint_write_sizing_test;
mod cold_tier_pruning_test;
mod cold_tier_statistics_test;
mod cold_tier_test;
mod cold_tier_trigger_test;
mod column_stats_test;
mod commit_overwrite_atomicity_test;
mod cross_partition_overwrite_test;
mod cte_materialization_query_test;
mod data_inlining_test;
mod datafusion_dynamic_filter_backports_test;
mod deletion_strategy_detection_test;
mod deletion_strategy_test;
mod deletion_test;
mod deletion_vector_bug_test;
mod deletion_vector_integration_test;
mod delta_encoding_test;
mod dynamic_filter_sharing_lineage_test;
mod file_based_retention_delete_test;
mod file_pruning_test;
mod file_scan_statistics_source_test;
mod filtered_delete_scan_test;
mod fixed_offset_timezone_test;
mod float_nan_predicate_test;
mod float_zero_predicate_test;
mod in_list_null_semantics_test;
mod incremental_stats_test;
mod ingest_fsync_regression_test;
mod inline_tombstone_reclaim_test;
mod inline_with_pending_deletions_test;
mod int64_pk_deletion_test;
mod integration_test;
mod keybased_deletion_test;
mod large_upsert_test;
mod layout_pruning_ab_test;
mod light_encode_roundtrip;
mod limit_underdelivery_key_deletion_regression_test;
mod lookup_index_budget_test;
mod lookup_index_composition_test;
mod lookup_index_lifecycle_test;
mod lookup_index_memory_mode_test;
mod lookup_index_test;
mod lookup_index_widening_test;
mod maintained_aggregate_filter_test;
mod maintained_aggregate_pushed_filter_test;
mod maintained_aggregate_serve_soundness_test;
mod max_column_count_test;
mod mem_tier_budget_fallback_upsert_test;
mod mem_tier_budget_sharded_release_test;
mod mem_tier_overwrite_retention_test;
mod memory_accounting_test;
mod memory_mode_cdc_delete_test;
mod memory_mode_dml_semantics_test;
mod multi_partition_test;
mod mutation_model_test;
mod mutation_property_test;
mod mutation_roundtrip_test;
mod null_equal_join_dynamic_filter_test;
mod on_conflict_edge_cases_test;
mod on_conflict_test;
mod orphan_dv_compaction_test;
mod overwrite_concurrent_scan_atomicity_test;
mod overwrite_resurrection_test;
mod p1_subset_path_test;
mod partition_chunking_test;
mod partition_pruning_test;
mod partitioned_overlay_test;
mod position_based_deletion_test;
mod position_mode_upsert_test;
mod predicate_delete_ram_tier_rows;
mod protected_snapshot_projection_test;
mod read_time_filtering_test;
mod result_correctness_census_test;
mod result_correctness_inventory_test;
#[cfg(feature = "result-correctness-duckdb")]
mod result_correctness_standalone_engines_test;
#[cfg(feature = "result-correctness-duckdb")]
mod result_correctness_vs_duckdb_test;
mod result_correctness_vs_sqlite_test;
mod retention_test;
mod scalar_function_pushdown_test;
mod scan_view_retention_test;
mod selective_join_pk_probe_test;
mod shared_metastore_concurrency_test;
mod small_file_compaction_position_mode_test;
mod small_files_compaction_test;
mod snapshot_cleanup_convergence_test;
mod sort_merge_join_filter_column_order_test;
mod sort_merge_join_limit_test;
mod sort_rewrite_test;
mod staged_append_test;
mod stats_edge_cases_test;
mod string_bounds_statistics_test;
mod timestamp_unit_roundtrip_test;
#[cfg(feature = "turso")]
mod turso_0_7_2_open_compat_test;
mod update_test;
mod upsert_with_pending_deletions_test;
mod vortex_compressible_test;
mod vortex_task_cancellation;
mod write_back_pk_shapes_test;

/// `autotests = false` means cargo compiles only the test targets `Cargo.toml`
/// names, so a new `tests/<name>.rs` that is neither a module here nor its own
/// `[[test]]` would build nothing and its tests would never run, silently.
#[test]
fn every_test_file_is_compiled() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::fs::read_to_string(dir.join("tests/integration.rs"))
        .expect("tests/integration.rs should be readable");
    let manifest =
        std::fs::read_to_string(dir.join("Cargo.toml")).expect("Cargo.toml should be readable");
    let mut missing: Vec<String> = std::fs::read_dir(dir.join("tests"))
        .expect("tests/ should be readable")
        .map(|entry| entry.expect("tests/ entry should be readable").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .filter_map(|path| Some(path.file_stem()?.to_str()?.to_string()))
        .filter(|stem| stem != "integration")
        .filter(|stem| {
            !root
                .lines()
                .any(|line| line.trim() == format!("mod {stem};"))
                && !manifest.contains(&format!("path = \"tests/{stem}.rs\""))
        })
        .collect();
    missing.sort();
    assert!(
        missing.is_empty(),
        "these files in crates/cayenne/tests are not compiled into any test binary: \
         {missing:?}. Add `mod <name>;` to tests/integration.rs, or a `[[test]]` \
         entry in crates/cayenne/Cargo.toml if the file needs its own process"
    );
}
