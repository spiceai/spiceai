/*
Copyright 2025 The Spice.ai OSS Authors

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

#![deny(missing_docs)]

//! Cayenne: a lakehouse format using `SQLite` for transactional metadata
//! and Vortex files as the data lake.
//!
//! This module provides a lakehouse format that combines:
//! - `SQLite` for transactional metadata management (schemas, tables, files,
//!   inline-data memtable, deletion vectors, snapshot sequences)
//! - Vortex files for efficient columnar data storage
//!
//! # Architecture
//!
//! Cayenne has three key components:
//! - **Metadata catalog**: `SQLite` database storing table metadata, file
//!   references, deletion vectors, and the inline-data level-0 memtable.
//! - **Data lake**: directory of Vortex files containing the persistent data.
//! - **Staging WAL**: crash-safe write-ahead log for in-progress appends.
//!
//! # Virtual Files Concept
//!
//! An initial design principle in Cayenne is that "files" are **virtual files** - they are not
//! single physical files, but rather Vortex `ListingTables` at unique directories. Each
//! `DataFile` entry in the catalog represents:
//!
//! - A unique directory path (e.g., `table_dir/file_000001/`)
//! - A Vortex `ListingTable` that manages Vortex files within that directory
//! - Metadata (row count, size) cached from the `ListingTable`'s statistics
//!
//! **Operations delegate to `ListingTables`:**
//! - **Reading a file**: Query the `ListingTable` at that directory
//! - **Appending to a file**: Write via the `ListingTable` (creates new Vortex files)
//! - **Deleting a file**: Delete the `ListingTable`'s directory
//! - **Getting stats**: Query the `ListingTable`'s statistics
//!
//! This design allows Cayenne to leverage Vortex's columnar format and `DataFusion`'s
//! `ListingTable` capabilities while maintaining transactional metadata in `SQLite`.
//!
//! # Core Concepts
//!
//! - **Tables**: Metadata about table schemas and structure
//! - **Data Files**: Metadata for virtual files (Vortex `ListingTables` at unique directories)

pub mod catalog;
pub mod catalog_provider;
pub mod cayenne_catalog;
#[cfg(feature = "partition-table-provider")]
pub mod ddl;
pub mod hll;
#[cfg(feature = "partition-table-provider")]
pub use ddl::CayenneDdlHandler;
pub(crate) mod bounded_fifo;
pub mod cte_materialization;
pub mod dataset_storage;
pub mod logical_optimizer;
pub mod maintained_aggregate;
pub mod metadata;
pub mod metastore;
pub mod metastore_layout;

/// Cold-tier clustering kernel, re-exported for benchmarks only. Not a stable API.
#[doc(hidden)]
pub mod __bench_clustering {
    pub use crate::provider::clustering::cluster_keys;
}

/// In-memory segment statistics pruning, re-exported for benchmarks only. Not a stable API.
#[doc(hidden)]
pub mod __bench_segment_pruning {
    pub use crate::provider::file_pruning::{
        build_listing_pruning_predicate, should_prune_statistics, statistics_from_record_batches,
        statistics_may_match,
    };
}

/// Returns whether Cayenne can encode `data_type` as a clustering dimension.
///
/// This is exposed for accelerator configuration validation so unsupported
/// columns fail dataset registration instead of silently producing a constant
/// clustering key.
#[doc(hidden)]
#[must_use]
pub fn is_clusterable_type(data_type: &arrow_schema::DataType) -> bool {
    provider::clustering::is_clusterable(data_type)
}
pub mod optimizer_rules;
#[cfg(feature = "partition-table-provider")]
pub(crate) mod partition_creator;
pub mod partition_naming;
pub mod provider;
pub(crate) mod resource_starvation;
/// Probe and build accounting for a table's secondary indexes (the
/// acceleration's `indexes`), so a check can prove a query used row selection
/// rather than silently falling back to an ordinary scan.
pub mod lookup_index {
    pub use crate::provider::IndexPersistence;
    pub use crate::provider::lookup_index::{LookupIndexCounters, LookupIndexVerification};
}

/// Per-file scan statistics accounting, so a check can prove concurrent cold
/// scans shared their footer reads and metastore writes. Not a stable API.
#[doc(hidden)]
pub mod scan_file_statistics {
    pub use crate::provider::ScanFileStatisticsCounters;
}

pub mod row_converter;
pub(crate) mod schema;
pub mod stats;
pub mod stats_aggregate;

pub use catalog::MetadataCatalog;
pub use catalog::{CatalogError, CatalogResult};
pub use catalog_provider::{
    CayenneCatalogProvider, CayenneCatalogProviderConfig, CayenneSchemaProvider,
};
pub use cayenne_catalog::{CayenneCatalog, is_retryable_write_conflict};
pub use cte_materialization::{
    CTE_SCAN_NODE_NAME, CayenneCteMaterialization, CayenneCteMaterializationPlanner,
    MATERIALIZED_CTE_NODE_NAME,
};
pub use metadata::{
    CdcDurability, DataFile, DeleteFile, InlinedData, InlinedDataStats, InlinedDelete,
    ObjectStoreConfig, PartitionMetadata, StorageClass, TableMetadata, TableStatistics,
};
pub use metastore::sqlite::{SqliteAutoVacuum, SqliteMetastoreConfig, set_sqlite_metastore_config};
#[cfg(feature = "partition-table-provider")]
pub use partition_creator::{CayennePartitionCreator, partition_column_labels};
pub use provider::RebuildableWrite;
pub use provider::constants::{
    LOOKUP_INDEX_DIR_NAME, STAGING_DIR_NAME, STAGING_WAL_FILENAME, STAGING_WAL_TMP_FILENAME,
};
pub use provider::{
    AutoKeyDeletesReason, AutoKeyTableKind, CayenneCdcWrite, CayenneContext, CayenneStagedAppend,
    CayenneStagedUpsert, CayenneTableProvider, CayenneTableProviderBuilder, CayenneTransaction,
    DeletionModeChoice, EncodeBudgetSnapshot, FileDeletionHold, LastSmallFileCompactPath,
    LookupIndexSnapshotHold, PARTITIONED_WAL_DIR, PartitionedWal, PartitionedWalEntry,
    PreparedOverwrite, PreparedStagedAppend, PreparedTxnCommit, QueryObservations, ScanViewReuse,
    SlotAdvancer, SnapshotDirPins, TableStorage, TimeRetentionFilterBuilder, TransactionCommit,
    TransactionWriteToken, TxnTable, auto_keeps_small_tables_compacted, auto_key_deletes_reason,
    begin_compaction_shutdown, cap_global_encode_concurrency, clear_global_mem_tier_pool_account,
    compaction_budget, compaction_budget_permits, deregister_query_observations,
    drain_compaction_tasks, encode_budget_snapshot, global_mem_tier_pool_account_bytes,
    global_mem_tier_total, global_mem_tier_used, global_pk_keyset_total, global_pk_keyset_used,
    global_qph, in_flight_compaction_tasks, record_global_query, record_query_latency,
    register_query_observations, release_global_mem_tier_bytes, reset_compaction_shutdown,
    set_compaction_runtime_env, set_compaction_runtime_handle, set_cpu_burstable,
    set_global_encode_concurrency, set_global_mem_tier_bytes, set_global_mem_tier_pool_account,
    set_global_memory_budget, set_global_pk_keyset_bytes, set_query_admission_governor,
    try_reserve_global_mem_tier_bytes, update_global_mem_tier_total,
};
pub use schema::{CAYENNE_TYPE_REWRITE_RULES, transform_schema_for_vortex};

/// Whether a table path names an object-store location (`s3://`, a local
/// directory addressed as `file://`, or a dataset's run-time URL `cayenne://`;
/// see [`dataset_storage`]), so every data operation goes through the table's
/// `ObjectStore` rather than the local filesystem.
#[must_use]
pub fn is_object_store_path(path: &str) -> bool {
    path.starts_with("s3://")
        || path.starts_with("file://")
        || path
            .strip_prefix(dataset_storage::DATASET_STORE_SCHEME)
            .is_some_and(|rest| rest.starts_with("://"))
}

/// `prefix/name` as an object path. [`object_store::path::Path`] keeps no trailing
/// `/`, so joining by string concatenation alone fuses the last segment of the
/// prefix with `name`; `Path::from` drops empty segments, so a leading `/` on
/// `name` or an empty prefix is harmless here.
#[must_use]
pub(crate) fn object_child(
    prefix: &object_store::path::Path,
    name: &str,
) -> object_store::path::Path {
    object_store::path::Path::from(format!("{}/{name}", prefix.as_ref()))
}

/// `location` relative to `prefix`, without the separating `/`; `None` when
/// `location` is not under `prefix` on a segment boundary.
#[must_use]
pub(crate) fn object_relative<'a>(
    prefix: &object_store::path::Path,
    location: &'a object_store::path::Path,
) -> Option<&'a str> {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return Some(location.as_ref());
    }
    location.as_ref().strip_prefix(prefix)?.strip_prefix('/')
}

#[cfg(test)]
mod object_key_tests {
    use super::{object_child, object_relative};
    use object_store::path::Path;

    /// The joins every former `format!("{prefix}{name}")` site now uses: an object
    /// path keeps no trailing `/`, so string concatenation fused the prefix's last
    /// segment with the name (`t/stagingwal.json` rather than `t/staging/wal.json`).
    #[test]
    fn a_child_key_has_exactly_one_separator() {
        let prefix = Path::from("data/orders/01a1/_staging");
        assert_eq!(
            object_child(&prefix, "wal.json").as_ref(),
            "data/orders/01a1/_staging/wal.json"
        );
        assert_eq!(
            object_child(&prefix, "sub/part.vortex").as_ref(),
            "data/orders/01a1/_staging/sub/part.vortex"
        );
        assert_eq!(
            object_child(&Path::from(""), "wal.json").as_ref(),
            "wal.json"
        );
    }

    /// The inverse: relative to a prefix only on a segment boundary, without the
    /// separating `/`.
    #[test]
    fn a_relative_key_strips_the_prefix_on_a_segment_boundary() {
        let prefix = Path::from("data/orders/01a1/snap");
        let inside = Path::from("data/orders/01a1/snap/part_0.vortex");
        let nested = Path::from("data/orders/01a1/snap/deletions/d.arrow");
        let sibling = Path::from("data/orders/01a1/snapshot/part_0.vortex");
        assert_eq!(object_relative(&prefix, &inside), Some("part_0.vortex"));
        assert_eq!(object_relative(&prefix, &nested), Some("deletions/d.arrow"));
        assert_eq!(object_relative(&prefix, &sibling), None);
        assert_eq!(object_relative(&prefix, &prefix), None);
        assert_eq!(
            object_relative(&Path::from(""), &inside),
            Some("data/orders/01a1/snap/part_0.vortex")
        );
        for name in ["part_0.vortex", "deletions/d.arrow"] {
            assert_eq!(
                object_relative(&prefix, &object_child(&prefix, name)),
                Some(name)
            );
        }
    }
}
