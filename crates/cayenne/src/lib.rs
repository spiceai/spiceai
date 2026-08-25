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

//! Cayenne: a lakehouse format using `SQLite` (or Turso) for transactional metadata
//! and Vortex files as the data lake.
//!
//! This module provides a lakehouse format that combines:
//! - `SQLite` or Turso for transactional metadata management (schemas, tables, files,
//!   inline-data memtable, deletion vectors, snapshot sequences)
//! - Vortex files for efficient columnar data storage
//!
//! # Architecture
//!
//! Cayenne has three key components:
//! - **Metadata catalog**: `SQLite`/Turso database storing table metadata, file
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
pub mod logical_optimizer;
pub mod maintained_aggregate;
pub mod metadata;
pub mod metastore;

/// Z-order clustering kernel, re-exported for benchmarks only. Not a stable API.
#[doc(hidden)]
pub mod __bench_zorder {
    pub use crate::provider::zorder::zorder_keys;
}
pub mod optimizer_rules;
#[cfg(feature = "partition-table-provider")]
pub(crate) mod partition_creator;
pub(crate) mod partition_naming;
pub mod provider;
pub(crate) mod resource_starvation;
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
pub use metadata::{
    CdcDurability, DataFile, DeleteFile, InlinedData, InlinedDataStats, InlinedDelete,
    ObjectStoreConfig, PartitionMetadata, StorageClass, TableMetadata, TableStatistics,
};
pub use metastore::sqlite::{SqliteAutoVacuum, SqliteMetastoreConfig, set_sqlite_metastore_config};
#[cfg(feature = "partition-table-provider")]
pub use partition_creator::CayennePartitionCreator;
pub use provider::constants::{STAGING_DIR_NAME, STAGING_WAL_FILENAME, STAGING_WAL_TMP_FILENAME};
pub use provider::{
    CayenneCdcWrite, CayenneContext, CayenneStagedAppend, CayenneStagedUpsert,
    CayenneTableProvider, CayenneTableProviderBuilder, CayenneTransaction, EncodeBudgetSnapshot,
    LastSmallFileCompactPath, PARTITIONED_WAL_DIR, PartitionedWal, PartitionedWalEntry,
    PreparedOverwrite, PreparedStagedAppend, PreparedTxnCommit, QueryObservations, SlotAdvancer,
    TimeRetentionFilterBuilder, TransactionCommit, TransactionWriteToken, TxnTable,
    begin_compaction_shutdown, cap_global_encode_concurrency, clear_global_mem_tier_pool_account,
    compaction_budget, compaction_budget_permits, deregister_query_observations,
    drain_compaction_tasks, encode_budget_snapshot, global_mem_tier_pool_account_bytes,
    global_mem_tier_total, global_mem_tier_used, global_qph, in_flight_compaction_tasks,
    record_global_query, record_query_latency, register_query_observations,
    release_global_mem_tier_bytes, reset_compaction_shutdown, set_compaction_runtime_env,
    set_compaction_runtime_handle, set_cpu_burstable, set_global_encode_concurrency,
    set_global_mem_tier_bytes, set_global_mem_tier_pool_account, set_global_memory_budget,
    set_global_pk_keyset_bytes, set_query_admission_governor, try_reserve_global_mem_tier_bytes,
    update_global_mem_tier_total,
};
pub use schema::{CAYENNE_TYPE_REWRITE_RULES, transform_schema_for_vortex};
