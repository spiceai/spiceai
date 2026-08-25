/*
Copyright 2026, Spice AI, Inc.

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

//! [`CayennePartitionCreator`] — implements [`PartitionCreator`] for Cayenne-backed
//! partitioned tables, creating and opening per-partition [`CayenneTableProvider`]s.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DFSchema;
use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers::UnsupportedTypeAction;
use runtime_table_partition::Partition;
use runtime_table_partition::creator::filename::{
    encode_composite_key, encode_key, parse_partition_value, to_hive_partition_dir,
};
use runtime_table_partition::creator::{self, PartitionCreator};
use runtime_table_partition::expression::PartitionedBy;
use snafu::ResultExt as _;

use crate::{
    CayenneContext, CayenneTableProviderBuilder, MetadataCatalog, PartitionMetadata,
    TimeRetentionFilterBuilder, metadata,
};

/// Implements [`PartitionCreator`] for Cayenne-backed partitioned tables.
///
/// Creates and opens per-partition [`CayenneTableProvider`]s rooted at
/// Hive-style subdirectories under `base_path`.
///
/// Two callers construct this: `CREATE TABLE … PARTITIONED BY` in
/// [`crate::ddl::operations`], and the Cayenne accelerator in `runtime`. Both
/// run their partitions' interval compaction through the process-wide budget
/// ([`Self::with_background_compaction`]); they differ only in that the
/// accelerator's tables are targets for the accelerated dual-write path
/// ([`Self::with_direct_partition_writes`]).
pub struct CayennePartitionCreator {
    table_name: String,
    base_path: PathBuf,
    partition_by: Vec<PartitionedBy>,
    schema: SchemaRef,
    catalog: Arc<dyn MetadataCatalog>,
    table_id: String,
    unsupported_type_action: UnsupportedTypeAction,
    retention_filters: Vec<Expr>,
    time_retention_filter_builder: Option<TimeRetentionFilterBuilder>,
    vortex_config: metadata::VortexConfig,
    object_store_config: Option<metadata::ObjectStoreConfig>,
    primary_key: Vec<String>,
    on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
    /// Shared context (footer/segment caches) created once, shared across all partitions.
    context: Arc<CayenneContext>,
    /// Compaction budget shared with the creating engine, if it runs one. Every
    /// partition provider spawns its background compaction task through this
    /// semaphore, so the whole table shares one concurrency budget. `None`
    /// leaves partitions without an interval compactor; they still compact on
    /// write through `schedule_post_write_compaction`.
    compaction_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    /// See [`PartitionCreator::accepts_direct_partition_writes`].
    accepts_direct_partition_writes: bool,
}

impl std::fmt::Debug for CayennePartitionCreator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayennePartitionCreator")
            .field("table_name", &self.table_name)
            .field("base_path", &self.base_path)
            .field("partition_by", &self.partition_by)
            .field("schema", &self.schema)
            .field("catalog", &"<dyn MetadataCatalog>")
            .field("table_id", &self.table_id)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .field("retention_filters", &self.retention_filters.len())
            .field(
                "time_retention_filter_builder",
                &self.time_retention_filter_builder.is_some(),
            )
            .field("vortex_config", &"<VortexConfig>")
            .field("object_store_config", &self.object_store_config.is_some())
            .field("primary_key", &self.primary_key)
            .field("on_conflict", &self.on_conflict.is_some())
            .field("context", &"<CayenneContext>")
            .field("compaction_semaphore", &self.compaction_semaphore.is_some())
            .field(
                "accepts_direct_partition_writes",
                &self.accepts_direct_partition_writes,
            )
            .finish_non_exhaustive()
    }
}

impl CayennePartitionCreator {
    /// Create a partition creator that runs no interval compaction and is not a
    /// dual-write target. Both engines that open Cayenne tables opt into a
    /// compaction budget with [`Self::with_background_compaction`]; only the
    /// accelerator opts into [`Self::with_direct_partition_writes`].
    #[expect(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        table_name: String,
        base_path: PathBuf,
        partition_by: Vec<PartitionedBy>,
        schema: SchemaRef,
        catalog: Arc<dyn MetadataCatalog>,
        table_id: String,
        unsupported_type_action: UnsupportedTypeAction,
        retention_filters: Vec<Expr>,
        time_retention_filter_builder: Option<TimeRetentionFilterBuilder>,
        vortex_config: metadata::VortexConfig,
        object_store_config: Option<metadata::ObjectStoreConfig>,
        primary_key: Vec<String>,
        on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Self {
        let context =
            CayenneContext::new_for_partition_child(&vortex_config, runtime_env, &table_name);
        Self {
            table_name,
            base_path,
            partition_by,
            schema,
            catalog,
            table_id,
            unsupported_type_action,
            retention_filters,
            time_retention_filter_builder,
            vortex_config,
            object_store_config,
            primary_key,
            on_conflict,
            context,
            compaction_semaphore: None,
            accepts_direct_partition_writes: false,
        }
    }

    /// Run each partition's background compaction through `semaphore`, so every
    /// partition of this table draws on one shared concurrency budget.
    #[must_use]
    pub fn with_background_compaction(mut self, semaphore: Arc<tokio::sync::Semaphore>) -> Self {
        self.compaction_semaphore = Some(semaphore);
        self
    }

    /// Accept writes addressed to the partition table itself, making this table
    /// a target for the accelerated dual-write path. See
    /// [`PartitionCreator::accepts_direct_partition_writes`].
    #[must_use]
    pub fn with_direct_partition_writes(mut self) -> Self {
        self.accepts_direct_partition_writes = true;
        self
    }

    /// Wire a freshly opened partition provider into the shared caches and, when
    /// the creating engine runs one, the shared compaction budget.
    fn init_partition_provider(&self, provider: &Arc<crate::CayenneTableProvider>) {
        if let Some(semaphore) = &self.compaction_semaphore {
            provider.spawn_background_compaction(Arc::clone(semaphore));
        }
        // Wire the demand scan-view cache (weak self-ref for spawn_blocking builds +
        // the idle evictor) so a partition provider offloads its builds and releases
        // idle cached views' pinned snapshot dirs, like the top-level provider.
        provider.init_scan_view_cache();
    }

    fn partition_column_labels(&self) -> Vec<String> {
        self.partition_by
            .iter()
            .map(|p| match &p.expression {
                Expr::Column(col) => col.name.clone(),
                _ => p.name.clone(),
            })
            .collect()
    }

    fn partition_table_name(&self, partition_key: &str) -> String {
        crate::partition_naming::partition_child_table_name(&self.table_name, partition_key)
    }

    fn legacy_partition_table_name(&self, partition_values: &[String]) -> String {
        crate::partition_naming::legacy_partition_child_table_name(
            &self.table_name,
            partition_values,
        )
    }

    fn partition_dir(&self, partition_values: &[ScalarValue]) -> Result<PathBuf, creator::Error> {
        let pairings: Vec<(PartitionedBy, ScalarValue)> = self
            .partition_by
            .iter()
            .cloned()
            .zip(partition_values.iter().cloned())
            .collect();
        let partition_dir = to_hive_partition_dir(&pairings)
            .boxed()
            .context(creator::CreatePartitionSnafu)?;
        Ok(self.base_path.join(partition_dir))
    }
}

#[async_trait]
impl PartitionCreator for CayennePartitionCreator {
    /// Cayenne owns its partition storage, so a partitioned Cayenne table can be
    /// written to directly — but only the accelerator asks for that, via
    /// [`Self::with_direct_partition_writes`]. A table created through
    /// `CREATE TABLE … PARTITIONED BY` is not reachable from the dual-write path
    /// at all (it is a catalog table, never an accelerator), so it keeps the
    /// trait's conservative default.
    fn accepts_direct_partition_writes(&self) -> bool {
        self.accepts_direct_partition_writes
    }

    async fn create_partition(
        &self,
        partition_values: Vec<ScalarValue>,
    ) -> Result<Partition, creator::Error> {
        if partition_values.is_empty() {
            return Err(creator::Error::CreatePartition {
                source: "At least one partition value is required".into(),
            });
        }
        if partition_values.len() != self.partition_by.len() {
            return Err(creator::Error::CreatePartition {
                source: format!(
                    "Expected {} partition values but got {} (one per partition_by expression)",
                    self.partition_by.len(),
                    partition_values.len()
                )
                .into(),
            });
        }

        let partition_dir = self.partition_dir(&partition_values)?;
        let partition_path = partition_dir.to_string_lossy().to_string();

        let partition_value_strings: Vec<String> = partition_values
            .iter()
            .map(encode_key)
            .collect::<Result<Vec<_>, _>>()
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        tracing::debug!("creating Cayenne partition at {partition_path}");
        tokio::fs::create_dir_all(&partition_dir)
            .await
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        // For local FS, sync the parent (table base_path) after creating a new
        // partition sub-directory so its directory entry is durable before we
        // record the partition in the catalog via add_partition. This follows
        // the same uniform contract as snapshot directories, _partitioned_wal/,
        // deletions/ subdirs, and initial table creation.
        if self.object_store_config.is_none()
            && let Some(parent) = partition_dir.parent()
        {
            let parent = parent.to_path_buf();
            let parent_display = parent.display().to_string();
            match tokio::task::spawn_blocking(move || {
                std::fs::File::open(&parent).and_then(|f| f.sync_all())
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(
                    "Failed to sync Cayenne partition parent directory {parent_display}: {error}"
                ),
                Err(error) => tracing::warn!(
                    "Failed to join Cayenne partition parent directory sync task for {parent_display}: {error}"
                ),
            }
        }

        let partition_column_names = self.partition_column_labels();
        let partition_key = encode_composite_key(&partition_values)
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        let partition_metadata = PartitionMetadata::new_composite(
            self.table_id.clone(),
            partition_column_names,
            partition_value_strings.clone(),
            partition_path.clone(),
            false,
        );
        self.catalog
            .add_partition(partition_metadata)
            .await
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        let table_options = metadata::CreateTableOptions {
            table_name: self.partition_table_name(&partition_key),
            schema: Arc::clone(&self.schema),
            primary_key: self.primary_key.clone(),
            on_conflict: self.on_conflict.clone(),
            base_path: partition_path.clone(),
            partition_column: None,
            vortex_config: self.vortex_config.clone(),
        };

        let mut builder = CayenneTableProviderBuilder::new(
            Arc::clone(&self.catalog),
            Arc::clone(self.context.runtime_env()),
        )
        .with_context(Arc::clone(&self.context))
        .with_retention_filters(self.retention_filters.clone());
        if let Some(ref rb) = self.time_retention_filter_builder {
            builder = builder.with_time_retention_filter_builder(rb.clone());
        }
        if let Some(ref os) = self.object_store_config {
            builder = builder.with_object_store(os.clone());
        }
        let cayenne_table = builder
            .create(table_options)
            .await
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        let table_provider = Arc::new(cayenne_table);
        self.init_partition_provider(&table_provider);
        Ok(Partition {
            partition_values,
            table_provider,
        })
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, creator::Error> {
        let partitions = self
            .catalog
            .get_partitions(&self.table_id)
            .await
            .boxed()
            .context(creator::InferringPartitionsSnafu)?;

        let mut result = Vec::new();
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))
            .boxed()
            .context(creator::InferringPartitionsSnafu)?;
        let expected_partition_columns = self.partition_column_labels();

        for partition_meta in partitions {
            if partition_meta.partition_columns != expected_partition_columns {
                return Err(creator::Error::PartitionByExpressionsChanged);
            }

            let mut partition_values = Vec::with_capacity(self.partition_by.len());
            for (partition_expr, value_str) in self
                .partition_by
                .iter()
                .zip(&partition_meta.partition_values)
            {
                let partition_value = parse_partition_value(&df_schema, partition_expr, value_str)
                    .map_err(|e| creator::Error::InferringPartitions {
                        source: Box::new(e),
                    })?;
                partition_values.push(partition_value);
            }

            let partition_key = partition_meta.composite_key();
            let partition_table_name = self.partition_table_name(&partition_key);

            let mut builder = CayenneTableProviderBuilder::new(
                Arc::clone(&self.catalog),
                Arc::clone(self.context.runtime_env()),
            )
            .with_context(Arc::clone(&self.context))
            .with_retention_filters(self.retention_filters.clone());
            if let Some(ref rb) = self.time_retention_filter_builder {
                builder = builder.with_time_retention_filter_builder(rb.clone());
            }
            if let Some(ref os) = self.object_store_config {
                builder = builder.with_object_store(os.clone());
            }
            let cayenne_table = match builder.open(&partition_table_name).await {
                Ok(table) => table,
                Err(crate::provider::Error::Catalog {
                    source: crate::catalog::CatalogError::TableNotFound { .. },
                }) => {
                    let legacy_name =
                        self.legacy_partition_table_name(&partition_meta.partition_values);
                    let mut legacy_builder = CayenneTableProviderBuilder::new(
                        Arc::clone(&self.catalog),
                        Arc::clone(self.context.runtime_env()),
                    )
                    .with_context(Arc::clone(&self.context))
                    .with_retention_filters(self.retention_filters.clone());
                    if let Some(ref rb) = self.time_retention_filter_builder {
                        legacy_builder =
                            legacy_builder.with_time_retention_filter_builder(rb.clone());
                    }
                    if let Some(ref os) = self.object_store_config {
                        legacy_builder = legacy_builder.with_object_store(os.clone());
                    }
                    legacy_builder
                        .open(&legacy_name)
                        .await
                        .boxed()
                        .context(creator::InferringPartitionsSnafu)?
                }
                Err(error) => {
                    return Err(creator::Error::InferringPartitions {
                        source: Box::new(error),
                    });
                }
            };

            let table_provider = Arc::new(cayenne_table);
            self.init_partition_provider(&table_provider);
            result.push(Partition {
                partition_values,
                table_provider,
            });
        }

        Ok(result)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let partition_columns: std::collections::HashSet<_> = self
            .partition_by
            .iter()
            .flat_map(|p| p.expression.column_refs())
            .collect();

        Ok(filters
            .iter()
            .map(|filter| {
                let filter_columns = filter.column_refs();
                let matches = filter_columns.is_empty()
                    || filter_columns
                        .iter()
                        .all(|fc| partition_columns.iter().any(|pc| fc.name == pc.name));
                if matches {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::execution::context::SessionContext;
    use datafusion::prelude::col;
    use datafusion::scalar::ScalarValue;
    use tempfile::TempDir;

    use crate::metadata::{CreateTableOptions, VortexConfig};
    use crate::{CayenneCatalog, CayenneTableProvider};
    use arrow::datatypes::{DataType, Field, Schema};

    const TABLE: &str = "partitioned_events";

    struct Fixture {
        catalog: Arc<dyn MetadataCatalog>,
        table_id: String,
        schema: SchemaRef,
        base_path: PathBuf,
        runtime_env: Arc<RuntimeEnv>,
        _tmp: TempDir,
    }

    /// A parent table registered in a sqlite metastore, partitioned by the
    /// `bucket` column, with its data rooted in a temp dir.
    async fn fixture() -> Fixture {
        let tmp = TempDir::new().expect("tempdir");
        let base_path = tmp.path().join(TABLE);
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("bucket", DataType::Utf8, false),
        ]));

        let catalog: Arc<dyn MetadataCatalog> = Arc::new(
            CayenneCatalog::new(format!("sqlite://{}", tmp.path().join("meta.db").display()))
                .expect("catalog opens"),
        );
        catalog.init().await.expect("catalog schema initializes");

        let table_id = catalog
            .create_table(CreateTableOptions {
                table_name: TABLE.to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: base_path.to_string_lossy().to_string(),
                partition_column: Some("bucket".to_string()),
                vortex_config: VortexConfig::default(),
            })
            .await
            .expect("catalog create_table");

        Fixture {
            catalog,
            table_id,
            schema,
            base_path,
            runtime_env: SessionContext::new().runtime_env(),
            _tmp: tmp,
        }
    }

    fn creator_for(fixture: &Fixture) -> CayennePartitionCreator {
        CayennePartitionCreator::new(
            TABLE.to_string(),
            fixture.base_path.clone(),
            vec![PartitionedBy {
                name: "bucket".to_string(),
                expression: col("bucket"),
            }],
            Arc::clone(&fixture.schema),
            Arc::clone(&fixture.catalog),
            fixture.table_id.clone(),
            UnsupportedTypeAction::Error,
            Vec::new(),
            None,
            crate::metadata::VortexConfig::default(),
            None,
            Vec::new(),
            None,
            Arc::clone(&fixture.runtime_env),
        )
    }

    /// A table registered in the same metastore as `fixture`'s, rooted outside
    /// its data directory — the shape an operator gets by accelerating a second
    /// dataset into one metastore.
    async fn unrelated_table(fixture: &Fixture, table_name: &str, dir: &str) {
        fixture
            .catalog
            .create_table(CreateTableOptions {
                table_name: table_name.to_string(),
                schema: Arc::clone(&fixture.schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture
                    .base_path
                    .with_file_name(dir)
                    .to_string_lossy()
                    .to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            })
            .await
            .unwrap_or_else(|error| panic!("the table {table_name} is created: {error}"));
    }

    fn bucket(value: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(value.to_string()))
    }

    /// The `CREATE TABLE … PARTITIONED BY` path is not reachable from the
    /// accelerated dual-write path (it produces a catalog table, never an
    /// accelerator), so it keeps the trait's conservative default; the
    /// accelerator opts in explicitly.
    #[tokio::test]
    async fn only_the_accelerator_opts_into_direct_partition_writes() {
        let fixture = fixture().await;

        assert!(
            !creator_for(&fixture).accepts_direct_partition_writes(),
            "a DDL-created partitioned table must not be a dual-write target"
        );
        assert!(
            creator_for(&fixture)
                .with_direct_partition_writes()
                .accepts_direct_partition_writes(),
            "the accelerator opts in, so its partitions must be dual-write targets"
        );
        assert!(
            !creator_for(&fixture)
                .with_background_compaction(Arc::new(tokio::sync::Semaphore::new(1)))
                .accepts_direct_partition_writes(),
            "sharing a compaction budget must not imply accepting direct writes"
        );
    }

    fn assert_background_compaction(partitions: &[Partition], expected: bool, context: &str) {
        assert!(!partitions.is_empty(), "{context}: nothing to assert on");
        for partition in partitions {
            let provider = partition
                .table_provider
                .downcast_ref::<CayenneTableProvider>()
                .expect("a Cayenne partition is backed by a CayenneTableProvider");
            assert_eq!(
                provider.has_background_compactor(),
                expected,
                "{context}: background compaction must follow the shared budget"
            );
        }
    }

    /// Each partition provider joins the creating engine's compaction budget
    /// only when one was supplied — on the create path and on the reopen path
    /// alike. Without a budget a partition still compacts on write, but runs no
    /// interval compactor.
    #[tokio::test]
    async fn partitions_join_the_shared_compaction_budget_only_when_it_is_supplied() {
        let fixture = fixture().await;
        let plain = creator_for(&fixture);
        let shared = creator_for(&fixture)
            .with_background_compaction(Arc::new(tokio::sync::Semaphore::new(4)));

        let created_plain = plain
            .create_partition(vec![bucket("solo")])
            .await
            .expect("partition is created without a shared budget");
        assert_background_compaction(&[created_plain], false, "created without a budget");

        let created_shared = shared
            .create_partition(vec![bucket("shared")])
            .await
            .expect("partition is created with a shared budget");
        assert_background_compaction(&[created_shared], true, "created with a budget");

        // Reopening the same two partitions must make the same decision.
        let reopened_plain = plain
            .infer_existing_partitions()
            .await
            .expect("partitions are inferred without a shared budget");
        assert_background_compaction(&reopened_plain, false, "reopened without a budget");

        let reopened_shared = shared
            .infer_existing_partitions()
            .await
            .expect("partitions are inferred with a shared budget");
        assert_background_compaction(&reopened_shared, true, "reopened with a budget");
    }

    /// A created partition must be recoverable: the Hive-style directory exists
    /// on disk and re-opening the table reads back every partition value.
    #[tokio::test]
    async fn created_partitions_round_trip_through_inference() {
        let fixture = fixture().await;
        let creator = creator_for(&fixture);

        for value in ["alpha", "beta"] {
            creator
                .create_partition(vec![bucket(value)])
                .await
                .expect("partition is created");
        }

        // Partition values are encoded into the directory name, so assert the
        // shape and the count rather than a spelling the codec owns.
        let partition_dirs: Vec<String> = std::fs::read_dir(&fixture.base_path)
            .expect("the table directory is readable")
            .map(|entry| {
                entry
                    .expect("the directory entry is readable")
                    .file_name()
                    .to_string_lossy()
                    .to_string()
            })
            .filter(|name| name.starts_with("bucket="))
            .collect();
        assert_eq!(
            partition_dirs.len(),
            2,
            "one Hive-style directory per created partition, got {partition_dirs:?}"
        );

        let inferred = creator
            .infer_existing_partitions()
            .await
            .expect("partitions are inferred");
        let mut values: Vec<String> = inferred
            .iter()
            .map(|partition| match partition.partition_values.as_slice() {
                [ScalarValue::Utf8(Some(value))] => value.clone(),
                other => panic!("expected one Utf8 partition value, got {other:?}"),
            })
            .collect();
        values.sort();
        assert_eq!(values, vec!["alpha".to_string(), "beta".to_string()]);
    }

    /// One value per `partition_by` expression, no more and no fewer.
    #[tokio::test]
    async fn partition_values_must_match_the_partition_by_arity() {
        let fixture = fixture().await;
        let creator = creator_for(&fixture);

        let empty = creator
            .create_partition(vec![])
            .await
            .expect_err("no partition values must be rejected");
        assert!(
            empty.to_string().contains("At least one partition value"),
            "unexpected error for zero values: {empty}"
        );

        let too_many = creator
            .create_partition(vec![bucket("alpha"), bucket("beta")])
            .await
            .expect_err("surplus partition values must be rejected");
        assert!(
            too_many
                .to_string()
                .contains("Expected 1 partition values but got 2"),
            "unexpected error for surplus values: {too_many}"
        );
    }

    /// A partition value is hex-encoded into a single path component before it
    /// reaches the filesystem, so no character a user can write — a `#`, a path
    /// separator, a parent-directory reference — can escape or split the
    /// directory name. This is what makes every value legal on a local
    /// filesystem, and it is the property to keep if the encoding ever changes.
    #[tokio::test]
    async fn a_path_hostile_partition_value_becomes_one_safe_directory_component() {
        let fixture = fixture().await;
        let creator = creator_for(&fixture);

        let hostile = ["abcdef#123", "a/b", "..", "x=y", "with space"];
        for value in hostile {
            // The path the creator computes, before the filesystem sees it: a
            // value that kept a separator would nest below the table directory
            // rather than sit directly in it, and `read_dir` below could not
            // tell the difference — it only ever reports the first component.
            let partition_dir = creator
                .partition_dir(&[bucket(value)])
                .unwrap_or_else(|e| panic!("'{value}' must map to a partition directory: {e}"));
            assert_eq!(
                partition_dir.parent(),
                Some(fixture.base_path.as_path()),
                "'{value}' must name a direct child of the table directory, got {partition_dir:?}"
            );

            creator
                .create_partition(vec![bucket(value)])
                .await
                .unwrap_or_else(|e| panic!("'{value}' must be a legal partition value: {e}"));
        }

        let partition_dirs: Vec<String> = std::fs::read_dir(&fixture.base_path)
            .expect("the table directory is readable")
            .map(|entry| {
                entry
                    .expect("the directory entry is readable")
                    .file_name()
                    .to_string_lossy()
                    .to_string()
            })
            .filter(|name| name.starts_with("bucket="))
            .collect();
        assert_eq!(
            partition_dirs.len(),
            hostile.len(),
            "one directory per created partition, got {partition_dirs:?}"
        );
        for name in &partition_dirs {
            let encoded = name
                .strip_prefix("bucket=")
                .expect("the directory name is Hive-style");
            assert!(
                encoded
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_'),
                "'{name}' must be a single path-safe component"
            );
        }

        let mut values: Vec<String> = creator
            .infer_existing_partitions()
            .await
            .expect("partitions are inferred")
            .iter()
            .map(|partition| match partition.partition_values.as_slice() {
                [ScalarValue::Utf8(Some(value))] => value.clone(),
                other => panic!("expected one Utf8 partition value, got {other:?}"),
            })
            .collect();
        values.sort();
        let mut expected: Vec<String> = hostile.iter().map(ToString::to_string).collect();
        expected.sort();
        assert_eq!(
            values, expected,
            "every encoded value must read back exactly as written"
        );
    }

    /// Dropping a partitioned table must drop its per-partition child tables
    /// with it. A surviving child keeps its own stored schema and file manifest,
    /// so a later recreate of the parent silently reattaches to the old schema
    /// and to a manifest whose data files were deleted (#12999).
    #[tokio::test]
    async fn dropping_the_parent_drops_its_partition_child_tables() {
        let fixture = fixture().await;
        let creator = creator_for(&fixture);
        for value in ["a", "b"] {
            creator
                .create_partition(vec![bucket(value)])
                .await
                .unwrap_or_else(|error| panic!("partition {value} is created: {error}"));
        }

        let before = fixture
            .catalog
            .list_table_names()
            .await
            .expect("table names are listed before the drop");
        let children_before = before.iter().filter(|name| name.as_str() != TABLE).count();
        assert_eq!(
            children_before, 2,
            "the fixture must actually register a child table per partition, got {before:?}"
        );

        assert!(
            fixture
                .catalog
                .drop_table(TABLE)
                .await
                .expect("the parent drops"),
            "the parent table existed, so the drop reports it was dropped"
        );

        let after = fixture
            .catalog
            .list_table_names()
            .await
            .expect("table names are listed after the drop");
        assert!(
            after.is_empty(),
            "no table may outlive the parent drop, found {after:?}"
        );
    }

    /// An unpartitioned table has no `cayenne_partition` rows, so the cascade
    /// must be a no-op rather than matching a same-prefixed sibling table.
    #[tokio::test]
    async fn dropping_a_table_leaves_an_unrelated_same_prefix_table_alone() {
        let fixture = fixture().await;
        let sibling = format!("{TABLE}_p0000");
        unrelated_table(&fixture, &sibling, "sibling").await;

        assert!(
            fixture
                .catalog
                .drop_table(TABLE)
                .await
                .expect("the parent drops")
        );

        let after = fixture
            .catalog
            .list_table_names()
            .await
            .expect("table names are listed after the drop");
        assert_eq!(
            after,
            vec![sibling],
            "a table that is not a partition of the dropped table must survive"
        );
    }

    /// The legacy child-name convention (`{parent}_{values}`) can also spell a
    /// table an operator accelerated separately into the same metastore —
    /// partitioning `events` by year spells `events_2024`. Dropping the parent
    /// must not take that table with it, so a name match only counts when the row
    /// is rooted at the partition's own directory.
    #[tokio::test]
    async fn a_table_colliding_with_the_legacy_partition_name_is_not_dropped() {
        let fixture = fixture().await;
        let creator = creator_for(&fixture);
        creator
            .create_partition(vec![bucket("a")])
            .await
            .expect("partition a is created");

        // Exactly the legacy name this partition would carry, rooted elsewhere.
        // Derived from the catalog's own partition row rather than spelled by
        // hand: the recorded values are encoded, so a hand-written name would
        // collide with nothing and the test would pass without exercising the
        // guard at all.
        let partitions = fixture
            .catalog
            .get_partitions(&fixture.table_id)
            .await
            .expect("the partition is recorded");
        let [partition] = partitions.as_slice() else {
            panic!("expected exactly one partition, got {partitions:?}");
        };
        let impostor = crate::partition_naming::legacy_partition_child_table_name(
            TABLE,
            &partition.partition_values,
        );
        unrelated_table(&fixture, &impostor, "unrelated").await;

        assert!(
            fixture
                .catalog
                .drop_table(TABLE)
                .await
                .expect("the parent drops")
        );

        let after = fixture
            .catalog
            .list_table_names()
            .await
            .expect("table names are listed after the drop");
        assert_eq!(
            after,
            vec![impostor],
            "the real partition child must go and the name-alike must stay"
        );
    }
}
