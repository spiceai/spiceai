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
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use datafusion::common::DFSchema;
use datafusion::common::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers::UnsupportedTypeAction;
use regex::Regex;
use runtime_table_partition::Partition;
use runtime_table_partition::creator::filename::{
    encode_key, parse_partition_value, to_hive_partition_dir,
};
use runtime_table_partition::creator::{self, PartitionCreator};
use runtime_table_partition::expression::PartitionedBy;
use snafu::ResultExt as _;

use crate::{
    CayenneContext, CayenneTableProviderBuilder, MetadataCatalog, PartitionMetadata,
    TimeRetentionFilterBuilder, metadata,
};

/// Partition values matching `.*#\d+` (e.g. `"abcdef#123"`) are only supported
/// on S3 Express One Zone locations, not on local filesystem paths.
static UNSUPPORTED_LOCAL_PARTITION_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| match Regex::new(r".*#\d+$") {
        Ok(compiled) => compiled,
        Err(e) => unreachable!("Unable to compile regexp: {e}"),
    });

/// Implements [`PartitionCreator`] for Cayenne-backed partitioned tables.
///
/// Creates and opens per-partition [`CayenneTableProvider`]s rooted at
/// Hive-style subdirectories under `base_path`.
pub(crate) struct CayennePartitionCreator {
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
            .finish()
    }
}

impl CayennePartitionCreator {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
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
        let context = CayenneContext::new(&vortex_config, runtime_env);
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
        }
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
        let safe_key = partition_key.replace('/', "_");
        format!("{}_{}", self.table_name, safe_key)
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
                    "Expected {} partition values but got {}",
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

        if self.object_store_config.is_none() {
            for value in &partition_value_strings {
                if UNSUPPORTED_LOCAL_PARTITION_PATTERN.is_match(value) {
                    return Err(creator::Error::CreatePartition {
                        source: format!(
                            "Partition value '{value}' is not supported for local filesystem. \
                             Values matching '*#<digits>' are only supported on S3 Express One Zone."
                        )
                        .into(),
                    });
                }
            }
        }

        tracing::debug!("creating Cayenne partition at {partition_path}");
        std::fs::create_dir_all(&partition_dir)
            .boxed()
            .context(creator::CreatePartitionSnafu)?;

        // Durability: fsync the parent table directory after creating a new
        // partition subdir. Without this, a crash after mkdir but before the
        // directory entry is on disk can make the partition path unreachable
        // even though catalog metadata was written and data files may have
        // been created inside it. This is the last create_dir_all + catalog
        // metadata record site in the Cayenne write surface; it completes the
        // uniform durability contract (matching snapshot dirs, staging/,
        // deletions/, _partitioned_wal/, catalog DB dir, etc.).
        //
        // Only relevant for local FS; on object stores (S3) directories are
        // virtual and the "create" is a no-op.
        if self.object_store_config.is_none() {
            let parent = self.base_path.clone();
            tokio::task::spawn_blocking(move || {
                if let Ok(dir) = std::fs::File::open(&parent) {
                    let _ = dir.sync_all();
                }
            })
            .await
            .ok();
        }

        let partition_column_names = self.partition_column_labels();
        let partition_key = partition_value_strings.join("/");

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

        Ok(Partition {
            partition_values,
            table_provider: Arc::new(cayenne_table),
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

            let partition_key = partition_meta.partition_values.join("/");
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
            let cayenne_table = builder
                .open(&partition_table_name)
                .await
                .boxed()
                .context(creator::InferringPartitionsSnafu)?;

            result.push(Partition {
                partition_values,
                table_provider: Arc::new(cayenne_table),
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
