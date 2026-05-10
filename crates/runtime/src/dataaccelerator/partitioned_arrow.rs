/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use data_components::arrow::ArrowFactory;
use datafusion::{
    catalog::TableProviderFactory,
    common::Constraints,
    datasource::TableProvider,
    error::DataFusionError,
    execution::runtime_env::RuntimeEnv,
    logical_expr::{CreateExternalTable, TableProviderFilterPushDown},
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};
use runtime_table_partition::{
    Partition,
    creator::{CreatePartitionSnafu, Error as CreatorError, PartitionCreator},
    expression::PartitionedBy,
    provider::PartitionTableProvider,
};
use snafu::prelude::*;

use crate::{
    component::dataset::acceleration::{Engine, RefreshMode},
    parameters::ParameterSpec,
    register_data_accelerator,
};

use super::{AccelerationSource, DataAccelerator};

#[derive(Debug)]
pub(crate) struct ArrowPartitionCreator {
    cmd: CreateExternalTable,
    arrow_factory: ArrowFactory,
    partition_by: Vec<PartitionedBy>,
}

impl ArrowPartitionCreator {
    pub(crate) fn new(cmd: CreateExternalTable, partition_by: Vec<PartitionedBy>) -> Self {
        Self {
            cmd,
            arrow_factory: ArrowFactory::new(),
            partition_by,
        }
    }
}

#[async_trait]
impl PartitionCreator for ArrowPartitionCreator {
    async fn create_partition(
        &self,
        partition_values: Vec<ScalarValue>,
    ) -> Result<Partition, CreatorError> {
        if partition_values.is_empty() {
            return Err(CreatorError::CreatePartition {
                source: "At least one partition value is required".into(),
            });
        }

        if partition_values.len() != self.partition_by.len() {
            return Err(CreatorError::CreatePartition {
                source: format!(
                    "Expected {} partition values but got {}",
                    self.partition_by.len(),
                    partition_values.len()
                )
                .into(),
            });
        }

        let ctx = SessionContext::new();
        let table_provider =
            TableProviderFactory::create(&self.arrow_factory, &ctx.state(), &self.cmd)
                .await
                .boxed()
                .context(CreatePartitionSnafu)?;

        Ok(Partition {
            partition_values,
            table_provider,
        })
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, CreatorError> {
        // Arrow is purely in-memory — no prior state to recover
        Ok(vec![])
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

pub(crate) struct PartitionedArrowAccelerator;

impl PartitionedArrowAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for PartitionedArrowAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::runtime("file_watcher"),
    ParameterSpec::runtime("hash_index")
        .description("Enable hash index for fast primary key lookups and upserts. Automatically enabled when primary_key is supplied, except in caching refresh mode."),
    ParameterSpec::component("sort_columns")
        .description("Comma-separated list of columns to sort data by during inserts (e.g., 'timestamp,user_id')."),
];

#[async_trait]
impl DataAccelerator for PartitionedArrowAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "partitioned_arrow"
    }

    async fn create_external_table(
        &self,
        mut cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
        _runtime_env: Option<Arc<RuntimeEnv>>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        ensure!(
            !partition_by.is_empty(),
            super::InvalidConfigurationSnafu {
                msg: "PartitionedArrow accelerator requires non-empty `partition_by`".to_string()
            }
        );

        let acceleration = source.as_ref().and_then(|s| s.acceleration());
        let is_caching_mode = acceleration.is_some_and(|acceleration| {
            matches!(acceleration.refresh_mode, Some(RefreshMode::Caching))
        });

        if let Some(acceleration) = acceleration {
            if let Some(sort_cols_str) = acceleration.params.get("sort_columns") {
                cmd.options
                    .insert("sort_columns".to_string(), sort_cols_str.clone());
            }
            if let Some(hash_index_str) = acceleration.params.get("hash_index") {
                cmd.options
                    .insert("hash_index".to_string(), hash_index_str.clone());
            }
            // For caching mode, strip primary key constraints since Arrow uses InsertOp::Replace
            // which overwrites the entire table. Primary key constraints cause uniqueness validation
            // errors during inserts because Arrow doesn't support upsert operations.
            if is_caching_mode {
                cmd.constraints = Constraints::new_unverified(vec![]);
            }
        }

        if !is_caching_mode {
            super::arrow::enable_hash_index_for_primary_key(&mut cmd);
        }

        let schema = Arc::new(cmd.schema.as_arrow().clone());
        let creator = Arc::new(ArrowPartitionCreator::new(cmd, partition_by.clone()));
        let table_provider =
            Arc::new(PartitionTableProvider::new(creator, partition_by, schema).await?);

        Ok(table_provider as Arc<dyn TableProvider>)
    }

    fn prefix(&self) -> &'static str {
        "arrow"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

register_data_accelerator!(Engine::PartitionedArrow, PartitionedArrowAccelerator);
