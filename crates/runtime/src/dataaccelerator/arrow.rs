/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use async_trait::async_trait;
use data_components::arrow::ArrowFactory;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::{
    catalog::TableProviderFactory,
    common::{Constraint, Constraints},
    datasource::TableProvider,
    execution::context::SessionContext,
    logical_expr::CreateExternalTable,
};
use runtime_table_partition::expression::PartitionedBy;
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

use crate::component::dataset::acceleration::{Engine, RefreshMode};
use crate::parameters::ParameterSpec;
use crate::register_data_accelerator;

use super::{AccelerationSource, DataAccelerator};

pub struct ArrowAccelerator {
    arrow_factory: ArrowFactory,
}

impl ArrowAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            arrow_factory: ArrowFactory::new(),
        }
    }
}

impl Default for ArrowAccelerator {
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

pub(crate) fn enable_hash_index_for_primary_key(cmd: &mut CreateExternalTable) {
    let has_primary_key = cmd.constraints.iter().any(
        |constraint| matches!(constraint, Constraint::PrimaryKey(columns) if !columns.is_empty()),
    );

    if !has_primary_key {
        return;
    }

    if let Some(value) = cmd.options.get("hash_index")
        && !value.eq_ignore_ascii_case("enabled")
    {
        tracing::warn!(
            hash_index = %value,
            "Arrow acceleration with primary_key requires hash_index; overriding hash_index to enabled"
        );
    }

    cmd.options
        .insert("hash_index".to_string(), "enabled".to_string());
}

#[async_trait]
impl DataAccelerator for ArrowAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "arrow"
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        mut cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        _partition_by: Vec<PartitionedBy>,
        _runtime_env: Option<Arc<RuntimeEnv>>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        // For caching mode, strip primary key constraints since Arrow uses InsertOp::Replace
        // which overwrites the entire table. Primary key constraints cause uniqueness validation
        // errors during inserts because Arrow doesn't support upsert operations.
        let is_caching_mode = source
            .and_then(|s| s.acceleration())
            .and_then(|a| a.refresh_mode.as_ref())
            .is_some_and(|mode| matches!(mode, RefreshMode::Caching));

        if is_caching_mode {
            cmd.constraints = Constraints::new_unverified(vec![]);
        }

        // Extract sort_columns from acceleration params if provided
        if let Some(source) = source
            && let Some(acceleration) = source.acceleration()
            && let Some(sort_cols_str) = acceleration.params.get("sort_columns")
        {
            cmd.options
                .insert("sort_columns".to_string(), sort_cols_str.clone());
        }

        // Extract hash_index from acceleration params if provided
        if let Some(source) = source
            && let Some(acceleration) = source.acceleration()
            && let Some(hash_index_str) = acceleration.params.get("hash_index")
        {
            cmd.options
                .insert("hash_index".to_string(), hash_index_str.clone());
        }

        if !is_caching_mode {
            enable_hash_index_for_primary_key(&mut cmd);
        }

        let ctx = SessionContext::new();
        let table_provider = TableProviderFactory::create(&self.arrow_factory, &ctx.state(), &cmd)
            .await
            .boxed()?;

        Ok(table_provider)
    }

    fn prefix(&self) -> &'static str {
        "arrow"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

register_data_accelerator!(Engine::Arrow, ArrowAccelerator);
