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
use datafusion::{
    catalog::TableProviderFactory, common::Constraints, datasource::TableProvider,
    execution::context::SessionContext, logical_expr::CreateExternalTable,
};
use runtime_table_partition::expression::PartitionedBy;
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

use crate::{component::dataset::acceleration::RefreshMode, parameters::ParameterSpec};

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

const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::runtime("file_watcher")];

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
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        ensure!(
            partition_by.is_empty(),
            super::InvalidConfigurationSnafu {
                msg: "Arrow data accelerator does not support the `partition_by` parameter but it was provided".to_string()
            }
        );

        // For caching mode, strip primary key constraints since Arrow uses InsertOp::Replace
        // which overwrites the entire table. Primary key constraints cause uniqueness validation
        // errors during inserts because Arrow doesn't support upsert operations.
        let is_caching_mode = source
            .and_then(|s| s.acceleration())
            .and_then(|a| a.refresh_mode.as_ref())
            .is_some_and(|mode| matches!(mode, RefreshMode::Caching { .. }));

        let cmd = if is_caching_mode {
            let mut cmd = cmd;
            cmd.constraints = Constraints::new_unverified(vec![]);
            cmd
        } else {
            cmd
        };

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
