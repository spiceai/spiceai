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
    catalog::TableProviderFactory, datasource::TableProvider, execution::context::SessionContext,
    logical_expr::CreateExternalTable,
};
use snafu::ResultExt;
use std::{any::Any, sync::Arc};

use crate::{component::dataset::Dataset, parameters::ParameterSpec};

use super::DataAccelerator;

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
        cmd: &CreateExternalTable,
        _dataset: Option<&Dataset>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();
        TableProviderFactory::create(&self.arrow_factory, &ctx.state(), cmd)
            .await
            .boxed()
    }

    fn prefix(&self) -> &'static str {
        "arrow"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}
