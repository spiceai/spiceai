/*
Copyright 2024 The Spice.ai OSS Authors

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
use data_components::sqlite::SqliteTableFactory;
use datafusion::{
    datasource::{provider::TableProviderFactory, TableProvider},
    execution::context::SessionContext,
    logical_expr::CreateExternalTable,
};
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

use super::DataAccelerator;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create table: {source}"))]
    UnableToCreateTable {
        source: datafusion::error::DataFusionError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqliteAccelerator {
    sqlite_factory: SqliteTableFactory,
}

impl SqliteAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sqlite_factory: SqliteTableFactory::new(),
        }
    }
}

impl Default for SqliteAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataAccelerator for SqliteAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();
        TableProviderFactory::create(&self.sqlite_factory, &ctx.state(), cmd)
            .await
            .context(UnableToCreateTableSnafu)
            .boxed()
    }
}
