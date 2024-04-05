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

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::{
    datasource::{MemTable, TableProvider},
    logical_expr::CreateExternalTable,
};
use std::{any::Any, sync::Arc};

use super::DataAccelerator;

pub struct ArrowAccelerator {}

impl ArrowAccelerator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ArrowAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataAccelerator for ArrowAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let arrow_schema = Schema::from(cmd.schema.as_ref());

        let mem_table = MemTable::try_new(arrow_schema, vec![]).map_err(|e| Box::new(e))?;

        let table_provider = Arc::new(mem_table) as Arc<dyn TableProvider>;
        Ok(table_provider)
    }
}
