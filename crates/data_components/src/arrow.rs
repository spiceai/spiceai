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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::OwnedTableReference,
    datasource::{MemTable, TableProvider},
};
use std::{error::Error, sync::Arc};

use crate::{Read, ReadWrite};

pub struct Arrow {
    arrow_provider: Arc<dyn TableProvider>,
}

impl Arrow {
    pub fn try_new(schema: SchemaRef) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            arrow_provider: Arc::new(MemTable::try_new(schema, vec![])?),
        })
    }
}

#[async_trait]
impl Read for Arrow {
    async fn table_provider(
        &self,
        _table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        Ok(Arc::clone(&self.arrow_provider))
    }
}

#[async_trait]
impl ReadWrite for Arrow {
    async fn table_provider(
        &self,
        _table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        Ok(Arc::clone(&self.arrow_provider))
    }
}
