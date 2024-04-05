use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::OwnedTableReference,
    datasource::{MemTable, TableProvider},
};
use std::{error::Error, sync::Arc};

use crate::{Read, ReadWrite};

pub struct Arrow {
    memtable: Arc<dyn TableProvider>,
}

impl Arrow {
    pub fn try_new(schema: SchemaRef) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            memtable: Arc::new(MemTable::try_new(schema, vec![])?),
        })
    }
}

#[async_trait]
impl Read for Arrow {
    async fn table_provider(
        &self,
        _table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        Ok(Arc::clone(&self.memtable))
    }
}

#[async_trait]
impl ReadWrite for Arrow {
    async fn table_provider(
        &self,
        _table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        Ok(Arc::clone(&self.memtable))
    }
}
