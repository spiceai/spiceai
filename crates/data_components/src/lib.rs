#![allow(clippy::missing_errors_doc)]
use std::{error::Error, sync::Arc};

use async_trait::async_trait;
use datafusion::{common::OwnedTableReference, datasource::TableProvider};

pub mod arrow;
pub mod databricks;

#[async_trait]
pub trait Read {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait ReadWrite: Send + Sync {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait Stream {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>>;
}
