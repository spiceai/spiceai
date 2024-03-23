use std::sync::Arc;

use async_trait::async_trait;
use data_components::Write;
use datafusion::common::OwnedTableReference;
use datafusion::execution::context::SessionContext;
use spicepod::component::dataset::Dataset;

use crate::dataupdate::DataUpdate;

#[async_trait]
pub trait DataPublisher: Send + Sync {
    async fn add_data(
        &self,
        dataset: Arc<Dataset>,
        data_update: DataUpdate,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

#[async_trait]
impl DataPublisher for dyn Write + '_ {
    async fn add_data(
        &self,
        dataset: Arc<Dataset>,
        data_update: DataUpdate,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let write_table_provider = self
            .table_provider(OwnedTableReference::from(dataset.name.clone()))
            .await?;

        let ctx = SessionContext::new();

        Ok(())
    }
}
