use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;

use secrets::SecretsProvider;
use snafu::prelude::*;
use tokio::sync::RwLock;

pub mod spiceai_extension;
use crate::datafusion::DataFusion;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load secret: {source}"))]
    UnableToInitializeExtension {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

///
/// Extension trait
///
/// This trait is used to define the interface for extensions to the Spice runtime.
#[async_trait]
pub trait Extension: Send + Sync {
    fn name(&self) -> &'static str;
    fn metrics_connector(&self) -> Option<Box<dyn MetricsConnector>> {
        None
    }

    async fn initialize(&mut self, runtime: Box<&mut dyn Runtime>) -> Result<()>;

    async fn on_start(&mut self, runtime: Box<&mut dyn Runtime>) -> Result<()>;
}

///
/// Runtime trait
///
/// This trait is used to define the extensible interface for the Spice runtime.
#[async_trait]
pub trait Runtime: Send + Sync {
    fn datafusion(&self) -> Arc<RwLock<DataFusion>>;

    fn secrets_provider(&self) -> Arc<RwLock<SecretsProvider>>;

    async fn register_extension(&mut self, extension: Box<&mut dyn Extension>) -> Result<()>;
}

#[async_trait]
pub trait MetricsConnector: Send + Sync {
    async fn write_metrics(&self, record_batch: RecordBatch) -> Result<()>;
}
