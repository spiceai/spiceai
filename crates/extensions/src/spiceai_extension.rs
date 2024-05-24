use std::sync::Arc;

// use arrow::array::RecordBatch;
use async_trait::async_trait;
use snafu::prelude::*;
use tokio::sync::RwLock;

use runtime::{
    datafusion::DataFusion,
    extensions::{Extension, ExtensionFactory, Result},
    Runtime,
};
use secrets::Secret;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load secret: {source}"))]
    UnableToLoadSecret {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub struct SpiceExtension {
    datafusion: Arc<RwLock<DataFusion>>,
    secret: Option<Secret>,
}

impl SpiceExtension {
    pub fn new(datafusion: Arc<RwLock<DataFusion>>) -> Self {
        SpiceExtension {
            datafusion,
            secret: None,
        }
    }
}

#[async_trait]
impl Extension for SpiceExtension {
    fn name(&self) -> &'static str {
        "spiceai"
    }

    async fn initialize(&mut self, _runtime: &mut Runtime) -> Result<()> {
        tracing::info!("Initializing Spiceai Extension");

        Ok(())
    }

    async fn on_start(&mut self, runtime: &Runtime) -> Result<()> {
        tracing::info!("Starting Spiceai Extension");

        let secrets = runtime.secrets_provider.read().await;
        let secret = secrets
            .get_secret("spiceai")
            .await
            .map_err(|e| runtime::extensions::Error::UnableToInitializeExtension { source: e })?;

        self.secret = secret;

        // - create metrics recorder
        // - create table
        // - register table
        Ok(())
    }
}

pub struct SpiceExtensionFactory;

impl ExtensionFactory for SpiceExtensionFactory {
    fn create(&self) -> Box<dyn Extension> {
        Box::new(SpiceExtension::new(Arc::new(
            RwLock::new(DataFusion::new()),
        )))
    }
}

// pub struct SpiceMetricsConnector {}

// #[async_trait]
// impl MetricsConnector for SpiceMetricsConnector {
//     async fn write_metrics(&self, _record_batch: RecordBatch) -> super::Result<()> {
//         // - write metrics to Spiceai
//         Ok(())
//     }
// }
