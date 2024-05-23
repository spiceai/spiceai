use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use secrets::Secret;
use snafu::prelude::*;
use tokio::sync::RwLock;

use super::{Extension, MetricsConnector, Runtime};
use crate::datafusion::DataFusion;

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

    async fn initialize(&mut self, _runtime: Box<&mut dyn Runtime>) -> super::Result<()> {
        tracing::info!("Initializing Spiceai Extension");

        Ok(())
    }

    async fn on_start(&mut self, runtime: Box<&mut dyn Runtime>) -> super::Result<()> {
        tracing::info!("Starting Spiceai Extension");

        let secrets_provider = runtime.secrets_provider();
        let secrets = secrets_provider.read().await;
        let secret = secrets
            .get_secret("spiceai")
            .await
            .context(super::UnableToInitializeExtensionSnafu)?;

        self.secret = secret;

        // - create metrics recorder
        // - create table
        // - register table
        Ok(())
    }
}

pub struct SpiceMetricsConnector {}

#[async_trait]
impl MetricsConnector for SpiceMetricsConnector {
    async fn write_metrics(&self, _record_batch: RecordBatch) -> super::Result<()> {
        // - write metrics to Spiceai
        Ok(())
    }
}
