use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use snafu::Whatever;
use tokio::sync::RwLock;

use super::{Extension, MetricsConnector, Runtime};
use crate::datafusion::DataFusion;

pub struct SpiceExtension {
    datafusion: Arc<RwLock<DataFusion>>,
}

impl SpiceExtension {
    pub fn new(datafusion: Arc<RwLock<DataFusion>>) -> Self {
        SpiceExtension { datafusion }
    }
}

impl Extension for SpiceExtension {
    fn name(&self) -> &'static str {
        "spiceai"
    }

    fn initialize(&mut self, _runtime: Box<&mut dyn Runtime>) {
        tracing::info!("Initializing Spiceai Extension");
    }

    fn on_start(&mut self, _runtime: Box<&mut dyn Runtime>) {
        tracing::info!("Starting Spiceai Extension");

        // - read secret
        // - create metrics recorder
        // - create table
        // - register table
    }

    fn metrics_connector(&self) -> Option<Box<dyn MetricsConnector>> {
        None
    }
}

pub struct SpiceMetricsConnector {}

#[async_trait]
impl MetricsConnector for SpiceMetricsConnector {
    async fn write_metrics(&self, _record_batch: RecordBatch) -> Result<(), Whatever> {
        // - write metrics to Spiceai
        Ok(())
    }
}
