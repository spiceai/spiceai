use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;

use snafu::Whatever;
use tokio::sync::RwLock;

pub mod spiceai_extension;
use crate::datafusion::DataFusion;

pub trait Extension {
    fn name(&self) -> &'static str;
    fn initialize(&mut self, runtime: Box<&mut dyn Runtime>);
    fn on_start(&mut self, runtime: Box<&mut dyn Runtime>);

    fn metrics_connector(&self) -> Option<Box<dyn MetricsConnector>>;
}

pub trait Runtime {
    fn datafusion(&self) -> Arc<RwLock<DataFusion>>;

    fn register_extension(&mut self, extension: Box<&mut dyn Extension>);
}

#[async_trait]
pub trait MetricsConnector {
    async fn write_metrics(&self, record_batch: RecordBatch) -> Result<(), Whatever>;
}
