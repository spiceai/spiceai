use async_trait::async_trait;
use snafu::prelude::*;

use crate::Runtime;

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
    // fn metrics_connector(&self) -> Option<Box<dyn MetricsConnector>> {
    //     None
    // }

    async fn initialize(&mut self, runtime: &mut Runtime) -> Result<()>;

    async fn on_start(&mut self, runtime: &Runtime) -> Result<()>;
}

// #[async_trait]
// pub trait MetricsConnector: Send + Sync {
//     async fn write_metrics(&self, record_batch: RecordBatch) -> Result<()>;
// }

pub trait ExtensionFactory {
    fn create(&self) -> Box<dyn Extension>;
}
