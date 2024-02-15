use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use spicepod::component::dataset::Dataset;

use crate::dataupdate::DataUpdate;

pub type AddDataResult<'a> =
    Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send + 'a>>;

pub trait DataPublisher: Send + Sync {
    fn add_data(&self, dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult;

    fn name(&self) -> &str;
}
