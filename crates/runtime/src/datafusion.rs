use std::collections::HashMap;
use std::sync::Arc;

use crate::databackend::{self, DataBackend};
use crate::datasource::DataSource;
use datafusion::error::DataFusionError;
use datafusion::execution::{context::SessionContext, options::ParquetReadOptions};
use futures::StreamExt;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use tokio::task;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to register parquet file {}", file))]
    RegisterParquet {
        source: DataFusionError,
        file: String,
    },

    DataFusion {
        source: DataFusionError,
    },

    #[snafu(display("Table already exists"))]
    TableAlreadyExists {},

    DatasetConfigurationError {
        source: databackend::Error,
    },

    InvalidConfiguration {
        msg: String,
    },
}

pub struct DataFusion {
    pub ctx: Arc<SessionContext>,
    tasks: Vec<task::JoinHandle<()>>,
    backends: HashMap<String, Arc<Box<dyn DataBackend>>>,
}

impl DataFusion {
    #[must_use]
    pub fn new() -> Self {
        DataFusion {
            ctx: Arc::new(SessionContext::new()),
            tasks: Vec::new(),
            backends: HashMap::new(),
        }
    }

    pub async fn register_parquet(&self, table_name: &str, path: &str) -> Result<()> {
        self.ctx
            .register_parquet(table_name, path, ParquetReadOptions::default())
            .await
            .context(RegisterParquetSnafu { file: path })
    }

    pub fn attach_backend(
        &mut self,
        table_name: &str,
        backend: Box<dyn DataBackend>,
    ) -> Result<()> {
        let table_exists = self.ctx.table_exist(table_name).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        self.backends
            .insert(table_name.to_string(), Arc::new(backend));

        Ok(())
    }

    pub fn new_backend(&self, dataset: &Dataset) -> Result<Box<dyn DataBackend>> {
        let table_name = dataset.name.as_str();
        let acceleration =
            dataset
                .acceleration
                .as_ref()
                .ok_or_else(|| Error::InvalidConfiguration {
                    msg: "No acceleration configuration found".to_string(),
                })?;

        let params: Arc<Option<HashMap<String, String>>> = Arc::new(dataset.params.clone());

        let data_backend: Box<dyn DataBackend> = <dyn DataBackend>::new(
            &self.ctx,
            table_name,
            acceleration.engine(),
            acceleration.mode(),
            params,
        )
        .context(DatasetConfigurationSnafu)?;

        Ok(data_backend)
    }

    #[must_use]
    #[allow(clippy::borrowed_box)]
    pub fn get_backend(&self, dataset: &str) -> Option<&Arc<Box<dyn DataBackend>>> {
        self.backends.get(dataset)
    }

    #[must_use]
    pub fn has_backend(&self, dataset: &str) -> bool {
        self.backends.contains_key(dataset)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn attach(
        &mut self,
        dataset: &Dataset,
        data_source: &'static mut dyn DataSource,
        backend: Box<dyn DataBackend>,
    ) -> Result<()> {
        let table_name = dataset.name.as_str();
        let table_exists = self.ctx.table_exist(table_name).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let dataset_clone = dataset.clone();
        let task_handle = task::spawn(async move {
            let mut stream = data_source.get_data(&dataset_clone);
            loop {
                let future_result = stream.next().await;
                match future_result {
                    Some(data_update) => match backend.add_data(data_update).await {
                        Ok(()) => (),
                        Err(e) => tracing::error!("Error adding data: {e:?}"),
                    },
                    None => break,
                };
            }
        });

        self.tasks.push(task_handle);

        Ok(())
    }
}

impl Drop for DataFusion {
    fn drop(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

impl Default for DataFusion {
    fn default() -> Self {
        Self::new()
    }
}
