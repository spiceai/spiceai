use std::sync::Arc;

use crate::databackend::{self, memtable::MemTableBackend, DataBackend};
use crate::datasource::DataSource;
use datafusion::error::DataFusionError;
use datafusion::execution::{context::SessionContext, options::ParquetReadOptions};
use futures::StreamExt;
use snafu::prelude::*;
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

    TableAlreadyExists {},
}

pub struct DataFusion {
    pub ctx: Arc<SessionContext>,
    tasks: Vec<task::JoinHandle<()>>,
}

impl DataFusion {
    #[must_use]
    pub fn new() -> Self {
        DataFusion {
            ctx: Arc::new(SessionContext::new()),
            tasks: Vec::new(),
        }
    }

    pub async fn register_parquet(&self, table_name: &str, path: &str) -> Result<()> {
        self.ctx
            .register_parquet(table_name, path, ParquetReadOptions::default())
            .await
            .context(RegisterParquetSnafu { file: path })
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn attach(
        &mut self,
        dataset: &str,
        data_source: &'static dyn DataSource,
        backend: databackend::DataBackendType,
    ) -> Result<()> {
        let table_exists = self.ctx.table_exist(dataset).context(DataFusionSnafu)?;
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let mut data_backend: Box<dyn DataBackend> = match backend {
            databackend::DataBackendType::Memtable => {
                Box::new(MemTableBackend::new(self.ctx.clone(), dataset))
            }
            databackend::DataBackendType::DuckDB => {
                todo!("DuckDB backend not implemented yet");
            }
        };

        let dataset = dataset.to_string();
        let task_handle = task::spawn(async move {
            let mut stream = data_source.get_data(dataset.as_str());
            loop {
                let future_result = stream.next().await;
                match future_result {
                    Some(data_update) => match data_backend.add_data(data_update).await {
                        Ok(()) => (),
                        Err(e) => tracing::error!("Error adding data: {e:?}"),
                    },
                    None => continue,
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
