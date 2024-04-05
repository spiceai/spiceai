use std::{any::Any, sync::Arc, time::Duration};

use arrow::datatypes::SchemaRef;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{
    datasource::{TableProvider, TableType},
    execution::context::SessionContext,
    logical_expr::Expr,
};
use futures::{stream::BoxStream, StreamExt};
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::RefreshMode;
use tokio::task::JoinHandle;

use crate::{
    dataconnector::{self, get_all_data},
    dataupdate::{DataUpdate, DataUpdateExecutionPlan, UpdateType},
    status,
    timing::TimeMeasurement,
};

#[derive(Debug, Snafu)]
enum Error {
    AddDataError {
        e: String,
    },

    DataConnectorError {
        source: dataconnector::Error,
    },

    #[snafu(display("Unable to scan table provider: {source}"))]
    UnableToScanTableProvider {
        source: datafusion::error::DataFusionError,
    },
}

type Result<T> = std::result::Result<T, Error>;

// An accelerated table consists of a source table and a local accelerator.
//
// The accelerator must support inserts.
pub(crate) struct AcceleratedTable {
    dataset_name: String,
    accelerator: Arc<dyn TableProvider>,
    refresh_handle: JoinHandle<()>,
}

impl AcceleratedTable {
    pub fn new(
        dataset_name: String,
        source: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        refresh_mode: RefreshMode,
        refresh_interval: Option<Duration>,
    ) -> Self {
        let refresh_handle = tokio::spawn(Self::start_refresh(
            dataset_name.clone(),
            source,
            refresh_mode,
            refresh_interval,
            Arc::clone(&accelerator),
        ));

        Self {
            dataset_name,
            accelerator,
            refresh_handle,
        }
    }

    async fn start_refresh(
        dataset_name: String,
        source: Arc<dyn TableProvider>,
        refresh_mode: RefreshMode,
        refresh_interval: Option<Duration>,
        accelerator: Arc<dyn TableProvider>,
    ) {
        let mut stream =
            Self::stream_updates(dataset_name.clone(), source, refresh_mode, refresh_interval);
        loop {
            let future_result = stream.next().await;
            match future_result {
                Some(data_update) => {
                    let data_update = match data_update {
                        Ok(data_update) => data_update,
                        Err(e) => {
                            tracing::debug!("Error streaming data for {dataset_name}: {e}");
                            break;
                        }
                    };
                    let ctx = SessionContext::new();
                    let state = ctx.state();
                    let overwrite = data_update.update_type == UpdateType::Overwrite;
                    if let Err(e) = accelerator
                        .insert_into(
                            &state,
                            Arc::new(DataUpdateExecutionPlan::new(data_update)),
                            overwrite,
                        )
                        .await
                    {
                        tracing::error!("Error adding data for {dataset_name}: {e}");
                    };
                }
                None => break,
            };
        }
    }

    fn stream_updates<'a>(
        dataset_name: String,
        source: Arc<dyn TableProvider>,
        refresh_mode: RefreshMode,
        refresh_interval: Option<Duration>,
    ) -> BoxStream<'a, Result<DataUpdate>> {
        return Box::pin(stream! {
            match refresh_mode {
                RefreshMode::Append => {
                    let ctx = SessionContext::new();
                    let plan = source.scan(&ctx.state(), None, &[], None).await.context(UnableToScanTableProviderSnafu {})?;

                    if plan.output_partitioning().partition_count() > 1 {
                        tracing::error!("Append is not supported for tables with multiple partitions: {dataset_name}");
                        return;
                    }

                    let schema = source.schema();

                    let mut stream = plan.execute(0, ctx.task_ctx()).context(UnableToScanTableProviderSnafu {})?;
                    loop {
                        match stream.next().await {
                            Some(Ok(batch)) => {
                                yield Ok(DataUpdate {
                                    schema: Arc::clone(&schema),
                                    data: vec![batch],
                                    update_type: UpdateType::Append,
                                });
                            }
                            Some(Err(e)) => {
                                tracing::error!("Error reading data for {dataset_name}: {e}");
                                yield Err(Error::UnableToScanTableProvider { source: e });
                            }
                            None => break,
                        }
                    }
                }
                RefreshMode::Full => loop {
                  tracing::info!("Refreshing data for {dataset_name}");
                  status::update_dataset(&dataset_name, status::ComponentStatus::Refreshing);
                  let timer = TimeMeasurement::new("load_dataset_duration_ms", vec![("dataset", dataset_name.clone())]);
                  let all_data = match get_all_data(source.as_ref()).await {
                      Ok(data) => data,
                      Err(e) => {
                          tracing::error!("Error refreshing data for {dataset_name}: {e}");
                          yield Err(Error::DataConnectorError { source: e });
                          continue;
                      }
                  };
                  yield Ok(DataUpdate {
                      schema: all_data.0,
                      data: all_data.1,
                      update_type: UpdateType::Overwrite,
                  });
                  drop(timer);
                  match refresh_interval {
                      Some(interval) => tokio::time::sleep(interval).await,
                      None => break,
                  };
              },
            }
        });
    }
}

impl Drop for AcceleratedTable {
    fn drop(&mut self) {
        self.refresh_handle.abort();
    }
}

#[async_trait]
impl TableProvider for AcceleratedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.accelerator.schema()
    }

    fn table_type(&self) -> TableType {
        self.accelerator.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.accelerator.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.accelerator
            .scan(state, projection, filters, limit)
            .await
    }
}
