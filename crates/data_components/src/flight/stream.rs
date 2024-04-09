/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use arrow_flight::decode::DecodedPayload;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    common::OwnedTableReference,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use flight_client::FlightClient;
use futures::{Stream, StreamExt};
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to subscribe to data from the Flight endpoint: {source}"))]
    UnableToSubscribeData {
        source: flight_client::Error,
    },

    UnableToRetrieveSchema,

    UnableToDecodeFlightData {
        source: arrow_flight::error::FlightError,
    },

    StreamInterrupted {
        source: flight_client::Error,
    },

    #[snafu(display("Projection (column filtering) is not supported for Flight Streams."))]
    ProjectionNotSupported,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct FlightTableStreamer {
    table_reference: OwnedTableReference,
    flight_client: FlightClient,
    schema: SchemaRef,
}

impl FlightTableStreamer {
    pub async fn create(
        table_reference: OwnedTableReference,
        flight_client: FlightClient,
    ) -> Result<Arc<dyn TableProvider>> {
        let schema = Self::get_schema(table_reference.clone(), flight_client.clone()).await?;

        Ok(Arc::new(Self {
            table_reference,
            flight_client,
            schema,
        }))
    }

    async fn get_schema(
        table_reference: OwnedTableReference,
        mut flight_client: FlightClient,
    ) -> Result<SchemaRef> {
        let mut decoder = flight_client
            .subscribe(&table_reference.to_string())
            .await
            .context(UnableToSubscribeDataSnafu)?;

        let decoded_flight_data = decoder
            .next()
            .await
            .context(UnableToRetrieveSchemaSnafu)?
            .context(UnableToDecodeFlightDataSnafu)?;

        match decoded_flight_data.payload {
            DecodedPayload::Schema(schema) => Ok(schema),
            DecodedPayload::RecordBatch(batch) => Ok(batch.schema()),
            DecodedPayload::None => UnableToRetrieveSchemaSnafu.fail()?,
        }
    }
}

#[async_trait]
impl TableProvider for FlightTableStreamer {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if projection.is_some() {
            return Err(to_external_error(ProjectionNotSupportedSnafu.build()));
        }

        Ok(Arc::new(FlightStreamExec::new(
            &self.schema,
            &self.table_reference,
            self.flight_client.clone(),
        )))
    }
}

#[derive(Clone)]
struct FlightStreamExec {
    table_reference: OwnedTableReference,
    client: FlightClient,
    schema: SchemaRef,
}

impl FlightStreamExec {
    fn new(
        schema: &SchemaRef,
        table_reference: &OwnedTableReference,
        client: FlightClient,
    ) -> Self {
        Self {
            table_reference: table_reference.clone(),
            client,
            schema: Arc::clone(schema),
        }
    }
}

impl std::fmt::Debug for FlightStreamExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FlightStreamExec")
    }
}

impl DisplayAs for FlightStreamExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "FlightStreamExec")
    }
}

impl ExecutionPlan for FlightStreamExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let stream_adapter = RecordBatchStreamAdapter::new(
            self.schema(),
            subscribe_to_stream(self.client.clone(), self.table_reference.to_string()),
        );

        Ok(Box::pin(stream_adapter))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn subscribe_to_stream(
    mut client: FlightClient,
    table_reference: String,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    stream! {
        match client.subscribe(&table_reference).await {
            Ok(mut stream) => {
                while let Some(decoded_data) = stream.next().await {
                    match decoded_data {
                        Ok(decoded_data) => match decoded_data.payload {
                          DecodedPayload::None => continue,
                          DecodedPayload::Schema(_) => continue,
                          DecodedPayload::RecordBatch(batch) => yield Ok(batch),
                        },
                        Err(error) => {
                            yield Err(to_external_error(Error::UnableToDecodeFlightData { source: error }));
                        }
                    }
                }
            }
            Err(error) => yield Err(to_external_error(Error::StreamInterrupted{ source: error }))
        }
    }
}

fn to_external_error(e: Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
