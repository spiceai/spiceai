/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use arrow_tools::map_entries::{self, StreamNormalizer};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    sql::TableReference,
};
use flight_client::FlightClient;
use futures::{Stream, StreamExt};
use snafu::prelude::*;
use std::{fmt, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to subscribe to data from the Flight endpoint: {source}"))]
    UnableToSubscribeData { source: flight_client::Error },

    #[snafu(display("Unable to retrieve schema from Flight DoExchange."))]
    UnableToRetrieveSchema,

    #[snafu(display("Failed to decode data from Flight endpoint: {source}"))]
    UnableToDecodeFlightData {
        source: arrow_flight::error::FlightError,
    },

    #[snafu(display("Flight data stream was interrupted: {source}"))]
    StreamInterrupted { source: flight_client::Error },

    #[snafu(display("Projection (column filtering) is not supported for Flight Streams."))]
    ProjectionNotSupported,

    #[snafu(display(
        "Failed to read the change stream from Arrow Flight for table '{table}' ({source}), \
        so the dataset stops receiving updates. \
        Remove the null map entries at the source, or expose the column as a string with `to_json(<column>)`. \
        See: https://spiceai.org/docs/components/data-connectors"
    ))]
    MapEntriesNotNormalizable {
        table: String,
        source: map_entries::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct FlightTableStreamer {
    table_reference: TableReference,
    flight_client: FlightClient,
    schema: SchemaRef,
}

impl FlightTableStreamer {
    pub async fn create(
        table_reference: TableReference,
        flight_client: FlightClient,
    ) -> Result<Arc<dyn TableProvider>> {
        let schema = Self::get_schema(table_reference.clone(), flight_client.clone()).await?;
        // A producer is free to declare a MAP's `entries` field nullable, which the Arrow map
        // layout forbids. Correcting it here keeps the schema this table reports to the planner
        // in step with the batches `execute` hands back, which are normalized to the same shape.
        let schema = map_entries::conforming_schema(schema);

        Ok(Arc::new(Self {
            table_reference,
            flight_client,
            schema,
        }))
    }

    async fn get_schema(
        table_reference: TableReference,
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
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
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
    table_reference: TableReference,
    client: FlightClient,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FlightStreamExec {
    fn new(schema: &SchemaRef, table_reference: &TableReference, client: FlightClient) -> Self {
        Self {
            table_reference: table_reference.clone(),
            client,
            schema: Arc::clone(schema),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            )),
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
    fn name(&self) -> &'static str {
        "FlightStreamExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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

fn subscribe_to_stream(
    mut client: FlightClient,
    table_reference: String,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    stream! {
        // The subscription carries one schema per stream, so the normalizer is resolved from the
        // first batch and reused for the rest.
        let mut normalizer = StreamNormalizer::new();
        match client.subscribe(&table_reference).await {
            Ok(mut stream) => {
                while let Some(decoded_data) = stream.next().await {
                    match decoded_data {
                        Ok(decoded_data) => match decoded_data.payload {
                          DecodedPayload::None => {},
                          DecodedPayload::Schema(_) => {},
                          DecodedPayload::RecordBatch(batch) => yield normalizer
                              .normalize(batch)
                              .map_err(|source| to_external_error(Error::MapEntriesNotNormalizable {
                                  table: table_reference.clone(),
                                  source,
                              })),
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

#[cfg(test)]
mod tests {
    use super::FlightTableStreamer;
    use crate::flight::tests::TestServer;
    use arrow::array::MapArray;
    use arrow::datatypes::{DataType, SchemaRef};
    use datafusion::catalog::TableProvider;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use flight_client::{Credentials, FlightClient};
    use std::sync::Arc;

    /// Regression test for #13495 over the subscription read path: `FlightTableStreamer` learns
    /// its schema from a `DoExchange` subscription and yields that subscription's batches, so a
    /// producer declaring a `MAP`'s `entries` field nullable reaches the planner and the scan
    /// unless both halves are relabelled — the same correction the `DoGet` path applies.
    #[tokio::test]
    async fn a_producers_nullable_map_entries_declaration_is_corrected_on_subscribe() {
        let server = TestServer::start().await;
        let client = FlightClient::try_new(
            Arc::from(format!("http://{}", server.addr)),
            Credentials::anonymous(),
            None,
            None,
        )
        .await
        .expect("client should connect");

        let table = FlightTableStreamer::create(TableReference::bare("t"), client)
            .await
            .expect("table should be created");

        let conforming = |schema: &SchemaRef| match schema.field(0).data_type() {
            DataType::Map(entries, _) => !entries.is_nullable(),
            other => panic!("expected a Map column, got {other:?}"),
        };
        assert!(
            conforming(&TableProvider::schema(table.as_ref())),
            "the schema reported to the planner still declares nullable entries"
        );

        let ctx = SessionContext::new();
        let plan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan should plan");
        let batches = collect(plan, ctx.task_ctx())
            .await
            .expect("a nullable entries declaration is relabelled, not refused");

        let [batch] = batches.as_slice() else {
            panic!(
                "the producer serves exactly one batch, got {}",
                batches.len()
            );
        };
        assert!(
            conforming(&batch.schema()),
            "the subscription batch still carries the producer's non-conforming declaration"
        );

        // The property the declaration controls: every kernel that touches a map column rebuilds
        // it through this constructor, and a nullable `entries` field is refused there outright.
        let map = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("a Map column");
        let (field, offsets, entries, nulls, ordered) = map.clone().into_parts();
        MapArray::try_new(field, offsets, entries, nulls, ordered)
            .expect("the corrected column can be rebuilt by a kernel");

        server.shutdown().await;
    }
}
