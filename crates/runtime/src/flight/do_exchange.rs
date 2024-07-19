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

use std::sync::Arc;

use arrow::array::{Array, ListArray, StringArray, StructArray};
use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::{flight_service_server::FlightService, FlightData, SchemaAsIpc};
use arrow_ipc::writer::{self, DictionaryTracker, IpcDataGenerator};
use data_components::cdc::changes_schema;
use datafusion::{catalog::schema, sql::TableReference};
use futures::{stream, StreamExt};
use tokio::sync::broadcast;
use tonic::{Request, Response, Status, Streaming};

use crate::dataupdate::{DataUpdate, UpdateType};

use super::Service;

#[allow(clippy::too_many_lines)]
pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoExchangeStream>, Status> {
    let mut streaming_request = request.into_inner();
    let req = streaming_request.next().await;
    let Some(subscription_request) = req else {
        return Err(Status::invalid_argument(
            "Need to send a FlightData message with a FlightDescriptor to subscribe to",
        ));
    };

    let subscription_request = match subscription_request {
        Ok(subscription_request) => subscription_request,
        Err(e) => {
            return Err(Status::invalid_argument(format!(
                "Unable to read subscription request: {e}",
            )));
        }
    };

    // TODO: Support multiple flight descriptors to subscribe to multiple data sources
    let Some(flight_descriptor) = subscription_request.flight_descriptor else {
        return Err(Status::invalid_argument(
            "Flight descriptor required to indicate which data to subscribe to",
        ));
    };

    if flight_descriptor.path.is_empty() {
        return Err(Status::invalid_argument(
            "Flight descriptor needs to specify a path to indicate which data to subscribe to",
        ));
    };

    let data_path = TableReference::parse_str(&flight_descriptor.path.join("."));

    if flight_svc
        .datafusion
        .get_table(data_path.clone())
        .await
        .is_none()
    {
        return Err(Status::invalid_argument(format!(
            r#"Unknown dataset: "{data_path}""#,
        )));
    }

    let channel_map = Arc::clone(&flight_svc.channel_map);
    let channel_map_read = channel_map.read().await;
    let (tx, rx) = if let Some(channel) = channel_map_read.get(&data_path) {
        (Arc::clone(channel), channel.subscribe())
    } else {
        drop(channel_map_read);
        let mut channel_map_write = channel_map.write().await;
        let (tx, rx) = broadcast::channel(100);
        let tx = Arc::new(tx);
        channel_map_write.insert(data_path.clone(), Arc::clone(&tx));
        (tx, rx)
    };

    let response_stream = stream::unfold(rx, move |mut rx| {
        let encoder = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let write_options = writer::IpcWriteOptions::default();
        async move {
            match rx.recv().await {
                Ok(data_update) => {
                    let mut schema_sent: bool = false;

                    let mut flights = vec![];

                    for batch in &data_update.data {
                        if !schema_sent {
                            let schema = batch.schema();

                            flights
                                .push(FlightData::from(SchemaAsIpc::new(&schema, &write_options)));
                            schema_sent = true;
                        }
                        let Ok((flight_dictionaries, flight_batch)) =
                            encoder.encoded_batch(batch, &mut tracker, &write_options)
                        else {
                            panic!("Unable to encode batch")
                        };

                        flights.extend(flight_dictionaries.into_iter().map(Into::into));
                        flights.push(flight_batch.into());
                    }

                    metrics::counter!("flight_do_exchange_data_updates_sent")
                        .increment(flights.len() as u64);
                    let output = futures::stream::iter(flights.into_iter().map(Ok));

                    Some((output, rx))
                }
                Err(_e) => {
                    let output = futures::stream::iter(vec![].into_iter().map(Ok));
                    Some((output, rx))
                }
            }
        }
    })
    .flat_map(|x| x);

    let datafusion = Arc::clone(&flight_svc.datafusion);
    tokio::spawn(async move {
        let Ok(df) = datafusion
            .ctx
            .sql(&format!(r#"SELECT * FROM {data_path}"#))
            .await
        else {
            return;
        };
        let Ok(results) = df.collect().await else {
            return;
        };
        if results.is_empty() {
            return;
        }

        for batch in &results {
            let schema = batch.schema();
            let row_count = batch.num_rows();

            let op_data = vec!["r"; row_count];
            let op_array = StringArray::from(op_data);

            let primary_keys_array = ListArray::new_null(
                Arc::new(Field::new("item", DataType::Utf8, false)),
                row_count,
            );

            let data_array = StructArray::from(batch.clone());

            let new_schema = Arc::new(changes_schema(schema.as_ref()));
            let new_record_batch = RecordBatch::try_new(
                new_schema.clone(),
                vec![
                    Arc::new(op_array),
                    Arc::new(primary_keys_array),
                    Arc::new(data_array),
                ],
            )
            .expect("Failed to create new RecordBatch");

            let data_update = DataUpdate {
                data: vec![new_record_batch.clone()],
                schema: new_schema,
                update_type: UpdateType::Changes,
            };
            let _ = tx.send(data_update);
        }
    });

    Ok(Response::new(response_stream.boxed()))
}
