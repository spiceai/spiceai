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

use std::sync::Arc;

use arrow::array::{make_builder, ArrayBuilder, ListBuilder, RecordBatch, StringBuilder};
use arrow::array::{ListArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow_flight::{flight_service_server::FlightService, FlightData, SchemaAsIpc};
use arrow_ipc::writer::{self, DictionaryTracker, IpcDataGenerator};
use data_components::cdc::changes_schema;
use datafusion::common::{Constraint, Constraints};
use datafusion::sql::TableReference;
use futures::{stream, StreamExt};
use tokio::sync::broadcast;
use tonic::{Request, Response, Status, Streaming};

use crate::dataupdate::{DataUpdate, UpdateType};

use super::{metrics, Service};

#[allow(clippy::too_many_lines)]
pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoExchangeStream>, Status> {
    let _start = metrics::track_flight_request("do_exchange", None);
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

    let Some(table_provider) = flight_svc.datafusion.get_table(&data_path).await else {
        return Err(Status::invalid_argument(format!(
            r#"Unknown dataset: "{data_path}""#,
        )));
    };

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

    let table_provider_stream = Arc::clone(&table_provider);
    let response_stream = stream::unfold(rx, move |mut rx| {
        let encoder = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let write_options = writer::IpcWriteOptions::default();
        let table_provider = Arc::clone(&table_provider_stream);
        async move {
            match rx.recv().await {
                Ok(data_update) => {
                    let mut schema_sent: bool = false;

                    let mut flights = vec![];

                    for batch in &data_update.data {
                        // Convert record batch to match change schema
                        let schema = batch.schema();
                        let row_count = batch.num_rows();

                        // "r" stands for ChangeOperation::Read
                        let op_data = vec!["r"; row_count];
                        let op_array = StringArray::from(op_data);

                        let primary_keys_opt =
                            get_primary_keys_from_constraints(table_provider.constraints());

                        let primary_keys_array = match primary_keys_opt {
                            Some(pk) => {
                                let primary_keys = get_primary_keys(&schema, pk[0]);
                                get_primary_keys_array(&primary_keys, row_count)
                            }
                            None => ListArray::new_null(
                                Arc::new(Field::new("item", DataType::Utf8, false)),
                                row_count,
                            ),
                        };

                        let data_array = StructArray::from(batch.clone());

                        let new_schema = Arc::new(changes_schema(schema.as_ref()));
                        let Ok(new_record_batch) = RecordBatch::try_new(
                            Arc::clone(&new_schema),
                            vec![
                                Arc::new(op_array),
                                Arc::new(primary_keys_array),
                                Arc::new(data_array),
                            ],
                        ) else {
                            panic!("Unable to convert record batch into change event")
                        };

                        if !schema_sent {
                            flights.push(FlightData::from(SchemaAsIpc::new(
                                &new_schema,
                                &write_options,
                            )));
                            schema_sent = true;
                        }
                        let Ok((flight_dictionaries, flight_batch)) =
                            encoder.encoded_batch(&new_record_batch, &mut tracker, &write_options)
                        else {
                            panic!("Unable to encode batch")
                        };

                        flights.extend(flight_dictionaries.into_iter().map(Into::into));
                        flights.push(flight_batch.into());
                    }

                    metrics::DO_EXCHANGE_DATA_UPDATES_SENT.add(flights.len() as u64, &[]);
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
    let table_provider = Arc::clone(&table_provider);
    tokio::spawn(async move {
        let Ok(df) = datafusion.ctx.read_table(table_provider) else {
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
            let data_update = DataUpdate {
                data: vec![batch.clone()],
                schema,
                update_type: UpdateType::Append,
            };
            let _ = tx.send(data_update);
        }
    });

    Ok(Response::new(response_stream.boxed()))
}

fn get_primary_keys_from_constraints(
    constraints: Option<&Constraints>,
) -> Option<Vec<&Vec<usize>>> {
    constraints.map(|c| {
        c.iter()
            .filter_map(|c| match c {
                Constraint::PrimaryKey(pk) => Some(pk),
                Constraint::Unique(_) => None,
            })
            .collect::<Vec<_>>()
    })
}

fn get_primary_keys<'a>(schema: &'a SchemaRef, primary_key_idx: &[usize]) -> Vec<&'a str> {
    primary_key_idx
        .iter()
        .map(|idx| schema.field(*idx).name().as_str())
        .collect()
}

fn get_primary_keys_array(primary_keys: &[&str], row_count: usize) -> ListArray {
    let mut list_builder_generic = make_builder(
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        row_count,
    );
    let list_builder = list_builder_generic
        .as_any_mut()
        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
        .unwrap_or_else(|| unreachable!("created above as a list builder"));
    for _ in 0..row_count {
        let str_builder = list_builder
            .values()
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap_or_else(|| unreachable!("created above as a string builder"));
        for key in primary_keys {
            str_builder.append_value(key);
        }
        list_builder.append(true);
    }
    list_builder.finish()
}
