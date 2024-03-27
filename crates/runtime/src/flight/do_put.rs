/*
Copyright 2021-2024 The Spice.ai OSS Authors

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

use std::{collections::HashMap, sync::Arc};

use arrow_flight::{flight_service_server::FlightService, FlightData, PutResult};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use futures::stream;
use tokio::sync::{broadcast::Sender, RwLock};
use tonic::{Request, Response, Status, Streaming};

use crate::{
    dataupdate::{DataUpdate, UpdateType},
    timing::{TimeMeasurement, TimedStream},
};

use super::Service;

async fn get_sender_channel(
    channel_map: Arc<RwLock<HashMap<String, Arc<Sender<DataUpdate>>>>>,
    path: String,
) -> Option<Arc<Sender<DataUpdate>>> {
    let channel_map_read = channel_map.read().await;
    if channel_map_read.contains_key(&path) {
        let channel = channel_map_read.get(&path)?;
        Some(Arc::clone(channel))
    } else {
        None
    }
}

pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    let mut duration_metric = TimeMeasurement::new("flight_do_put_duration_ms", vec![]);
    let mut streaming_flight = request.into_inner();

    let Ok(Some(message)) = streaming_flight.message().await else {
        return Err(Status::invalid_argument("No flight data provided"));
    };
    let Some(fd) = &message.flight_descriptor else {
        return Err(Status::invalid_argument("No flight descriptor provided"));
    };
    if fd.path.is_empty() {
        return Err(Status::invalid_argument("No path provided"));
    };

    let path = fd.path.join(".");

    duration_metric.with_labels(vec![("path", path.clone())]);

    let df = flight_svc.datafusion.read().await;

    let Some(publishers) = df.get_publishers(&path) else {
        return Err(Status::invalid_argument(format!(
            "No publishers registered for path: {path}",
        )));
    };
    let dataset = Arc::clone(&publishers.0);
    let data_publishers = Arc::clone(&publishers.1);

    let schema = try_schema_from_flatbuffer_bytes(&message.data_header)
        .map_err(|e| Status::internal(format!("Failed to get schema from data header: {e}")))?;
    let schema = Arc::new(schema);
    let dictionaries_by_id = Arc::new(HashMap::new());

    // Sometimes the first message only contains the schema and no data
    let first_batch = arrow_flight::utils::flight_data_to_arrow_batch(
        &message,
        schema.clone(),
        &dictionaries_by_id,
    )
    .ok();

    let mut batches = vec![];
    if let Some(first_batch) = first_batch {
        batches.push(first_batch);
    }

    let channel_map = Arc::clone(&flight_svc.channel_map);

    let response_stream = stream::unfold(streaming_flight, move |mut flight| {
        let schema = Arc::clone(&schema);
        let dictionaries_by_id = Arc::clone(&dictionaries_by_id);
        let dataset = Arc::clone(&dataset);
        let data_publishers = Arc::clone(&data_publishers);
        let path = path.clone();
        let channel_map = Arc::clone(&channel_map);
        async move {
            match flight.message().await {
                Ok(Some(message)) => {
                    let new_batch = match arrow_flight::utils::flight_data_to_arrow_batch(
                        &message,
                        schema.clone(),
                        &dictionaries_by_id,
                    ) {
                        Ok(batches) => batches,
                        Err(e) => {
                            tracing::error!("Failed to convert flight data to batches: {e}");
                            return None;
                        }
                    };
                    tracing::trace!("Received batch with {} rows", new_batch.num_rows());

                    let data_update = DataUpdate {
                        data: vec![new_batch],
                        update_type: UpdateType::Append,
                    };

                    if let Some(channel) = get_sender_channel(channel_map, path).await {
                        let _ = channel.send(data_update.clone());
                    };

                    let data_publishers = data_publishers.read().await;
                    for publisher in data_publishers.iter() {
                        if let Err(err) = publisher
                            .add_data(Arc::clone(&dataset), data_update.clone())
                            .await
                            .map_err(|e| Status::internal(format!("Failed to add data: {e}")))
                        {
                            return Some((Err(err), flight));
                        };
                    }

                    Some((Ok(PutResult::default()), flight))
                }
                Ok(None) => {
                    // End of the stream
                    None
                }
                Err(e) => Some((
                    Err(Status::internal(format!("Error reading message: {e}"))),
                    flight,
                )),
            }
        }
    });

    let timed_stream = TimedStream::new(response_stream, move || duration_metric);

    Ok(Response::new(Box::pin(timed_stream)))
}
