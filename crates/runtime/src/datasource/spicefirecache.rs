use async_stream::stream;
use futures::StreamExt;
use futures_core::stream::Stream;
use spice_rs::Client;
use std::time::Duration;

pub struct SpiceFirecache {
    pub spice_client: Client,
    pub sleep_duration: Duration,
}

impl super::DataSource for SpiceFirecache {
    fn get_data(&self) -> impl Stream<Item = super::DataUpdate> {
        stream! {
            loop {
                tokio::time::sleep(self.sleep_duration).await;

                let flight_record_batch_stream_result = self.spice_client.fire_query("SELECT * FROM eth.recent_blocks").await.map_err(|e| e.to_string());
                let mut flight_record_batch_stream = match flight_record_batch_stream_result {
                        Ok(stream) => stream,
                        Err(error) => {
                            println!("Failed to query with spice client: {:?}", error);
                            continue;
                        },
                };

                let mut result_data = vec![];
                while let Some(batch) = flight_record_batch_stream.next().await {
                    match batch {
                        Ok(batch) => { result_data.push(batch); },
                        Err(error) => {
                            println!("Failed to read batch from spice client: {:?}", error);
                            continue;
                        },
                    };
                };

                yield super::DataUpdate {
                    log_sequence_number: 0,
                    data: result_data,
                };
            }
        }
    }
}
