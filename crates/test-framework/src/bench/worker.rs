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

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use anyhow::Result;
use flight_client::FlightClient;
use futures::StreamExt;
use tokio::task::JoinHandle;

pub(crate) struct BenchQueryWorker {
    iterations: usize,
    flight_client: FlightClient,
    query_set: Vec<(&'static str, &'static str)>,
}

pub struct BenchQueryWorkerResult {
    pub query_durations: BTreeMap<String, Vec<Duration>>,
    pub connection_failed: bool,
}

impl BenchQueryWorkerResult {
    pub fn new(query_durations: BTreeMap<String, Vec<Duration>>, connection_failed: bool) -> Self {
        Self {
            query_durations,
            connection_failed,
        }
    }
}

impl BenchQueryWorker {
    pub fn new(
        iterations: usize,
        flight_client: FlightClient,
        query_set: Vec<(&'static str, &'static str)>,
    ) -> Self {
        Self {
            iterations,
            flight_client,
            query_set,
        }
    }

    pub fn start(self) -> JoinHandle<Result<BenchQueryWorkerResult>> {
        tokio::spawn(async move {
            let mut query_durations: BTreeMap<String, Vec<Duration>> = BTreeMap::new();

            for query in &self.query_set {
                for idx in 0..self.iterations {
                    let query_start = Instant::now();
                    eprintln!("Running Query '{}'", query.0);
                    match self.flight_client.query(query.1).await {
                        Ok(mut result_stream) => {
                            while let Some(batch) = result_stream.next().await {
                                match batch {
                                    Ok(batch) => {}
                                    Err(e) => {
                                        eprintln!("FAIL - Query '{}' failed: {}", query.0, e);
                                        query_durations.entry(query.0.to_string()).or_default();
                                    }
                                }
                            }
                            let duration = query_start.elapsed();
                            query_durations
                                .entry(query.0.to_string())
                                .or_default()
                                .push(duration);
                        }
                        Err(e) => match e {
                            flight_client::Error::UnableToConnectToServer { .. }
                            | flight_client::Error::UnableToPerformHandshake { .. } => {
                                eprintln!("FAIL - EARLY EXIT - Query '{}' failed: {}", query.0, e);
                                return Ok(BenchQueryWorkerResult::new(query_durations, true));
                            }
                            _ => {
                                eprintln!("FAIL - Query '{}' failed: {}", query.0, e);
                                query_durations.entry(query.0.to_string()).or_default();
                            }
                        },
                    };
                }
            }
            Ok(BenchQueryWorkerResult::new(query_durations, false))
        })
    }
}
