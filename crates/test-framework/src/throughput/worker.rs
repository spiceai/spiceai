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

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use flight_client::FlightClient;
use tokio::task::JoinHandle;

use super::EndCondition;

pub(crate) struct ThroughputQueryWorker {
    id: usize,
    query_set: Vec<(&'static str, &'static str)>,
    end_condition: EndCondition,
    flight_client: FlightClient,
}

impl ThroughputQueryWorker {
    pub fn new(
        id: usize,
        query_set: Vec<(&'static str, &'static str)>,
        end_condition: EndCondition,
        flight_client: FlightClient,
    ) -> Self {
        Self {
            id,
            query_set,
            end_condition,
            flight_client,
        }
    }

    pub fn start(self) -> JoinHandle<Result<Duration>> {
        tokio::spawn(async move {
            let mut total_query_duration = Duration::ZERO;
            let mut query_set_count = 0;
            let start = Instant::now();

            while !self.end_condition.is_met(&start, query_set_count) {
                for query in &self.query_set {
                    let query_start = Instant::now();
                    match self.flight_client.query(query.1).await {
                        Ok(_) => (),
                        Err(e) => {
                            return Err(e).context(format!(
                                "Query worker {} failed to execute query {}",
                                self.id, query.0
                            ));
                        }
                    };
                    let duration = query_start.elapsed();
                    total_query_duration += duration;
                }
                query_set_count += 1;
            }
            Ok(total_query_duration)
        })
    }
}
