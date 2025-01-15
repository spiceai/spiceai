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

use anyhow::Result;
use reqwest::Client;
use tokio::task::JoinHandle;

use super::ConsistencyComponent;

pub type WorkerHandle = JoinHandle<ConsistencyWorkerResult>;
pub struct ConsistencyWorkerResult {
    /// The duration of requests, per bucket.
    pub durations: Vec<Vec<Duration>>,
    pub error_count: usize,
}

pub(crate) struct ConsistencyWorker {
    id: usize,
    duration: Duration,
    buckets: usize,
    client: Client,

    /// The component to test against.
    component: ConsistencyComponent,
}

impl ConsistencyWorker {
    pub fn new(
        id: usize,
        duration: Duration,
        buckets: usize,
        client: Client,
        component: ConsistencyComponent,
    ) -> Self {
        Self {
            id,
            duration,
            buckets,
            client,
            component,
        }
    }
    pub fn start(self) -> JoinHandle<ConsistencyWorkerResult> {
        tokio::spawn(async move {
            let mut durations: Vec<Vec<Duration>> = vec![vec![]; self.buckets];
            let bucket_duration = self.duration.as_secs() / self.buckets as u64;
            let mut error_count = 0;
            let start = Instant::now();

            while start.elapsed() < self.duration {
                let start_request = Instant::now();
                println!("Starting req..");
                match self.component.send_request(&self.client, "payload").await {
                    Ok(request_duration) => {
                        println!("Rook some time: {:?}", request_duration);
                        let idx = start_request
                            .duration_since(start)
                            .as_secs()
                            .div_euclid(bucket_duration) as usize;
                        durations[idx].push(request_duration);
                    }
                    Err(e) => {
                        eprintln!("Worker {} - Request failed: {}", self.id, e);
                        error_count += 1;
                        continue;
                    }
                }
            }

            ConsistencyWorkerResult {
                durations,
                error_count,
            }
        })
    }
}
