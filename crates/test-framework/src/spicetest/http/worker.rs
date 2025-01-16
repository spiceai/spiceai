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
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use rand::Rng;
use reqwest::Client;
use tokio::task::JoinHandle;

use super::{HttpComponent, HttpConfig};

pub type WorkerHandle = JoinHandle<Result<HttpWorkerResult>>;

#[derive(Default)]
pub struct HttpWorkerResult {
    /// The duration of requests, per bucket.
    pub durations: Vec<Vec<Duration>>,
    pub error_count: usize,
}

pub(crate) struct HttpWorker {
    id: usize,
    duration: Duration,
    buckets: usize,
    client: Client,

    /// The component to test against.
    component: HttpComponent,

    payload: Vec<Arc<str>>,
}

impl HttpWorker {
    pub fn new(id: usize, cfg: HttpConfig, client: Client) -> Self {
        Self {
            id,
            duration: cfg.duration,
            buckets: cfg.buckets,
            client,
            component: cfg.component,
            payload: cfg.payloads,
        }
    }

    pub fn start(self) -> WorkerHandle {
        tokio::spawn(async move {
            let mut durations: Vec<Vec<Duration>> = vec![vec![]; self.buckets];
            let bucket_duration = self.duration.as_secs() / self.buckets as u64;
            let mut error_count = 0;
            let start = Instant::now();

            while start.elapsed() < self.duration {
                let start_request = Instant::now();
                let Some(p) = get_random_element(&self.payload) else {
                    eprintln!("Worker {} - No payload found. Exiting...", self.id);
                    return Ok(HttpWorkerResult::default());
                };
                match self
                    .component
                    .send_request(&self.client, &Arc::clone(p))
                    .await
                {
                    Ok(request_duration) => {
                        let idx = usize::try_from(
                            start_request
                                .duration_since(start)
                                .as_secs()
                                .div_euclid(bucket_duration),
                        )?;
                        durations[idx].push(request_duration);
                    }
                    Err(e) => {
                        eprintln!("Worker {} - Request failed: {}", self.id, e);
                        error_count += 1;
                        continue;
                    }
                }
            }

            Ok(HttpWorkerResult {
                durations,
                error_count,
            })
        })
    }
}

fn get_random_element<T>(vec: &[T]) -> Option<&T> {
    if vec.is_empty() {
        None
    } else {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..vec.len());
        Some(&vec[index])
    }
}
