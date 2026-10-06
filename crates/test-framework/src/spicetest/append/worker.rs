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
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::Result;
use tokio::task::JoinHandle;

use crate::queries::QuerySet;

use super::sources::AppendableSource;

pub(crate) struct AppendConfig {
    pub(crate) end_duration: Duration,
    pub(crate) query_set: QuerySet,
    pub(crate) load_steps: u16,
    pub(crate) load_interval: Duration,
    pub(crate) temp_directory: PathBuf,
    pub(crate) with_conflict_data: bool,
    pub(crate) with_retention_data: bool,
}

impl AppendConfig {
    pub fn new(end_duration: Duration, query_set: QuerySet, temp_directory: PathBuf) -> Self {
        Self {
            end_duration,
            query_set,
            load_steps: 10,
            load_interval: Duration::from_mins(4),
            temp_directory,
            with_conflict_data: false,
            with_retention_data: false,
        }
    }

    pub fn with_load_interval(mut self, load_interval: Duration) -> Self {
        self.load_interval = load_interval;
        self
    }

    pub fn with_load_steps(mut self, load_steps: u16) -> Self {
        self.load_steps = load_steps;
        self
    }

    pub fn with_conflict_data(mut self, with_conflict_data: bool) -> Self {
        self.with_conflict_data = with_conflict_data;
        self
    }

    pub fn with_retention_test_data(mut self, with_retention_test_data: bool) -> Self {
        self.with_retention_data = with_retention_test_data;
        self
    }
}

pub(crate) struct AppendWorker {
    config: AppendConfig,
    source: Box<dyn AppendableSource>,
}

impl AppendWorker {
    pub fn new(config: AppendConfig, source: Box<dyn AppendableSource>) -> Self {
        Self { config, source }
    }

    /// Writes the initial data, which `spiced` loads on startup, so it must run
    /// before `spiced` starts.
    pub async fn setup(&self) -> Result<()> {
        println!("AppendWorker - Running append data setup");
        self.source.setup(&self.config).await
    }

    /// Loads every step, then waits out the rest of the test duration, both
    /// counted from this call. Call it once the query workers are running, so
    /// every load lands while queries are running.
    ///
    /// Loads are paced by `load_interval`, but never cut off by the test
    /// duration: generating the data is the harness's own work, so a slow runner
    /// lengthens the run instead of failing it. Only a run whose loads exceed
    /// twice the expected length fails, which catches a stuck load.
    pub fn start_loads(self) -> JoinHandle<Result<()>> {
        let end_time = Instant::now() + self.config.end_duration;
        let load_steps = self.config.load_steps;
        let load_timeout = self.load_timeout();
        tokio::spawn(async move {
            println!("AppendWorker - Starting append data generation");
            let mut loaded = 1;
            let loads = async {
                for load_index in 1..load_steps {
                    tokio::time::sleep(self.config.load_interval).await;
                    self.source.generate(&self.config, load_index).await?;
                    loaded += 1;
                }
                Ok::<(), anyhow::Error>(())
            };
            let loads_result = tokio::time::timeout(load_timeout, loads).await;

            if matches!(loads_result, Ok(Ok(()))) {
                // Keep the query workers running for the full test duration.
                tokio::time::sleep(end_time.saturating_duration_since(Instant::now())).await;
            }

            println!("AppendWorker - Running append data teardown");
            self.source.teardown(&self.config).await?;

            match loads_result {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!(
                    "Append loads did not finish within {load_timeout:?}. Only loaded {loaded}/{load_steps}"
                )),
            }
        })
    }

    /// Twice the longer of the test duration and the paced loads alone.
    fn load_timeout(&self) -> Duration {
        let paced_loads = self
            .config
            .load_interval
            .saturating_mul(u32::from(self.config.load_steps.saturating_sub(1)));
        self.config.end_duration.max(paced_loads).saturating_mul(2)
    }
}
