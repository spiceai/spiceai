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
            load_interval: Duration::from_secs(60 * 4),
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

    pub async fn start(self) -> Result<JoinHandle<Result<()>>> {
        // Outside of the join handle, run some initial setup
        // This ensures the appendable dataset is ready before the workers start
        let end_time = Instant::now() + self.config.end_duration;
        println!("AppendWorker - Running append data setup");
        self.source.setup(&self.config).await?;

        let mut load_index = 1;
        Ok(tokio::spawn(async move {
            println!("AppendWorker - Starting append data generation");
            while Instant::now() < end_time {
                if load_index >= self.config.load_steps {
                    tokio::time::sleep(self.config.load_interval).await; // don't break here - we don't want teardown to run before the end time
                    continue;
                }

                tokio::time::sleep(self.config.load_interval).await;
                self.source.generate(&self.config, load_index).await?;

                load_index += 1;
            }

            println!("AppendWorker - Running append data teardown");
            self.source.teardown(&self.config).await?;

            if load_index < self.config.load_steps {
                return Err(anyhow::anyhow!(
                    "Failed to load all append data in time. Only loaded {load_index}/{load_steps}",
                    load_steps = self.config.load_steps
                ));
            }

            Ok(())
        }))
    }
}
