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

use std::{collections::HashMap, sync::Arc};

use scheduler::{
    schedule::Schedule,
    scheduler::{Running, Scheduler, SchedulerBuilder},
};
use snafu::ResultExt;
use tokio::sync::RwLock;

use crate::{Result, Runtime, component::dataset::Dataset, scheduling::DatasetRefreshTask};

const REFRESH_SCHEDULER_NAME: &str = "refresh_scheduler";

pub(crate) type ScheduleRegistry = RwLock<HashMap<Arc<str>, Arc<Scheduler<Running>>>>;

impl Runtime {
    pub async fn create_dataset_schedule(self: Arc<Self>, dataset: Arc<Dataset>) -> Result<()> {
        // TODO: Actually schedule the refresh task for cron - https://github.com/spiceai/spiceai/issues/6015
        if dataset.refresh_cron().is_none() {
            return Ok(());
        }

        tracing::debug!("Creating dataset scheduler for dataset: {}", dataset.name);
        let scheduler_lock = Arc::clone(&self.schedulers);
        let mut schedulers = scheduler_lock.write().await;
        let dataset_name = dataset.name.to_string().into();

        let refresh_task = Arc::new(DatasetRefreshTask::from(Arc::clone(&dataset)));
        let schedule = Arc::new(Schedule::new(Arc::clone(&dataset_name), refresh_task));

        // a `refresh_scheduler` exists but does not contain this dataset's schedule
        if let Some(scheduler) = schedulers.get(REFRESH_SCHEDULER_NAME) {
            if scheduler
                .schedules()
                .await
                .iter()
                .any(|s| s.name() == schedule.name())
            {
                tracing::debug!(
                    "Dataset schedule already exists in refresh scheduler for dataset: {}",
                    dataset.name
                );
                return Ok(());
            }

            tracing::debug!(
                "Adding dataset schedule to existing refresh scheduler for dataset: {}",
                dataset.name
            );
            scheduler
                .add_schedule(schedule)
                .await
                .context(crate::FailedToAddScheduleSnafu {
                    name: dataset_name.to_string(),
                    scheduler: REFRESH_SCHEDULER_NAME.to_string(),
                })?;
            return Ok(());
        }

        // no `refresh_scheduler` exists, create a new one
        tracing::debug!(
            "Creating new refresh scheduler for dataset schedule: {}",
            dataset.name
        );
        let scheduler = Arc::new(
            SchedulerBuilder::new(REFRESH_SCHEDULER_NAME.into())
                .add_schedule(schedule)
                .build()
                .context(crate::FailedToBuildSchedulerSnafu)?
                .start()
                .await
                .context(crate::FailedToStartSchedulerSnafu)?,
        );

        schedulers.insert(REFRESH_SCHEDULER_NAME.into(), Arc::clone(&scheduler));

        Ok(())
    }
}
