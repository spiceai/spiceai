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
    channel::cron::CronRequestChannel,
    schedule::Schedule,
    scheduler::{Running, Scheduler, SchedulerBuilder},
};
use serde_json::Value;
use snafu::ResultExt;
use spicepod::component::worker::Worker;
use tokio::sync::RwLock;

use crate::{
    Result, Runtime,
    component::dataset::Dataset,
    scheduling::{DatasetRefreshTask, WorkerPromptTask},
};

const REFRESH_SCHEDULER_NAME: &str = "refresh_scheduler";
const WORKER_SCHEDULER_NAME: &str = "worker_scheduler";

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

    pub async fn create_worker_schedule(self: Arc<Self>, worker: Worker) -> Result<()> {
        let (start_cron, prompt) = match (worker.cron.clone(), worker.params.get("prompt")) {
            (Some(cron), Some(Value::String(prompt))) => (cron, prompt.clone()),
            (Some(_), None) => {
                tracing::warn!(
                    "Worker '{}' has a 'start_cron' but no prompt is specified.\nThe worker will not be scheduled to run.\nSpecify a 'prompt' parameter and try again.",
                    worker.name
                );
                return Ok(());
            }
            (None, Some(Value::String(_))) => {
                tracing::warn!(
                    "Worker '{}' has a 'prompt' but no 'start_cron' is specified.\nThe worker will not be scheduled to run.\nSpecify a 'start_cron' parameter and try again.",
                    worker.name
                );
                return Ok(());
            }
            (_, Some(v)) => {
                tracing::warn!(
                    "Worker '{}' has a 'prompt' but it is not a string: {v}.\nThe worker will not be scheduled to run.\nSpecify a valid 'prompt' parameter and try again.",
                    worker.name,
                );
                return Ok(());
            }
            (None, None) => {
                tracing::debug!(
                    "Worker {} has no start cron or prompt, skipping schedule creation",
                    worker.name
                );
                return Ok(());
            }
        };

        let scheduler_lock = Arc::clone(&self.schedulers);
        let mut schedulers = scheduler_lock.write().await;
        let worker_name = worker.name.to_string().into();

        let worker_prompt_task = Arc::new(WorkerPromptTask::new(
            Arc::clone(&self),
            Arc::new(worker),
            Arc::from(prompt),
        ));

        let cron_request_channel = Arc::new(RwLock::new(
            CronRequestChannel::new(&start_cron.clone().into()).context(
                crate::FailedToCreateCronChannelSnafu {
                    cron: start_cron.clone(),
                },
            )?,
        ));

        let schedule = Arc::new(
            Schedule::new(Arc::clone(&worker_name), worker_prompt_task)
                .add_trigger(cron_request_channel),
        );

        tracing::debug!("Creating worker schedule for worker: {worker_name}");

        if let Some(scheduler) = schedulers.get(WORKER_SCHEDULER_NAME) {
            if scheduler
                .schedules()
                .await
                .iter()
                .any(|s| s.name() == schedule.name())
            {
                tracing::debug!(
                    "Worker schedule already exists in worker scheduler for worker: {worker_name}",
                );
                return Ok(());
            }

            tracing::debug!(
                "Adding worker schedule to existing worker scheduler for worker: {worker_name}",
            );
            scheduler
                .add_schedule(schedule)
                .await
                .context(crate::FailedToAddScheduleSnafu {
                    name: worker_name.to_string(),
                    scheduler: WORKER_SCHEDULER_NAME.to_string(),
                })?;
            return Ok(());
        }

        // create a new 'worker_scheduler' if it doesn't exist
        tracing::debug!("Creating new worker scheduler for worker schedule: {worker_name}",);

        let scheduler = Arc::new(
            SchedulerBuilder::new(WORKER_SCHEDULER_NAME.into())
                .add_schedule(schedule)
                .build()
                .context(crate::FailedToBuildSchedulerSnafu)?
                .start()
                .await
                .context(crate::FailedToStartSchedulerSnafu)?,
        );
        schedulers.insert(WORKER_SCHEDULER_NAME.into(), Arc::clone(&scheduler));
        tracing::debug!(
            "Worker scheduler created for worker '{worker_name}' with cron: {start_cron}",
        );

        Ok(())
    }

    pub async fn remove_worker_schedule(self: Arc<Self>, worker: Worker) -> Result<()> {
        let scheduler_lock = Arc::clone(&self.schedulers);
        let schedulers = scheduler_lock.read().await;
        let worker_name = worker.name.to_string().into();

        if let Some(scheduler) = schedulers.get(WORKER_SCHEDULER_NAME) {
            if scheduler
                .schedules()
                .await
                .iter()
                .any(|s| s.name() == worker_name)
            {
                tracing::debug!("Removing worker schedule for worker: {worker_name}",);
                scheduler
                    .remove_schedule(Arc::clone(&worker_name))
                    .await
                    .context(crate::FailedToRemoveScheduleSnafu {
                        name: worker_name.to_string(),
                        scheduler: WORKER_SCHEDULER_NAME.to_string(),
                    })?;
            } else {
                tracing::debug!("No worker schedule found for worker: {worker_name}",);
            }
        } else {
            tracing::debug!(
                "No worker scheduler found, cannot remove schedule for worker: {worker_name}",
            );
        }

        Ok(())
    }
}
