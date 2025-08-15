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

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use llms::chat::Chat;
use serde_json::Value;
use spicepod::component::worker::Worker as WorkerComponent;
use tokio::sync::RwLock;
use workers::RouterModel;

use crate::{Result, Runtime};

pub type WorkerRegistry = Arc<RwLock<HashMap<String, Arc<dyn Worker>>>>;

#[derive(Clone, Debug, Default, PartialEq)]
pub enum WorkerType {
    #[default]
    LoadBalance,
    Sql,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkerScheduleParameters {
    Sql { cron: String, sql: String },
    Prompt { cron: String, prompt: String },
}

impl WorkerScheduleParameters {
    pub fn cron(&self) -> String {
        match self {
            WorkerScheduleParameters::Prompt { cron, .. }
            | WorkerScheduleParameters::Sql { cron, .. } => cron.clone(),
        }
    }
}

impl std::fmt::Display for WorkerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerType::LoadBalance => write!(f, "load_balance"),
            WorkerType::Sql => write!(f, "sql"),
        }
    }
}

#[allow(clippy::result_large_err)]
fn infer_worker_type(worker: &WorkerComponent) -> Result<WorkerType> {
    match (worker.load_balance.as_ref(), worker.sql.as_ref()) {
        (Some(_), None) => Ok(WorkerType::LoadBalance),
        (None, Some(_)) => Ok(WorkerType::Sql),
        _ => Err(super::Error::FailedToInferWorkerType {
            name: worker.name.clone(),
        }),
    }
}

#[allow(clippy::result_large_err)]
pub fn try_construct_worker(worker: &WorkerComponent, rt: &Runtime) -> Result<Arc<dyn Worker>> {
    let worker_type = infer_worker_type(worker)?;

    match worker_type {
        WorkerType::LoadBalance => {
            let Some(load_balance) = &worker.load_balance else {
                unreachable!("LoadBalance worker must have load_balance defined");
            };

            let schedule_parameters = match (worker.cron.clone(), worker.params.get("prompt")) {
                (Some(cron), Some(Value::String(prompt))) => {
                    Some(WorkerScheduleParameters::Prompt {
                        cron,
                        prompt: prompt.clone(),
                    })
                }
                (Some(_), None) => {
                    tracing::warn!(
                        "Worker '{}' has a 'cron' but no prompt is specified.\nThe worker will not be scheduled to run.\nSpecify a 'prompt' parameter and try again.",
                        worker.name
                    );
                    None
                }
                (None, Some(Value::String(_))) => {
                    tracing::warn!(
                        "Worker '{}' has a 'prompt' but no 'cron' is specified.\nThe worker will not be scheduled to run.\nSpecify a 'cron' parameter and try again.",
                        worker.name
                    );
                    None
                }
                (_, Some(v)) => {
                    tracing::warn!(
                        "Worker '{}' has a 'prompt' but it is not a string: {v}.\nThe worker will not be scheduled to run.\nSpecify a valid 'prompt' parameter and try again.",
                        worker.name,
                    );
                    None
                }
                (None, None) => {
                    tracing::debug!(
                        "Worker {} has no cron or prompt, skipping schedule creation",
                        worker.name
                    );
                    None
                }
            };

            let model = RouterModel::new(
                worker.name.clone(),
                load_balance.routing.as_slice(),
                Arc::clone(&rt.llms),
            );
            Ok(Arc::new(LoadBalanceWorker::new(
                Arc::new(model),
                worker.description.clone(),
                schedule_parameters,
            )))
        }
        WorkerType::Sql => {
            let Some(sql) = &worker.sql else {
                unreachable!("SQL worker must have sql defined");
            };

            let schedule_parameters = if let Some(cron) = &worker.cron {
                Some(WorkerScheduleParameters::Sql {
                    cron: cron.clone(),
                    sql: sql.clone(),
                })
            } else {
                tracing::debug!(
                    "Worker {} has no cron, skipping schedule creation",
                    worker.name
                );
                None
            };

            Ok(Arc::new(SQLWorker::new(
                worker.name.clone(),
                worker.description.clone(),
                schedule_parameters,
            )))
        }
    }
}

#[async_trait]
pub trait Worker: Send + Sync {
    fn name(&self) -> Cow<'_, str>;

    fn description(&self) -> Option<Cow<'_, str>>;

    fn as_model(self: Arc<Self>) -> Option<Arc<dyn Chat>> {
        None
    }

    fn schedule_parameters(&self) -> Option<WorkerScheduleParameters> {
        None
    }
}

pub struct LoadBalanceWorker {
    description: Option<String>,
    model: Arc<RouterModel>,
    schedule_parameters: Option<WorkerScheduleParameters>,
}

impl LoadBalanceWorker {
    pub fn new(
        model: Arc<RouterModel>,
        description: Option<String>,
        schedule_parameters: Option<WorkerScheduleParameters>,
    ) -> Self {
        Self {
            description,
            model,
            schedule_parameters,
        }
    }
}

impl Worker for LoadBalanceWorker {
    fn name(&self) -> Cow<'_, str> {
        self.model.router_name.clone().into()
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.description.as_ref().map(Into::into)
    }

    fn as_model(self: Arc<Self>) -> Option<Arc<dyn Chat>> {
        let model = Arc::clone(&self.model) as Arc<dyn Chat>;
        Some(model)
    }

    fn schedule_parameters(&self) -> Option<WorkerScheduleParameters> {
        self.schedule_parameters.clone()
    }
}

pub struct SQLWorker {
    name: String,
    description: Option<String>,
    schedule_parameters: Option<WorkerScheduleParameters>,
}

impl SQLWorker {
    pub fn new(
        name: String,
        description: Option<String>,
        schedule_parameters: Option<WorkerScheduleParameters>,
    ) -> Self {
        Self {
            name,
            description,
            schedule_parameters,
        }
    }
}

impl Worker for SQLWorker {
    fn name(&self) -> Cow<'_, str> {
        self.name.clone().into()
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.description.as_ref().map(Into::into)
    }

    fn schedule_parameters(&self) -> Option<WorkerScheduleParameters> {
        self.schedule_parameters.clone()
    }
}
