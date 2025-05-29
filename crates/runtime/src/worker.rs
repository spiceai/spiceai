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
use spicepod::component::worker::Worker as WorkerComponent;
use tokio::sync::RwLock;
use workers::RouterModel;

use crate::{Result, Runtime};

pub type WorkerRegistry = Arc<RwLock<HashMap<String, Arc<dyn Worker>>>>;

#[derive(Clone, Debug, Default, PartialEq)]
pub enum WorkerType {
    #[default]
    LoadBalance,
}

impl std::fmt::Display for WorkerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerType::LoadBalance => write!(f, "load_balance"),
        }
    }
}

fn infer_worker_type(worker: &WorkerComponent) -> Result<WorkerType> {
    if worker.load_balance.is_some() {
        Ok(WorkerType::LoadBalance)
    } else {
        Err(super::Error::FailedToInferWorkerType {
            name: worker.name.clone(),
        })
    }
}

pub fn try_construct_worker(worker: &WorkerComponent, rt: &Runtime) -> Result<Arc<dyn Worker>> {
    let worker_type = infer_worker_type(worker)?;

    match worker_type {
        WorkerType::LoadBalance => {
            let Some(load_balance) = &worker.load_balance else {
                unreachable!("LoadBalance worker must have load_balance defined");
            };

            let model = RouterModel::new(
                worker.name.clone(),
                load_balance.routing.as_slice(),
                Arc::clone(&rt.llms),
            );
            Ok(Arc::new(LoadBalanceWorker::new(
                Arc::new(model),
                worker.description.clone(),
            )))
        }
    }
}

#[async_trait]
pub trait Worker: Send + Sync {
    fn name(&self) -> Cow<'_, str>;

    fn description(&self) -> Option<Cow<'_, str>>;

    fn as_model(self: Arc<Self>) -> Option<Arc<dyn Chat>>;
}

pub struct LoadBalanceWorker {
    description: Option<String>,
    model: Arc<RouterModel>,
}

impl LoadBalanceWorker {
    pub fn new(model: Arc<RouterModel>, description: Option<String>) -> Self {
        Self { description, model }
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
}
