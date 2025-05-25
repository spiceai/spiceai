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

use std::{borrow::Cow, str::FromStr, sync::Arc};

use async_trait::async_trait;
use llms::chat::Chat;
use serde::{Deserialize, Serialize};
use spicepod::component::worker::Worker as WorkerComponent;
use workers::RouterModel;

use crate::Runtime;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum WorkerType {
    #[default]
    LoadBalance,
}

impl WorkerType {
    pub fn construct_worker(&self, worker: &WorkerComponent, rt: &Runtime) -> Arc<dyn Worker> {
        match self {
            WorkerType::LoadBalance => {
                let model = RouterModel::new(
                    worker.name.clone(),
                    worker.models.clone(),
                    Arc::clone(&rt.llms),
                );
                Arc::new(LoadBalanceWorker::new(
                    Arc::new(model),
                    worker.description.clone(),
                ))
            }
        }
    }
}

impl FromStr for WorkerType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "load_balance" => Ok(WorkerType::LoadBalance),
            _ => Err(format!("Unknown worker type: {s}")),
        }
    }
}

#[async_trait]
pub trait Worker: Send + Sync {
    fn name(&self) -> Cow<'_, str>;

    fn role(&self) -> WorkerType;

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

    fn role(&self) -> WorkerType {
        WorkerType::LoadBalance
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.description.as_ref().map(Into::into)
    }

    fn as_model(self: Arc<Self>) -> Option<Arc<dyn Chat>> {
        let model = Arc::clone(&self.model) as Arc<dyn Chat>;
        Some(model)
    }
}
