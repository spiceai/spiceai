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

use std::sync::Arc;

use crate::{Runtime, metrics, status, timing::TimeMeasurement, worker::try_construct_worker};
use opentelemetry::KeyValue;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {}

impl Runtime {
    pub(crate) async fn load_workers(&self) {
        let app_lock = self.app.read().await;

        if let Some(app) = app_lock.as_ref() {
            for worker in &app.workers {
                self.status
                    .update_worker(&worker.name, status::ComponentStatus::Initializing);
                self.load_worker(worker).await;
            }
        }
    }

    async fn load_worker(&self, cfg: &spicepod::component::worker::Worker) {
        let _guard = TimeMeasurement::new(
            &metrics::workers::LOAD_DURATION_MS,
            &[KeyValue::new("worker", cfg.name.clone())],
        );

        tracing::info!("Loading worker [{}]...", cfg.name);

        let worker = match try_construct_worker(&cfg.r#type, cfg, self) {
            Ok(worker) => worker,
            Err(e) => {
                tracing::error!("Failed to load worker [{}]: {e}", cfg.name);
                self.status
                    .update_worker(&cfg.name, status::ComponentStatus::Error);
                return;
            }
        };

        if let Some(model) = Arc::clone(&worker).as_model() {
            let mut llm_registry = self.llms.write().await;
            llm_registry.insert(cfg.name.clone(), model);
            drop(llm_registry);
        }

        let mut worker_registry = self.workers.write().await;
        worker_registry.insert(cfg.name.clone(), worker);

        tracing::info!("Worker [{}] loaded, ready for use", cfg.name);
        metrics::workers::COUNT.add(1, &[KeyValue::new("worker", cfg.name.clone())]);
        self.status
            .update_worker(&cfg.name, status::ComponentStatus::Ready);
    }

    async fn remove_worker(&self, cfg: &spicepod::component::worker::Worker) {
        let mut llm_registry = self.llms.write().await;
        llm_registry.remove(&cfg.name);

        tracing::info!("Worker [{}] has been unloaded", cfg.name);
        metrics::workers::COUNT.add(-1, &[KeyValue::new("worker", cfg.name.clone())]);
    }

    async fn update_worker(&self, worker_config: &spicepod::component::worker::Worker) {
        self.status
            .update_worker(&worker_config.name, status::ComponentStatus::Refreshing);
        self.remove_worker(worker_config).await;
        self.load_worker(worker_config).await;
    }

    pub(crate) async fn apply_worker_diff(
        &self,
        current_app: &Arc<app::App>,
        new_app: &Arc<app::App>,
    ) {
        // Remove workers that are no longer in the app
        for worker in &current_app.workers {
            if !new_app.workers.iter().any(|w| w.name == worker.name) {
                self.status
                    .update_worker(&worker.name, status::ComponentStatus::Disabled);
                self.remove_worker(worker).await;
            }
        }

        for worker in &new_app.workers {
            if let Some(current_worker) = current_app.workers.iter().find(|w| w.name == worker.name)
            {
                if current_worker != worker {
                    self.update_worker(worker).await;
                }
            } else {
                self.status
                    .update_worker(&worker.name, status::ComponentStatus::Initializing);
                self.load_worker(worker).await;
            }
        }
    }
}
