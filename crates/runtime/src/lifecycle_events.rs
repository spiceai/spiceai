/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use chrono::Utc;
use serde::Serialize;
use spicepod::component::worker::EventFilters;
use tokio::sync::RwLock;

use crate::status::{ComponentStatus, ComponentStatusUpdate, RuntimeStatus};

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEventLevel {
    Success,
    Info,
    Error,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEventTopic {
    Registration,
    Datasets,
    Views,
    Models,
    SearchIndexing,
    EmbeddingVectorization,
    Accelerations,
    Refreshes,
    Crons,
    Workers,
    Other,
}

#[derive(Clone, Debug, Serialize)]
pub struct LifecycleEvent {
    pub timestamp: String,
    pub level: LifecycleEventLevel,
    pub topic: LifecycleEventTopic,
    pub component: String,
    pub source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl LifecycleEvent {
    #[must_use]
    pub fn new(
        level: LifecycleEventLevel,
        topic: LifecycleEventTopic,
        component: String,
        source: &'static str,
        status: Option<String>,
        message: Option<String>,
    ) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            level,
            topic,
            component,
            source,
            status,
            message,
        }
    }
}

impl LifecycleEventLevel {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            LifecycleEventLevel::Success => "success",
            LifecycleEventLevel::Info => "info",
            LifecycleEventLevel::Error => "error",
        }
    }
}

impl LifecycleEventTopic {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            LifecycleEventTopic::Registration => "registration",
            LifecycleEventTopic::Datasets => "datasets",
            LifecycleEventTopic::Views => "views",
            LifecycleEventTopic::Models => "models",
            LifecycleEventTopic::SearchIndexing => "search_indexing",
            LifecycleEventTopic::EmbeddingVectorization => "embedding_vectorization",
            LifecycleEventTopic::Accelerations => "accelerations",
            LifecycleEventTopic::Refreshes => "refreshes",
            LifecycleEventTopic::Crons => "crons",
            LifecycleEventTopic::Workers => "workers",
            LifecycleEventTopic::Other => "other",
        }
    }
}

#[derive(Clone, Debug)]
pub struct WebhookWorkerConfig {
    pub name: String,
    pub url: String,
    pub filters: LifecycleEventFilters,
}

#[derive(Clone, Copy, Debug)]
pub struct LifecycleEventFilters {
    success: bool,
    info: bool,
    errors: bool,
    registration: bool,
    datasets: bool,
    views: bool,
    models: bool,
    search_indexing: bool,
    embedding_vectorization: bool,
    accelerations: bool,
    refreshes: bool,
    crons: bool,
}

impl LifecycleEventFilters {
    #[must_use]
    pub fn from_event_filters(events: Option<&EventFilters>) -> Self {
        let Some(events) = events else {
            return Self::all_enabled();
        };

        let has_explicit = events.success.is_some()
            || events.info.is_some()
            || events.errors.is_some()
            || events.registration.is_some()
            || events.datasets.is_some()
            || events.views.is_some()
            || events.models.is_some()
            || events.search_indexing.is_some()
            || events.embedding_vectorization.is_some()
            || events.accelerations.is_some()
            || events.refreshes.is_some()
            || events.crons.is_some();

        let default_value = !has_explicit;

        Self {
            success: events.success.map_or(default_value, |t| t.is_enabled()),
            info: events.info.map_or(default_value, |t| t.is_enabled()),
            errors: events.errors.map_or(default_value, |t| t.is_enabled()),
            registration: events.registration.map_or(default_value, |t| t.is_enabled()),
            datasets: events.datasets.map_or(default_value, |t| t.is_enabled()),
            views: events.views.map_or(default_value, |t| t.is_enabled()),
            models: events.models.map_or(default_value, |t| t.is_enabled()),
            search_indexing: events.search_indexing.map_or(default_value, |t| t.is_enabled()),
            embedding_vectorization: events
                .embedding_vectorization
                .map_or(default_value, |t| t.is_enabled()),
            accelerations: events.accelerations.map_or(default_value, |t| t.is_enabled()),
            refreshes: events.refreshes.map_or(default_value, |t| t.is_enabled()),
            crons: events.crons.map_or(default_value, |t| t.is_enabled()),
        }
    }

    fn all_enabled() -> Self {
        Self {
            success: true,
            info: true,
            errors: true,
            registration: true,
            datasets: true,
            views: true,
            models: true,
            search_indexing: true,
            embedding_vectorization: true,
            accelerations: true,
            refreshes: true,
            crons: true,
        }
    }

    #[must_use]
    pub fn allows(&self, event: &LifecycleEvent) -> bool {
        let level_allowed = match event.level {
            LifecycleEventLevel::Success => self.success,
            LifecycleEventLevel::Info => self.info,
            LifecycleEventLevel::Error => self.errors,
        };

        let topic_allowed = match event.topic {
            LifecycleEventTopic::Registration => self.registration,
            LifecycleEventTopic::Datasets => self.datasets,
            LifecycleEventTopic::Views => self.views,
            LifecycleEventTopic::Models => self.models,
            LifecycleEventTopic::SearchIndexing => self.search_indexing,
            LifecycleEventTopic::EmbeddingVectorization => self.embedding_vectorization,
            LifecycleEventTopic::Accelerations => self.accelerations,
            LifecycleEventTopic::Refreshes => self.refreshes,
            LifecycleEventTopic::Crons => self.crons,
            LifecycleEventTopic::Workers | LifecycleEventTopic::Other => true,
        };

        level_allowed && topic_allowed
    }
}

#[derive(Default)]
pub struct LifecycleEventDispatcher {
    workers: Arc<RwLock<HashMap<String, WebhookWorkerConfig>>>,
    client: Option<reqwest::Client>,
}

impl LifecycleEventDispatcher {
    #[must_use]
    pub fn new() -> Self {
        let client = crate::dataconnector::default_spice_client("application/json").ok();
        Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            client,
        }
    }

    pub async fn register_webhook_worker(&self, config: WebhookWorkerConfig) {
        let mut workers = self.workers.write().await;
        workers.insert(config.name.clone(), config);
    }

    pub async fn unregister_webhook_worker(&self, name: &str) {
        let mut workers = self.workers.write().await;
        workers.remove(name);
    }

    pub fn spawn_status_update_listener(self: Arc<Self>, status: Arc<RuntimeStatus>) {
        let mut rx = status.subscribe_component_status_updates();
        tokio::spawn(async move {
            while let Some(update) = rx.recv().await {
                let event = map_component_status_update(update);
                self.dispatch_event(event);
            }
        });
    }

    pub fn dispatch_event(&self, event: LifecycleEvent) {
        // Non-blocking fan-out: enqueue sink work onto background tasks.
        let workers = Arc::clone(&self.workers);
        let client = self.client.clone();

        tokio::spawn(async move {
            emit_task_history_event(&event);

            let Some(client) = client else {
                tracing::warn!(
                    "Failed to dispatch lifecycle webhook event: HTTP client unavailable"
                );
                return;
            };

            let workers = workers.read().await;
            let destinations = workers
                .iter()
                .filter_map(|(_name, cfg)| {
                    if cfg.filters.allows(&event) {
                        Some((cfg.name.clone(), cfg.url.clone()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            drop(workers);

            for (worker_name, url) in destinations {
                let client = client.clone();
                let event = event.clone();
                tokio::spawn(async move {
                    let response = client.post(&url).json(&event).send().await;
                    match response {
                        Ok(resp) if resp.status().is_success() => {}
                        Ok(resp) => {
                            tracing::warn!(
                                "Lifecycle webhook worker '{}' failed to deliver event to '{}': HTTP {}",
                                worker_name,
                                url,
                                resp.status()
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Lifecycle webhook worker '{}' failed to deliver event to '{}': {e}",
                                worker_name,
                                url,
                            );
                        }
                    }
                });
            }
        });
    }
}

fn emit_task_history_event(event: &LifecycleEvent) {
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "lifecycle_event",
        input = %event.component,
        source = %event.source,
    );
    let _entered = span.enter();

    tracing::info!(
        target: "task_history",
        lifecycle_level = event.level.as_str(),
        lifecycle_topic = event.topic.as_str(),
        "labels"
    );

    if let Some(status) = &event.status {
        tracing::info!(target: "task_history", lifecycle_status = %status, "labels");
    }

    if let Some(message) = &event.message {
        tracing::info!(target: "task_history", captured_output = %message);
    }
}

fn map_component_status_update(update: ComponentStatusUpdate) -> LifecycleEvent {
    let topic = topic_from_component_name(&update.component_name);

    match update.status {
        ComponentStatus::Ready => LifecycleEvent::new(
            LifecycleEventLevel::Success,
            topic,
            update.component_name,
            "status",
            Some("ready".to_string()),
            None,
        ),
        ComponentStatus::Error(message) => LifecycleEvent::new(
            LifecycleEventLevel::Error,
            topic,
            update.component_name,
            "status",
            Some("error".to_string()),
            message,
        ),
        ComponentStatus::Refreshing => LifecycleEvent::new(
            LifecycleEventLevel::Info,
            LifecycleEventTopic::Refreshes,
            update.component_name,
            "status",
            Some("refreshing".to_string()),
            None,
        ),
        ComponentStatus::Initializing => LifecycleEvent::new(
            LifecycleEventLevel::Info,
            LifecycleEventTopic::Registration,
            update.component_name,
            "status",
            Some("initializing".to_string()),
            None,
        ),
        ComponentStatus::Disabled => LifecycleEvent::new(
            LifecycleEventLevel::Info,
            topic,
            update.component_name,
            "status",
            Some("disabled".to_string()),
            None,
        ),
        ComponentStatus::ShuttingDown => LifecycleEvent::new(
            LifecycleEventLevel::Info,
            topic,
            update.component_name,
            "status",
            Some("shutting_down".to_string()),
            None,
        ),
    }
}

#[must_use]
pub fn topic_from_component_name(component_name: &str) -> LifecycleEventTopic {
    let Some((prefix, _name)) = component_name.split_once(':') else {
        return LifecycleEventTopic::Other;
    };

    match prefix {
        "dataset" => LifecycleEventTopic::Datasets,
        "view" => LifecycleEventTopic::Views,
        "model" | "llm" => LifecycleEventTopic::Models,
        "embedding" => LifecycleEventTopic::EmbeddingVectorization,
        "worker" => LifecycleEventTopic::Workers,
        _ => LifecycleEventTopic::Other,
    }
}
