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

use scheduler::Result;
use scheduler::task::ScheduledTask;
use tonic::async_trait;
use tracing_futures::Instrument;

use crate::component::view::View;
use crate::lifecycle_events::{LifecycleEvent, LifecycleEventLevel, LifecycleEventTopic};

pub struct ViewRefreshTask(Arc<View>);

impl From<Arc<View>> for ViewRefreshTask {
    fn from(view: Arc<View>) -> Self {
        Self(view)
    }
}

#[async_trait]
impl ScheduledTask for ViewRefreshTask {
    async fn execute(&self) -> Result<()> {
        let view = Arc::clone(&self.0);
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "acceleration_refresh", input = %view.name.to_string());
        async {
            let runtime = Arc::clone(&view.runtime);
            let component = format!("view:{}", view.name);

            runtime.emit_lifecycle_event(LifecycleEvent::new(
                LifecycleEventLevel::Info,
                LifecycleEventTopic::Crons,
                component.clone(),
                "view_refresh_cron",
                Some("started".to_string()),
                Some("Started scheduled view refresh".to_string()),
            ));

            match runtime.datafusion().refresh_table(&view.name, None).await {
                Ok(notifier) => {
                    if let Some(notifier) = notifier {
                        notifier.notified().await;
                    }
                    runtime.emit_lifecycle_event(LifecycleEvent::new(
                        LifecycleEventLevel::Success,
                        LifecycleEventTopic::Refreshes,
                        component,
                        "view_refresh_cron",
                        Some("completed".to_string()),
                        Some("Completed scheduled view refresh".to_string()),
                    ));
                    runtime.emit_lifecycle_event(LifecycleEvent::new(
                        LifecycleEventLevel::Success,
                        LifecycleEventTopic::Accelerations,
                        format!("view:{}", view.name),
                        "view_refresh_cron",
                        Some("completed".to_string()),
                        Some("Completed accelerated view refresh".to_string()),
                    ));
                    Ok(())
                }
                Err(e) => {
                    runtime.emit_lifecycle_event(LifecycleEvent::new(
                        LifecycleEventLevel::Error,
                        LifecycleEventTopic::Refreshes,
                        component,
                        "view_refresh_cron",
                        Some("error".to_string()),
                        Some(e.to_string()),
                    ));
                    runtime.emit_lifecycle_event(LifecycleEvent::new(
                        LifecycleEventLevel::Error,
                        LifecycleEventTopic::Accelerations,
                        format!("view:{}", view.name),
                        "view_refresh_cron",
                        Some("error".to_string()),
                        Some(e.to_string()),
                    ));
                    Err(scheduler::Error::RefreshTaskFailure { source: e.into() })
                }
            }
        }
        .instrument(span)
        .await
    }
}
