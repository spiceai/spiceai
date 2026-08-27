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

            match runtime.datafusion().refresh_table(&view.name, None).await {
                Ok(completion) => {
                    if let Some(completion) = completion
                        && completion.wait().await.is_abandoned()
                    {
                        // The view was removed while its scheduled refresh was
                        // in flight. As for datasets, the task acts on nothing
                        // after the wait, so this is recorded rather than
                        // raised.
                        let view_name = &view.name;
                        tracing::debug!(
                            "{view_name} was removed before its scheduled refresh completed."
                        );
                    }
                    Ok(())
                }
                Err(e) => Err(scheduler::Error::RefreshTaskFailure { source: e.into() }),
            }
        }
        .instrument(span)
        .await
    }
}
