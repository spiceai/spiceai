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

use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::{Notify, RwLock};
use uuid::Uuid;

use crate::{
    channel::TaskRequestChannel,
    task::{RunningTask, ScheduledTask},
};

pub struct Schedule {
    id: Arc<str>,
    channels: Vec<Arc<RwLock<dyn TaskRequestChannel>>>,
    tasks: Vec<Arc<dyn ScheduledTask>>,
}

impl Hash for Schedule {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Schedule {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Schedule {}

impl Default for Schedule {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4().to_string().into(),
            channels: Vec::new(),
            tasks: Vec::new(),
        }
    }
}

impl Schedule {
    #[must_use]
    pub fn id(&self) -> Arc<str> {
        Arc::clone(&self.id)
    }

    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn add_channel(mut self, channel: Arc<RwLock<dyn TaskRequestChannel>>) -> Self {
        self.channels.push(channel);
        self
    }

    #[must_use]
    pub fn add_component(mut self, component: Arc<dyn ScheduledTask>) -> Self {
        self.tasks.push(component);
        self
    }

    /// Executes the components defined by this schedule.
    pub(crate) fn execute(self: &Arc<Self>, notifier: Arc<Notify>) -> RunningTask {
        let components = self.tasks.clone();
        let handle = tokio::spawn(async move {
            let mut failed_components = Vec::new();
            for component in components {
                if let Err(e) = component.execute().await {
                    failed_components.push(e);
                }
            }

            if !failed_components.is_empty() {
                // Log or handle the errors
            }

            notifier.notify_waiters();

            Ok(())
        });

        RunningTask::new(handle)
    }

    #[must_use]
    pub(crate) fn channels(&self) -> &Vec<Arc<RwLock<dyn TaskRequestChannel>>> {
        &self.channels
    }
}
