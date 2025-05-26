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

use std::time::Instant;

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::Result;

#[async_trait]
pub trait ScheduledTask: Send + Sync {
    /// Executes the defined component.
    async fn execute(&self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRequest {
    pub cancel_running: bool,
    pub clear_queue: bool,
    pub created_at: Instant,
}

impl Default for TaskRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskRequest {
    #[must_use]
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            cancel_running: false,
            clear_queue: false,
            created_at: now,
        }
    }

    #[must_use]
    pub fn cancels_running(mut self) -> Self {
        self.cancel_running = true;
        self
    }

    #[must_use]
    pub fn clears_queue(mut self) -> Self {
        self.clear_queue = true;
        self
    }
}

#[derive(Debug)]
pub(crate) struct RunningTask {
    pub(crate) handle: JoinHandle<Result<()>>,
}

impl RunningTask {
    #[must_use]
    pub(crate) fn new(handle: JoinHandle<Result<()>>) -> Self {
        Self { handle }
    }

    #[must_use]
    pub(crate) fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

impl RunningTask {
    #[must_use]
    pub fn consume_for_handle(self) -> JoinHandle<Result<()>> {
        self.handle
    }
}
