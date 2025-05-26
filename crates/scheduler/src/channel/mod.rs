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

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::task::TaskRequest;

pub mod interval;
pub mod manual;

pub trait TaskRequestChannel: Send + Sync {
    /// Sets the cancellation token for this task request channel.
    fn set_cancellation_token(&mut self, cancellation: Arc<CancellationToken>);
    /// Sets the notification channel to notify when a task is completed.
    fn set_task_completion_notification(&mut self, _notify: Arc<tokio::sync::Notify>) {}
    /// Sets the reset notification channel to notify when the requestor should reset and wait for the next notification.
    fn set_reset_notification(&mut self, notify: Arc<tokio::sync::Notify>);
    /// Sets the submission channel to send the task request.
    fn set_submission_channel(&mut self, tx: Arc<tokio::sync::mpsc::Sender<Arc<TaskRequest>>>);
    /// Starts the task request channel and returns a handle to the background task.
    #[allow(clippy::missing_errors_doc)]
    fn start(&self) -> Result<JoinHandle<Result<()>>>;
}
