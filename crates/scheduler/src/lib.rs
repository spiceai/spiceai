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

use snafu::prelude::*;
use task::TaskRequest;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("A cancellation token is required"))]
    CancellationTokenRequired,
    #[snafu(display("A notification channel is required"))]
    NotificationChannelRequired,
    #[snafu(display("A submission channel is required"))]
    SubmissionChannelRequired,
    #[snafu(display("A channel send error occurred: {source}"))]
    ChannelSendError {
        source: tokio::sync::mpsc::error::SendError<Arc<TaskRequest>>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod channel;
pub mod schedule;
pub mod scheduler;
pub mod task;
