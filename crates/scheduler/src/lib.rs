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
    #[snafu(display(
        "A cancellation token is required.\nThis is likely an internal error, if builtin task request triggers are used.\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    CancellationTokenRequired,
    #[snafu(display(
        "A notification channel is required.\nThis is likely an internal error, if builtin task request triggers are used.\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    NotificationChannelRequired,
    #[snafu(display(
        "A submission channel is required.\nThis is likely an internal error, if builtin task request triggers are used.\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    SubmissionChannelRequired,
    #[snafu(display(
        "A channel send error occurred.\n{source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    ChannelSendError {
        source: tokio::sync::mpsc::error::SendError<Arc<TaskRequest>>,
    },
    #[snafu(display(
        "The names of the schedules must be unique, but the name '{name}' is already in use"
    ))]
    DuplicateScheduleName { name: String },
    #[snafu(display("No schedules were specified for the scheduler '{name}'"))]
    NoSchedulesSpecified { name: String },
    #[snafu(display(
        "Failed to parse cron expression.\n{source}\nConfirm the cron expression is valid, and try again."
    ))]
    FailedToParseCron { source: croner::errors::CronError },
    #[snafu(display(
        "Failed to determine next cron expression run time.\n{source}\nConfirm the cron expression is valid, and try again."
    ))]
    FailedToDetermineNextCronRunTime { source: croner::errors::CronError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod channel;
pub mod schedule;
pub mod scheduler;
pub mod task;
