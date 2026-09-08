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
        "A cancellation token is required. This is likely an internal error, if builtin task request triggers are used."
    ))]
    CancellationTokenRequired,
    #[snafu(display(
        "A notification channel is required. This is likely an internal error, if builtin task request triggers are used."
    ))]
    NotificationChannelRequired,
    #[snafu(display(
        "A submission channel is required. This is likely an internal error, if builtin task request triggers are used."
    ))]
    SubmissionChannelRequired,
    #[snafu(display("A channel send error occurred. {source}"))]
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
        "Failed to parse cron expression. {source} Confirm the cron expression is valid, and try again."
    ))]
    FailedToParseCron { source: croner::errors::CronError },
    #[snafu(display(
        "Failed to determine next cron expression run time. {source} Confirm the cron expression is valid, and try again."
    ))]
    FailedToDetermineNextCronRunTime { source: croner::errors::CronError },
    #[snafu(display(
        "The specified schedule '{name}' does not exist in the scheduler. Ensure the schedule is defined, and try again."
    ))]
    ScheduleNotFound { name: String },
    #[snafu(display("An error occurred while executing the scheduled task. {source}"))]
    RefreshTaskFailure {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod channel;
pub mod schedule;
pub mod scheduler;
pub mod task;
