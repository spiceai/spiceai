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

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use chrono::Local;
use croner::Cron;
use croner::parser::{CronParser, Seconds, Year};
use snafu::{OptionExt, ResultExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::task::TaskRequest;

use super::TaskRequestChannel;
pub struct CronRequestChannel {
    cancellation: Option<Arc<CancellationToken>>,
    task_completion: Option<Arc<tokio::sync::Notify>>,
    reset: Option<Arc<tokio::sync::Notify>>,
    tx: Option<Arc<tokio::sync::mpsc::Sender<Arc<TaskRequest>>>>,
    cron: Arc<Cron>,
}

static CRON_PARSER: LazyLock<CronParser> = LazyLock::new(|| {
    CronParser::builder()
        .seconds(Seconds::Optional)
        .year(Year::Disallowed) // TODO: allow optional years in 2.0.0 - https://github.com/spiceai/spiceai/issues/6548
        .build()
});

impl CronRequestChannel {
    /// Creates a new `CronRequestChannel` with the given cron expression.
    ///
    /// # Errors
    ///
    /// Returns an error if the cron expression is invalid or cannot be parsed.
    pub fn new(cron: &Arc<str>) -> Result<Self> {
        Ok(Self {
            cancellation: None,
            task_completion: None,
            reset: None,
            tx: None,
            cron: Arc::new(
                CRON_PARSER
                    .parse(cron)
                    .context(crate::FailedToParseCronSnafu)?,
            ),
        })
    }
}

impl TaskRequestChannel for CronRequestChannel {
    fn set_cancellation_token(&mut self, cancellation: Arc<CancellationToken>) {
        self.cancellation = Some(cancellation);
    }

    fn set_task_completion_notification(&mut self, notify: Arc<tokio::sync::Notify>) {
        self.task_completion = Some(notify);
    }

    fn set_reset_notification(&mut self, notify: Arc<tokio::sync::Notify>) {
        self.reset = Some(notify);
    }

    fn set_submission_channel(&mut self, tx: Arc<tokio::sync::mpsc::Sender<Arc<TaskRequest>>>) {
        self.tx = Some(tx);
    }

    fn start(&self) -> Result<JoinHandle<Result<()>>> {
        // cancellation token to cancel the background task
        let cancellation = self
            .cancellation
            .clone()
            .context(crate::CancellationTokenRequiredSnafu)?;
        // reset channel to advise the requestor to reset and wait for the next notification
        // e.g. another requestor has started a task, and the task is currently running
        let reset = self
            .reset
            .clone()
            .context(crate::NotificationChannelRequiredSnafu)?;
        // notification channel to notify the requestor that a task has been completed
        let task_completion = self
            .task_completion
            .clone()
            .context(crate::NotificationChannelRequiredSnafu)?;
        // request submission channel to send the request
        let tx = self
            .tx
            .clone()
            .context(crate::SubmissionChannelRequiredSnafu)?;
        let cron = Arc::clone(&self.cron);

        Ok(tokio::spawn(async move {
            let mut first_run = true;
            loop {
                if first_run {
                    first_run = false;
                } else {
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            tracing::debug!("Cron evaluator cancelled");
                            return Ok(());
                        }
                        () = reset.notified() => {
                            tracing::debug!("Cron evaluator reset");
                            continue;
                        }
                        () = task_completion.notified() => {
                            tracing::debug!("Cron evaluator notified");
                        }
                    }
                }

                let time = Local::now();
                let next = cron
                    .find_next_occurrence(&time, false)
                    .context(crate::FailedToDetermineNextCronRunTimeSnafu)?;

                tracing::debug!("Next cron run time: {next}");

                let duration_till = next.signed_duration_since(time);
                // .to_std() errors when the duration is less than zero - the next expression time is in the past
                let interval = duration_till.to_std().unwrap_or(Duration::from_secs(1));

                tokio::select! {
                    () = cancellation.cancelled() => {
                        tracing::debug!("Cron evaluator cancelled");
                        return Ok(());
                    }
                    () = reset.notified() => {
                        tracing::debug!("Cron evaluator reset");
                        continue;
                    }
                    () = tokio::time::sleep(interval) => {
                        tracing::debug!("Cron evaluator time elapsed");
                    }
                }

                tx.send(Arc::new(TaskRequest::default()))
                    .await
                    .context(crate::ChannelSendSnafu)?;
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use chrono::Timelike;

    use super::*;

    #[tokio::test]
    async fn test_cron_request_channel() {
        let cron_expression = "*/5 * * * * *".into();
        let mut channel =
            CronRequestChannel::new(&cron_expression).expect("Cron expression should be valid");

        let cancellation_token = Arc::new(CancellationToken::new());
        channel.set_cancellation_token(Arc::clone(&cancellation_token));

        let task_completion = Arc::new(tokio::sync::Notify::new());
        channel.set_task_completion_notification(Arc::clone(&task_completion));

        let reset_notify = Arc::new(tokio::sync::Notify::new());
        channel.set_reset_notification(Arc::clone(&reset_notify));

        let (tx, mut rx) = tokio::sync::mpsc::channel(5);
        channel.set_submission_channel(Arc::new(tx));

        let handle = channel.start().expect("Cron channel should start");

        let request = rx.recv().await.expect("Should receive a task request");
        let now = Local::now();
        assert!(
            now.second().is_multiple_of(5),
            "The request should be sent at a 5-second interval"
        );
        assert!(!request.cancel_running);
        assert!(!request.clear_queue);

        task_completion.notify_waiters();

        let request = rx
            .recv()
            .await
            .expect("Should receive another task request");
        let next_now = Local::now();
        let elapsed = next_now.signed_duration_since(now).num_milliseconds();
        assert!(
            (4950..=5050).contains(&elapsed),
            "The next request should be sent after 5 seconds"
        );
        assert!(
            next_now.second().is_multiple_of(5),
            "The request should be sent at a 5-second interval"
        );
        assert!(!request.cancel_running);
        assert!(!request.clear_queue);

        cancellation_token.cancel();
        handle
            .await
            .expect("Should join handle")
            .expect("Handle should end successfully");
    }

    #[tokio::test]
    async fn test_cron_cannot_go_faster_than_second() {
        let cron_expression = "* * * * * * *".into(); // expression attempting to run every millisecond
        let channel = CronRequestChannel::new(&cron_expression);
        assert!(channel.is_err(), "Cron expression should be invalid");
    }

    #[tokio::test]
    async fn test_cron_resets_to_next() {
        // resetting while in-between a schedule should evaluate to the same next instance again
        let cron_expression = "*/5 * * * * *".into();
        let mut channel =
            CronRequestChannel::new(&cron_expression).expect("Cron expression should be valid");

        let cancellation_token = Arc::new(CancellationToken::new());
        channel.set_cancellation_token(Arc::clone(&cancellation_token));

        let task_completion = Arc::new(tokio::sync::Notify::new());
        channel.set_task_completion_notification(Arc::clone(&task_completion));

        let reset_notify = Arc::new(tokio::sync::Notify::new());
        channel.set_reset_notification(Arc::clone(&reset_notify));

        let (tx, mut rx) = tokio::sync::mpsc::channel(5);
        channel.set_submission_channel(Arc::new(tx));

        let handle = channel.start().expect("Cron channel should start");

        // Wait for the first request to peg to the interval
        let request = rx.recv().await.expect("Should receive a task request");
        let last_now = Local::now();
        assert!(
            last_now.second().is_multiple_of(5),
            "The request should be sent at a 5-second interval"
        );
        assert!(!request.cancel_running);
        assert!(!request.clear_queue);

        task_completion.notify_waiters();

        tokio::select! {
            request = rx.recv() => {
                panic!("Should not receive a task request yet, got: {request:?}");
            }
            () = tokio::time::sleep(Duration::from_secs(2)) => {
                reset_notify.notify_waiters();
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
        task_completion.notify_waiters();
        let now = Local::now();

        let request = rx
            .recv()
            .await
            .expect("Should receive another task request");
        let next_now = Local::now();
        let elapsed = next_now.signed_duration_since(now).num_milliseconds();
        let original_elapsed = next_now.signed_duration_since(last_now).num_milliseconds();
        assert!(
            (2950..=3050).contains(&elapsed),
            "The next request should be sent after 2  or 3 seconds"
        );
        assert!(
            (4950..=5050).contains(&original_elapsed),
            "The next request should be sent after 5 seconds"
        );
        assert!(
            next_now.second().is_multiple_of(5),
            "The request should be sent at a 5-second interval"
        );
        assert!(!request.cancel_running);
        assert!(!request.clear_queue);

        cancellation_token.cancel();
        handle
            .await
            .expect("Should join handle")
            .expect("Handle should end successfully");
    }

    #[tokio::test]
    async fn test_cron_seconds_optional() {
        // Test that 5-field cron expressions (without seconds) are valid and work correctly.
        // Using "* * * * *" but with a much shorter wait by injecting the schedule
        // and verifying parsing and next-instance calculation.
        let cron_expression = "* * * * *".into(); // every minute (standard 5-field format)
        let channel =
            CronRequestChannel::new(&cron_expression).expect("Cron expression should be valid");

        // Verify that the 5-field cron expression was parsed correctly
        // by checking that the next scheduled time is at second = 0
        let now = Local::now();
        let next = channel
            .cron
            .find_next_occurrence(&now, false)
            .expect("Should find next occurrence");

        // 5-field cron should schedule at second = 0
        assert_eq!(
            next.second(),
            0,
            "5-field cron expression should trigger at second 0"
        );

        // Verify the interval is 60 seconds (1 minute)
        let next_after = channel
            .cron
            .find_next_occurrence(&next, false)
            .expect("Should find occurrence after next");
        let interval = next_after.signed_duration_since(next).num_seconds();
        assert_eq!(interval, 60, "5-field cron should have 60-second intervals");
    }
}
