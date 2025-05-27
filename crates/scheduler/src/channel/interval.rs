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
use std::time::Duration;

use snafu::{OptionExt, ResultExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::task::TaskRequest;

use super::TaskRequestChannel;

pub struct IntervalRequestChannel {
    cancellation: Option<Arc<CancellationToken>>,
    task_completion: Option<Arc<tokio::sync::Notify>>,
    reset: Option<Arc<tokio::sync::Notify>>,
    tx: Option<Arc<tokio::sync::mpsc::Sender<Arc<TaskRequest>>>>,
    interval: Duration,
}

impl IntervalRequestChannel {
    #[must_use]
    pub fn new(interval: u64) -> Self {
        Self {
            cancellation: None,
            task_completion: None,
            reset: None,
            tx: None,
            interval: Duration::from_secs(interval),
        }
    }
}

impl TaskRequestChannel for IntervalRequestChannel {
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
        let interval = self.interval;

        Ok(tokio::spawn(async move {
            let mut first_run = true;
            loop {
                if first_run {
                    first_run = false;
                } else {
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            tracing::debug!("Interval evaluator cancelled");
                            return Ok(());
                        }
                        () = reset.notified() => {
                            tracing::debug!("Interval evaluator reset");
                            continue;
                        }
                        () = task_completion.notified() => {
                            tracing::debug!("Interval evaluator notified");
                        }
                    }
                }

                tokio::select! {
                    () = cancellation.cancelled() => {
                        tracing::debug!("Interval evaluator cancelled");
                        return Ok(());
                    }
                    () = reset.notified() => {
                        tracing::debug!("Interval evaluator reset");
                        continue;
                    }
                    () = tokio::time::sleep(interval) => {
                        tracing::debug!("Interval evaluator interval elapsed");
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
    use std::time::Instant;

    use super::*;

    #[tokio::test]
    async fn test_interval_request_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Arc<TaskRequest>>(1);

        let cancellation = Arc::new(CancellationToken::new());
        let task_completion = Arc::new(tokio::sync::Notify::new());
        let reset = Arc::new(tokio::sync::Notify::new());

        let mut channel = IntervalRequestChannel::new(1);
        channel.set_cancellation_token(Arc::clone(&cancellation));
        channel.set_task_completion_notification(Arc::clone(&task_completion));
        channel.set_reset_notification(Arc::clone(&reset));
        channel.set_submission_channel(Arc::new(tx));

        let channel_handle = channel.start().expect("To start request channel");

        let now = Instant::now();
        let request = rx.recv().await.expect("To receive request");
        let elapsed = now.elapsed();
        let now = Instant::now();
        assert!(request.created_at <= now);
        assert!(elapsed.as_millis() >= 990 && elapsed.as_millis() <= 1010);
        assert!(!request.cancel_running);

        // next request should wait for task notification
        tokio::select! {
            Some(_) = rx.recv() => {
                panic!("Should not receive next request");
            }
            () = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                // do nothing
            }
        }

        let now = Instant::now();
        task_completion.notify_one();
        let request = rx.recv().await.expect("To receive request");
        let elapsed = now.elapsed();
        let now = Instant::now();
        assert!(request.created_at < now);
        assert!(elapsed.as_millis() >= 990 && elapsed.as_millis() <= 1010);
        assert!(!request.cancel_running);

        cancellation.cancel();
        channel_handle
            .await
            .expect("To await channel handle")
            .expect("To end channel");
    }

    #[tokio::test]
    async fn test_multi_channel_requestors() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Arc<TaskRequest>>(1);

        let cancellation = Arc::new(CancellationToken::new());
        let task_completion = Arc::new(tokio::sync::Notify::new());
        let reset = Arc::new(tokio::sync::Notify::new());

        let tx = Arc::new(tx);
        let mut channel_one = IntervalRequestChannel::new(1);
        channel_one.set_cancellation_token(Arc::clone(&cancellation));
        channel_one.set_task_completion_notification(Arc::clone(&task_completion));
        channel_one.set_reset_notification(Arc::clone(&reset));
        channel_one.set_submission_channel(Arc::clone(&tx));
        let mut channel_two = IntervalRequestChannel::new(1);
        channel_two.set_cancellation_token(Arc::clone(&cancellation));
        channel_two.set_task_completion_notification(Arc::clone(&task_completion));
        channel_two.set_reset_notification(Arc::clone(&reset));
        channel_two.set_submission_channel(Arc::clone(&tx));

        let handle_one = channel_one.start().expect("To start request channel");
        let handle_two = channel_two.start().expect("To start request channel");

        // each channel will send a first request, resulting in two requests at the 1st second mark
        let now = Instant::now();
        let request_one = rx.recv().await.expect("To receive request");
        let request_two = rx.recv().await.expect("To receive request");
        let elapsed = now.elapsed();
        let now = Instant::now();
        assert!(request_one.created_at < now);
        assert!(request_two.created_at < now);
        assert!(elapsed.as_millis() >= 990 && elapsed.as_millis() <= 1010);
        assert!(!request_one.cancel_running);
        assert!(!request_two.cancel_running);

        cancellation.cancel();
        handle_one
            .await
            .expect("To await channel handle")
            .expect("To end channel");

        handle_two
            .await
            .expect("To await channel handle")
            .expect("To end channel");
    }
}
