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

use snafu::{OptionExt, ResultExt};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::task::TaskRequest;

use super::TaskRequestChannel;

pub struct ManualRequestChannel {
    cancellation: Option<Arc<CancellationToken>>,
    reset: Option<Arc<tokio::sync::Notify>>,
    tx: Option<Arc<tokio::sync::mpsc::Sender<Arc<TaskRequest>>>>,
    rx: Arc<RwLock<tokio::sync::mpsc::Receiver<Option<Arc<TaskRequest>>>>>,
}

impl ManualRequestChannel {
    #[must_use]
    pub fn new(rx: tokio::sync::mpsc::Receiver<Option<Arc<TaskRequest>>>) -> Self {
        Self {
            cancellation: None,
            reset: None,
            tx: None,
            rx: Arc::new(RwLock::new(rx)),
        }
    }
}

impl TaskRequestChannel for ManualRequestChannel {
    fn set_cancellation_token(&mut self, cancellation: Arc<CancellationToken>) {
        self.cancellation = Some(cancellation);
    }

    // manual request channels do not require task completion notifications
    fn set_task_completion_notification(&mut self, _notify: Arc<tokio::sync::Notify>) {}

    fn set_reset_notification(&mut self, notify: Arc<tokio::sync::Notify>) {
        self.reset = Some(notify);
    }

    fn set_submission_channel(&mut self, tx: Arc<tokio::sync::mpsc::Sender<Arc<TaskRequest>>>) {
        self.tx = Some(tx);
    }

    fn start(&self) -> Result<JoinHandle<Result<()>>> {
        let tx = self
            .tx
            .clone()
            .context(crate::SubmissionChannelRequiredSnafu)?;
        let cancellation = self
            .cancellation
            .clone()
            .context(crate::CancellationTokenRequiredSnafu)?;
        let rx_lock = Arc::clone(&self.rx);

        Ok(tokio::spawn(async move {
            loop {
                let mut rx = rx_lock.write().await;

                tokio::select! {
                    () = cancellation.cancelled() => {
                        tracing::info!("Cancellation token triggered, stopping manual request channel.");
                        break;
                    }
                    Some(request) = rx.recv() => {
                        if let Some(req) = request {
                            tx.send(req).await.context(crate::ChannelSendSnafu)?;
                        } else {
                            // a None sends a default immediate request
                            tracing::debug!("ManualRequestChannel received None request, sending request which cancels currently running tasks.");
                            let req = Arc::new(TaskRequest::default().cancels_running());
                            tx.send(req).await.context(crate::ChannelSendSnafu)?;
                        }
                    }
                }
            }
            Ok(())
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_manual_request_channel_send_and_cancel() {
        let (tx, rx) = mpsc::channel::<Option<Arc<TaskRequest>>>(2);
        let (submit_tx, mut submit_rx) = mpsc::channel::<Arc<TaskRequest>>(2);

        let cancellation = Arc::new(CancellationToken::new());

        let mut channel = ManualRequestChannel::new(rx);
        channel.set_cancellation_token(Arc::clone(&cancellation));
        channel.set_submission_channel(Arc::new(submit_tx));

        let handle = channel.start().expect("To start manual channel");

        // Send a normal request
        let req = Arc::new(TaskRequest::default());
        tx.send(Some(Arc::clone(&req)))
            .await
            .expect("To send request");
        let received = submit_rx.recv().await.expect("To receive request");
        let now = Instant::now();
        assert!(received.created_at < now);
        assert!(!received.cancel_running);

        // Send None to trigger cancel_running
        tx.send(None).await.expect("To send None request");
        let received = submit_rx.recv().await.expect("To receive cancel request");
        let now = Instant::now();
        assert!(received.created_at < now);
        assert!(received.cancel_running);

        cancellation.cancel();
        handle
            .await
            .expect("To await channel handle")
            .expect("To end channel");
    }

    #[tokio::test]
    async fn test_manual_request_channel_cancellation() {
        let (_tx, rx) = mpsc::channel::<Option<Arc<TaskRequest>>>(1);
        let (submit_tx, _submit_rx) = mpsc::channel::<Arc<TaskRequest>>(1);

        let cancellation = Arc::new(CancellationToken::new());

        let mut channel = ManualRequestChannel::new(rx);
        channel.set_cancellation_token(Arc::clone(&cancellation));
        channel.set_submission_channel(Arc::new(submit_tx));

        let handle = channel.start().expect("To start manual channel");

        cancellation.cancel();

        handle
            .await
            .expect("To await channel handle")
            .expect("To end channel");
    }
}
