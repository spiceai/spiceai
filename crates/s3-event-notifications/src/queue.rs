/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Receive and acknowledge SQS messages.
//!
//! Consumers go through [`MessageQueue`] rather than the SQS client so a test
//! can drive them with an in-memory queue.

use async_trait::async_trait;
use snafu::Snafu;

/// How long one receive waits for a message before returning empty. 20
/// seconds is the SQS long-polling maximum.
const SQS_LONG_POLL_SECONDS: i32 = 20;
/// The most messages one receive returns. 10 is the SQS maximum.
const SQS_MAX_MESSAGES: i32 = 10;
/// How long a received message stays hidden from other receives. A message
/// that is not deleted within this time is delivered again.
const SQS_VISIBILITY_TIMEOUT_SECONDS: i32 = 300;

#[async_trait]
pub trait MessageQueue: Send + Sync {
    async fn receive(&self) -> Result<Vec<QueueMessage>, QueueError>;
    async fn delete(&self, receipt_handle: &str) -> Result<(), QueueError>;
}

#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub body: String,
    pub receipt_handle: String,
}

/// An SQS request that failed. The queue URL is a secret parameter, so it is
/// never part of the message.
#[derive(Debug, Snafu)]
pub enum QueueError {
    #[snafu(display("Failed to receive messages from the configured SQS queue: {source}"))]
    Receive {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Failed to delete an SQS message from the configured SQS queue: {source}"))]
    Delete {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// A long-polling [`MessageQueue`] over one SQS queue.
pub struct SqsQueue {
    client: aws_sdk_sqs::Client,
    queue_url: String,
}

impl SqsQueue {
    #[must_use]
    pub fn new(client: aws_sdk_sqs::Client, queue_url: String) -> Self {
        Self { client, queue_url }
    }
}

#[async_trait]
impl MessageQueue for SqsQueue {
    async fn receive(&self) -> Result<Vec<QueueMessage>, QueueError> {
        let output = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(SQS_MAX_MESSAGES)
            .wait_time_seconds(SQS_LONG_POLL_SECONDS)
            .visibility_timeout(SQS_VISIBILITY_TIMEOUT_SECONDS)
            .send()
            .await
            .map_err(|source| QueueError::Receive {
                source: Box::new(source),
            })?;

        Ok(output
            .messages
            .unwrap_or_default()
            .into_iter()
            .filter_map(|message| {
                Some(QueueMessage {
                    body: message.body?,
                    receipt_handle: message.receipt_handle?,
                })
            })
            .collect())
    }

    async fn delete(&self, receipt_handle: &str) -> Result<(), QueueError> {
        self.client
            .delete_message()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt_handle)
            .send()
            .await
            .map_err(|source| QueueError::Delete {
                source: Box::new(source),
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUEUE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/s3-events";

    #[test]
    fn queue_errors_do_not_include_the_secret_queue_url() {
        let receive = QueueError::Receive {
            source: "access denied".into(),
        };
        let delete = QueueError::Delete {
            source: "access denied".into(),
        };
        for message in [receive.to_string(), delete.to_string()] {
            assert!(
                !message.contains("amazonaws")
                    && !message.contains("123456789012")
                    && !message.contains(QUEUE_URL),
                "SQS errors must not interpolate the queue URL, got: {message}"
            );
        }
    }
}
