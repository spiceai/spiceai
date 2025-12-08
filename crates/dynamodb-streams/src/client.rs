/*
Copyright 2025 The Spice.ai OSS Authors

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
use crate::checkpoint::{Checkpoint, CheckpointPosition, ShardCheckpoint};
use crate::client_sdk::SDKClient;
use crate::stream::{DynamodbStream, DynamodbStreamProducer};
use crate::stream_state::initialize_state_from_checkpoint;
use crate::{FailedToInitializeCheckpointSnafu, Result};
use aws_config::SdkConfig;
use snafu::OptionExt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio::time::Duration;
use util::retry_strategy::{BackoffMethod, RetryBackoffBuilder};

#[derive(Debug, Clone)]
#[expect(clippy::struct_field_names)]
pub struct Client {
    sdk_client: Arc<SDKClient>,
    table_name: String,
    interval: Option<Duration>,
    buffer: usize,
}

const DEFAULT_BUFFER_SIZE: usize = 100;
const DEFAULT_INTERVAL: Duration = Duration::from_secs(3);

impl Client {
    #[must_use]
    pub fn builder(sdk_config: SdkConfig, table_name: String) -> ClientBuilder {
        ClientBuilder::new(sdk_config, table_name)
    }

    /// Returns a checkpoint representing the current state of all open shards.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table has no stream enabled
    /// - AWS API calls fail (network, permissions, etc.)
    /// - Any open shard is missing a starting sequence number
    pub async fn latest_global_checkpoint(&self) -> Result<Checkpoint> {
        let stream_arn = self
            .sdk_client
            .get_stream_arn(self.table_name.clone())
            .await?;
        let shards = self.sdk_client.get_all_shards(&stream_arn).await?;

        let checkpoint_shards = shards
            .into_iter()
            // Only open shards
            .filter(|s| s.ending_sequence_number.is_none())
            .map(|s| {
                let sequence_number = s
                    .starting_sequence_number
                    .context(FailedToInitializeCheckpointSnafu)?;

                Ok((
                    s.shard_id.clone(),
                    ShardCheckpoint {
                        sequence_number,
                        parent_id: s.parent_shard_id,
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::At,
                    },
                ))
            })
            .collect::<Result<_>>()?;

        tracing::debug!("Latest checkpoint initialized: {:#?}", checkpoint_shards);

        Ok(Checkpoint {
            shards: checkpoint_shards,
        })
    }

    /// Creates a stream that processes records starting from the given checkpoint.
    ///
    /// The checkpoint must be from the same table and stream. Checkpoints are valid
    /// for 24 hours (`DynamoDB` Streams retention period).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The stream ARN cannot be retrieved
    /// - Checkpoint initialization fails (expired shards, invalid sequence numbers)
    /// - Initial shard iterator requests fail
    pub async fn stream_from_checkpoint(&self, checkpoint: Checkpoint) -> Result<DynamodbStream> {
        let stream_arn = self
            .sdk_client
            .get_stream_arn(self.table_name.clone())
            .await?;

        let state = initialize_state_from_checkpoint(
            stream_arn.clone(),
            &checkpoint,
            Arc::clone(&self.sdk_client),
        )
        .await?;

        tracing::debug!("Stream initialized from checkpoint: {:#?}", state);

        let (tx, rx) = mpsc::channel(self.buffer);

        let retry_strategy = RetryBackoffBuilder::new()
            .method(BackoffMethod::Fibonacci)
            .max_retries(None)
            .build();

        let producer = DynamodbStreamProducer {
            stream_arn,
            state,
            interval: self.interval,
            sender: tx,
            client: Arc::clone(&self.sdk_client),
            retry_strategy,
        };

        tokio::spawn(async move {
            producer.streaming().await;
        });

        Ok(DynamodbStream { receiver: rx })
    }
}

#[derive(Debug)]
pub struct ClientBuilder {
    sdk_config: SdkConfig,
    table_name: String,
    interval: Option<Duration>,
    buffer: usize,
    shard_record_limit: Option<i32>,
}

impl ClientBuilder {
    #[must_use]
    pub fn new(sdk_config: SdkConfig, table_name: String) -> Self {
        Self {
            sdk_config,
            table_name,
            interval: Some(DEFAULT_INTERVAL),
            buffer: DEFAULT_BUFFER_SIZE,
            shard_record_limit: None,
        }
    }

    #[must_use]
    pub fn interval(mut self, interval: Option<Duration>) -> Self {
        self.interval = interval;
        self
    }

    #[must_use]
    pub fn buffer(mut self, buffer: NonZeroUsize) -> Self {
        self.buffer = buffer.get();
        self
    }

    #[must_use]
    pub fn shard_record_limit(mut self, shard_record_limit: Option<i32>) -> Self {
        self.shard_record_limit = shard_record_limit;
        self
    }

    #[must_use]
    pub fn build(self) -> Client {
        Client {
            sdk_client: Arc::new(SDKClient::new(&self.sdk_config, self.shard_record_limit)),
            table_name: self.table_name,
            interval: self.interval,
            buffer: self.buffer,
        }
    }
}
