use crate::{FailedToInitializeCheckpointSnafu, Result};
use crate::client_sdk::SDKClient;
use crate::stream::{DynamodbStream, DynamodbStreamProducer};
use crate::types::checkpoint::{CheckpointPosition, GlobalCheckpoint, ShardCheckpoint};
use crate::stream_state::initialize_state_from_checkpoint;
use aws_config::SdkConfig;
use snafu::OptionExt;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio::time::Duration;

#[derive(Debug, Clone)]
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
    pub fn new(sdk_config: &SdkConfig, table_name: String) -> Self {
        Self {
            sdk_client: Arc::new(SDKClient::new(sdk_config, None)),
            table_name,
            interval: Some(DEFAULT_INTERVAL),
            buffer: DEFAULT_BUFFER_SIZE,
        }
    }

    #[must_use]
    pub fn interval(mut self, interval: Option<Duration>) -> Self {
        self.interval = interval;
        self
    }

    #[must_use]
    pub fn buffer(mut self, buffer: usize) -> Self {
        assert!(buffer > 0, "buffer must be positive");
        self.buffer = buffer;
        self
    }

    pub async fn latest_global_checkpoint(&self) -> Result<GlobalCheckpoint> {
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
                let sequence_number = s.starting_sequence_number
                    .context(FailedToInitializeCheckpointSnafu)?;

                Ok((
                    s.shard_id.clone(),
                    ShardCheckpoint {
                        sequence_number,
                        parent_id: s.parent_shard_id.clone(),
                        updated_at: SystemTime::now(),
                        position: CheckpointPosition::At,
                    },
                ))
            })
            .collect::<Result<_>>()?;

        Ok(GlobalCheckpoint {
            stream_arn,
            shards: checkpoint_shards,
        })
    }

    pub async fn stream_from_checkpoint(
        &self,
        checkpoint: GlobalCheckpoint,
    ) -> Result<DynamodbStream> {
        let state =
            initialize_state_from_checkpoint(&checkpoint, Arc::clone(&self.sdk_client)).await?;

        let (tx, rx) = mpsc::channel(self.buffer);

        let producer = DynamodbStreamProducer {
            stream_arn: checkpoint.stream_arn.clone(),
            state,
            interval: self.interval,
            sender: tx,
            client: Arc::clone(&self.sdk_client),
        };

        tokio::spawn(async move {
            producer.streaming().await;
        });

        Ok(DynamodbStream { receiver: rx })
    }
}
