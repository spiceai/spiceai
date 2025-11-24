use crate::client_sdk::SDKClient;
use crate::types::stream_state::{RecordBatch, StreamState};
use crate::{Result, StreamResult};
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use futures::{Stream, future::join_all};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::{
    sync::mpsc,
    time::{Duration, sleep},
};

#[derive(Debug)]
pub struct DynamodbStreamProducer {
    pub stream_arn: String,
    pub state: StreamState,
    pub interval: Option<Duration>,
    pub sender: mpsc::Sender<StreamResult>,
    pub client: Arc<SDKClient>,
}

impl DynamodbStreamProducer {
    async fn iterate(&mut self) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();

        // 1. Poll active shards
        let futures = self.state.active_shards().map(|shard| {
            let client = Arc::clone(&self.client);
            tracing::debug!(
                "Polling shard with iterator: shard_id={}, iterator={}",
                shard.shard_id,
                shard.iterator
            );
            async move {
                (
                    shard.shard_id.clone(),
                    client.get_iterator_records(&shard.iterator).await,
                )
            }
        });

        let results = join_all(futures).await;

        // 2. Process poll results
        for (shard_id, result) in results {
            match result {
                Ok((next_iter, records)) => {
                    if let Some(batch) =
                        self.state.handle_poll_result(&shard_id, next_iter, records)
                    {
                        batches.push(batch);
                    }
                }
                Err(e) => {
                    tracing::error!("Shard {} poll failed: {}", shard_id, e);
                }
            }
        }

        // 3. Discover new shards
        if let Ok(shards) = self.client.get_all_shards(&self.stream_arn).await {
            self.state.add_discovered(shards);
        }

        // 4. Initialize shards that require iterators
        self.initialize_shards_iterators().await;

        Ok(batches)
    }

    async fn initialize_shards_iterators(&mut self) {
        let shard_ids: Vec<String> = self.state.initializing.keys().cloned().collect();

        for shard_id in shard_ids {
            match self
                .client
                .get_shard_iterator(
                    &self.stream_arn,
                    &shard_id,
                    &ShardIteratorType::TrimHorizon,
                    None,
                )
                .await
            {
                Ok(iterator) => {
                    if let Some(iterator) = iterator {
                        self.state.mark_active(shard_id, iterator);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to initialize shard {}: {}", shard_id, e);
                }
            }
        }
    }

    pub async fn streaming(mut self) {
        loop {
            let batches = match self.iterate().await {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!("Iteration failed: {}", e);
                    continue;
                }
            };

            for batch in batches {
                if self.sender.send(Ok(batch)).await.is_err() {
                    return;
                }
            }

            if let Some(duration) = self.interval {
                sleep(duration).await;
            }
        }
    }
}

#[derive(Debug)]
pub struct DynamodbStream {
    pub receiver: mpsc::Receiver<StreamResult>,
}

impl Stream for DynamodbStream {
    type Item = StreamResult;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for DynamodbStream {
    fn drop(&mut self) {
        self.receiver.close();
    }
}
