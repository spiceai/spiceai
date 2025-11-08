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
use super::{
    DynamodbSDKClient, Error,
    channel::{self, ConsumerChannel, ProducerChannel},
    types::{Lineages, Shard},
};
use crate::types::checkpoint::Checkpoint;
use crate::types::initial::InitialIteratorType;
use aws_sdk_dynamodbstreams::types::{Record, ShardIteratorType};
use std::collections::HashSet;
use std::{
    cmp,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    sync::mpsc,
    time::{Duration, sleep},
};
use tokio_stream::Stream;
use tracing;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(3);
const DEFAULT_BUFFER_SIZE: usize = 100;

#[derive(Debug)]
pub struct DynamodbStreamProducer<Client>
where
    Client: DynamodbSDKClient + 'static,
{
    table_name: String,
    stream_arn: String,
    shards: Option<Vec<Shard>>,
    channel: ProducerChannel,
    client: Arc<Client>,
    interval: Option<Duration>,
    sender: mpsc::Sender<Vec<Record>>,
    // Need to evict old shards from the set.
    seen_shard_ids: HashSet<String>,
}

impl<Client> DynamodbStreamProducer<Client>
where
    Client: DynamodbSDKClient + 'static,
{
    fn client(&self) -> Arc<Client> {
        Arc::clone(&self.client)
    }

    async fn init(&mut self, initial: InitialIteratorType) -> Result<(), Error> {
        let stream_arn = self.client.get_stream_arn(self.table_name.clone()).await?;
        self.stream_arn = stream_arn;

        let shards = match initial {
            InitialIteratorType::Latest => {
                self.initialize_all_shards(ShardIteratorType::Latest).await
            }
            InitialIteratorType::TrimHorizon => {
                self.initialize_all_shards(ShardIteratorType::TrimHorizon)
                    .await
            }
            InitialIteratorType::AtCheckpoint(checkpoint) => {
                self.initialize_checkpoint(checkpoint, ShardIteratorType::AtSequenceNumber)
                    .await
            }
            InitialIteratorType::AfterCheckpoint(checkpoint) => {
                self.initialize_checkpoint(checkpoint, ShardIteratorType::AfterSequenceNumber)
                    .await
            }
        }?;

        self.shards = Some(shards);
        self.channel.send_init();

        Ok(())
    }

    async fn initialize_all_shards(
        &self,
        iterator_type: ShardIteratorType,
    ) -> Result<Vec<Shard>, Error> {
        let shards = self.client.get_all_shards(&self.stream_arn).await?;
        let shards = self.get_shard_iterators(shards, iterator_type).await;

        Ok(shards)
    }

    async fn initialize_checkpoint(
        &self,
        checkpoint: Checkpoint,
        iterator_type: ShardIteratorType,
    ) -> Result<Vec<Shard>, Error> {
        let mut shards = Vec::new();

        for (shard_id, sequence_number) in checkpoint.shard_sequence_numbers {
            let shard = self
                .client
                .get_shard_with_iterator(
                    self.stream_arn.clone(),
                    &shard_id,
                    None,
                    &iterator_type,
                    Some(sequence_number),
                )
                .await?;
            shards.push(shard);
        }

        Ok(shards)
    }

    async fn iterate(&mut self) -> Result<Vec<Vec<Record>>, Error> {
        let shards_to_look_into = self.shards.take().unwrap_or_default();
        for shard in &shards_to_look_into {
            self.seen_shard_ids.insert(shard.id().to_string());
        }

        // This buffer prevents mpsc::channel from panic when passed zero as its argument.
        let buf = cmp::max(1, shards_to_look_into.len());
        let (tx, mut rx) = mpsc::channel::<(Option<Shard>, Vec<Record>)>(buf);

        // lineages based on shards we want to look into
        let lineages: Lineages = shards_to_look_into.clone().into();

        lineages.get_records(&self.client(), &tx);

        let mut shards: Vec<Shard> = vec![];
        let mut records: Vec<Vec<Record>> = vec![];

        while let Some((opt, shard_records)) = rx.recv().await {
            // These shards represent shards with non-empty iterator
            if let Some(shard) = opt {
                shards.push(shard);
            }

            if !shard_records.is_empty() {
                records.push(shard_records);
            }
        }

        let new_shards = self
            .client
            .get_all_shards(&self.stream_arn)
            .await?
            .into_iter()
            .filter(|shard| !self.seen_shard_ids.contains(shard.id()))
            .collect::<Vec<Shard>>();

        let mut new_shards = self
            .get_shard_iterators(new_shards, ShardIteratorType::TrimHorizon)
            .await;

        shards.append(&mut new_shards);
        self.shards = Some(shards);

        Ok(records)
    }

    async fn streaming(&mut self, initial: InitialIteratorType) {
        ok_or_return!(self.init(initial).await, |err| {
            tracing::error!(
                "Unexpected error during initialization: {err}. Skip polling {} table.",
                self.table_name,
            );
        });

        loop {
            let record_batches = ok_or_return!(self.iterate().await, |err| {
                tracing::error!(
                    "Unexpected error during iteration: {err}. Stop polling {} table.",
                    self.table_name,
                );
            });

            if self.channel.should_close() {
                return;
            }

            if !record_batches.is_empty() {
                for record_batch in record_batches {
                    if self.sender.send(record_batch).await.is_err() {
                        return;
                    }
                }
            }

            if let Some(duration) = self.interval {
                sleep(duration).await;
            }
        }
    }

    async fn get_shard_iterators(
        &self,
        shards: Vec<Shard>,
        shard_iterator_type: ShardIteratorType,
    ) -> Vec<Shard> {
        // The buffer size must be positive (not zero).
        let buf = cmp::max(1, shards.len());
        let (tx, mut rx) = mpsc::channel::<Shard>(buf);
        let mut output: Vec<Shard> = vec![];
        let client = self.client();

        for shard in shards {
            let tx = tx.clone();
            let client = Arc::clone(&client);
            let stream_arn = self.stream_arn.clone();
            let shard_iterator_type = shard_iterator_type.clone();

            tokio::spawn(async move {
                let result = client.get_shard_with_iterator(
                    stream_arn,
                    shard.id(),
                    shard.parent_shard_id(),
                    &shard_iterator_type,
                    None,
                );

                let shard = ok_or_return!(result.await, |err| {
                    tracing::error!("Unexpected error during getting shard iterator: {err}");
                });

                if let Err(err) = tx.send(shard).await {
                    tracing::error!("Unexpected error during sending shard: {err}");
                }
            });
        }

        drop(tx);

        while let Some(shard) = rx.recv().await {
            output.push(shard);
        }

        output
    }
}

#[derive(Debug)]
pub struct DynamodbStream {
    receiver: mpsc::Receiver<Vec<Record>>,
    channel: Option<ConsumerChannel>,
}

impl DynamodbStream {
    pub fn take_channel(&mut self) -> Option<ConsumerChannel> {
        self.channel.take()
    }
}

impl Stream for DynamodbStream {
    type Item = Vec<Record>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for DynamodbStream {
    fn drop(&mut self) {
        self.receiver.close();
        if let Some(mut channel) = self.take_channel() {
            channel.close(|| {});
        }
    }
}

#[derive(Debug)]
pub struct DynamodbStreamBuilder<Client>
where
    Client: DynamodbSDKClient + 'static,
{
    table_name: String,
    client: Client,
    interval: Option<Duration>,
    buffer: usize,
    initial_iterator_type: InitialIteratorType,
}

impl<Client> DynamodbStreamBuilder<Client>
where
    Client: DynamodbSDKClient + 'static,
{
    #[must_use]
    pub fn new(
        client: Client,
        table_name: String,
        initial_iterator_type: InitialIteratorType,
    ) -> Self {
        Self {
            client,
            table_name,
            interval: Some(DEFAULT_INTERVAL),
            buffer: DEFAULT_BUFFER_SIZE,
            initial_iterator_type,
        }
    }

    #[must_use]
    pub fn interval(self, interval: Option<Duration>) -> Self {
        Self { interval, ..self }
    }

    #[must_use]
    pub fn buffer(self, buffer: usize) -> Self {
        Self { buffer, ..self }
    }

    pub fn build(self) -> DynamodbStream {
        let (c_half, rx) = self.build_producer();

        DynamodbStream {
            receiver: rx,
            channel: Some(c_half),
        }
    }

    fn build_producer(self) -> (ConsumerChannel, mpsc::Receiver<Vec<Record>>) {
        let (p_half, c_half) = channel::new();
        let (tx_mpsc, rx_mpsc) = mpsc::channel::<Vec<Record>>(self.buffer);

        let mut producer = DynamodbStreamProducer {
            table_name: self.table_name,
            stream_arn: String::new(),
            shards: None,
            channel: p_half,
            client: Arc::new(self.client),
            interval: self.interval,
            sender: tx_mpsc,
            seen_shard_ids: HashSet::new(),
        };

        let initial_iterator_type = self.initial_iterator_type;

        tokio::spawn(async move {
            producer.streaming(initial_iterator_type).await;
        });

        (c_half, rx_mpsc)
    }
}
