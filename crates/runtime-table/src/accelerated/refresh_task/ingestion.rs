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

use data_components::cdc::{
    self,
    mutation::{Recovery, ReplaceSet},
};
use datafusion::error::{DataFusionError, Result};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{mpsc, oneshot};

pub(crate) enum Mutation {
    Rows(cdc::ChangeEnvelope),
    ReplaceSet {
        replacement: ReplaceSet,
        recovery: Recovery,
        published: oneshot::Sender<Result<()>>,
    },
}

impl Mutation {
    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Self::Rows(envelope) => envelope.encoded_len(),
            Self::ReplaceSet { replacement, .. } => replacement.retained_bytes(),
        }
    }

    pub(crate) fn source_commit_ts_ms(&self) -> Option<i64> {
        match self {
            Self::Rows(envelope) => envelope.source_commit_ts_ms(),
            Self::ReplaceSet { .. } => None,
        }
    }

    pub(crate) fn is_heartbeat(&self) -> bool {
        matches!(self, Self::Rows(envelope) if envelope.is_heartbeat())
    }
}

pub(crate) type IngestItem = std::result::Result<Mutation, cdc::StreamError>;

pub(crate) struct IngestInput {
    pub(crate) receiver: mpsc::Receiver<IngestItem>,
    pub(crate) probe: mpsc::WeakSender<IngestItem>,
    pub(crate) queued_bytes: Arc<AtomicU64>,
    pub(crate) write_context: Option<datafusion::prelude::SessionContext>,
}

/// All producers feed the same bounded changes-driver channel.
#[derive(Clone)]
pub(crate) struct IngestSender {
    sender: mpsc::Sender<IngestItem>,
    queued_bytes: Arc<AtomicU64>,
}

pub(crate) fn channel(capacity: usize) -> (IngestSender, IngestInput) {
    let (sender, receiver) = mpsc::channel(capacity);
    let queued_bytes = Arc::new(AtomicU64::new(0));
    let input = IngestInput {
        receiver,
        probe: sender.downgrade(),
        queued_bytes: Arc::clone(&queued_bytes),
        write_context: None,
    };
    (
        IngestSender {
            sender,
            queued_bytes,
        },
        input,
    )
}

impl IngestSender {
    pub(crate) async fn closed(&self) {
        self.sender.closed().await;
    }

    pub(crate) async fn send_source(
        &self,
        item: std::result::Result<cdc::ChangeEnvelope, cdc::StreamError>,
    ) -> Result<()> {
        let permit = self.sender.reserve().await.map_err(|_| closed())?;
        let item = item.map(Mutation::Rows);
        self.queued_bytes.fetch_add(
            item.as_ref().map_or(0, Mutation::encoded_len) as u64,
            Ordering::Relaxed,
        );
        permit.send(item);
        Ok(())
    }

    /// Admission precedes transferring the complete input to the service.
    pub(crate) async fn admit(&self, deadline: tokio::time::Instant) -> Result<Admission> {
        let permit = tokio::time::timeout_at(deadline, self.sender.clone().reserve_owned())
            .await
            .map_err(|_| {
                DataFusionError::ResourcesExhausted(
                    "Change ingestion admission deadline exceeded".into(),
                )
            })?
            .map_err(|_| closed())?;
        Ok(Admission {
            permit,
            queued_bytes: Arc::clone(&self.queued_bytes),
        })
    }
}

pub(crate) struct Admission {
    permit: mpsc::OwnedPermit<IngestItem>,
    queued_bytes: Arc<AtomicU64>,
}

impl Admission {
    pub(crate) fn submit(self, replacement: ReplaceSet, recovery: Recovery) -> IngestReceipt {
        let (published, receiver) = oneshot::channel();
        self.queued_bytes
            .fetch_add(replacement.retained_bytes() as u64, Ordering::Relaxed);
        self.permit.send(Ok(Mutation::ReplaceSet {
            replacement,
            recovery,
            published,
        }));
        IngestReceipt(receiver)
    }
}

/// Publication completion is not a source acknowledgement or a durability receipt.
pub(crate) struct IngestReceipt(oneshot::Receiver<Result<()>>);

impl IngestReceipt {
    pub(crate) async fn published(self) -> Result<()> {
        self.0.await.map_err(|_| closed())?
    }
}

/// Dropping a session also stops its optional upstream reader.
pub(crate) struct ReaderTask(Option<tokio::task::JoinHandle<()>>);

impl ReaderTask {
    pub(crate) fn new(task: Option<tokio::task::JoinHandle<()>>) -> Self {
        Self(task)
    }
    pub(crate) fn is_source(&self) -> bool {
        self.0.is_some()
    }
    pub(crate) fn abort(&self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

impl std::future::Future for ReaderTask {
    type Output = std::result::Result<(), tokio::task::JoinError>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.0.as_mut() {
            Some(task) => std::pin::Pin::new(task).poll(cx),
            None => std::task::Poll::Ready(Ok(())),
        }
    }
}

impl Drop for ReaderTask {
    fn drop(&mut self) {
        self.abort();
    }
}

fn closed() -> DataFusionError {
    DataFusionError::Execution(
        "Change ingestion is closed; accepted publication may require recovery".into(),
    )
}
