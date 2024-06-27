/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow::array::RecordBatch;
use snafu::prelude::*;

use crate::kafka::KafkaMessage;

#[derive(Debug, Snafu)]
pub enum CommitError {
    #[snafu(display("Unable to commit change: {source}"))]
    UnableToCommitChange {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Debug)]
pub enum StreamError {
    Kafka(String),
    SerdeJsonError(String),
}

impl std::error::Error for StreamError {}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::Kafka(e) => write!(f, "Kafka error: {e}"),
            StreamError::SerdeJsonError(e) => write!(f, "Serde JSON error: {e}"),
        }
    }
}

/// Allows to commit a change that has been processed.
pub trait CommitChange {
    fn commit(&self) -> Result<(), CommitError>;
}

pub struct ChangeEnvelope {
    change_committer: Box<dyn CommitChange + Send>,
    pub rb: RecordBatch,
}

impl ChangeEnvelope {
    #[must_use]
    pub fn new(change_committer: Box<dyn CommitChange + Send>, rb: RecordBatch) -> Self {
        Self {
            change_committer,
            rb,
        }
    }

    pub fn commit(self) -> Result<(), CommitError> {
        self.change_committer.commit()
    }
}

impl<K, V> CommitChange for KafkaMessage<'_, K, V> {
    fn commit(&self) -> Result<(), CommitError> {
        self.mark_processed()
            .boxed()
            .context(UnableToCommitChangeSnafu)?;
        Ok(())
    }
}
