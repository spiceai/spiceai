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
use aws_sdk_dynamodbstreams as dynamodbstreams;

/// A shard representation to retreive `DynamoDB` Streams records.
#[derive(Debug, Clone)]
pub struct Shard {
    id: String,
    iterator: Option<String>,
    parent_shard_id: Option<String>,
    pub ending_sequence_number: Option<String>,
}

impl Shard {
    #[must_use]
    pub fn new(
        shard_id: String,
        parent_shard_id: Option<String>,
        iterator: Option<String>,
    ) -> Self {
        Self {
            id: shard_id,
            iterator,
            parent_shard_id,
            ending_sequence_number: None,
        }
    }

    #[must_use]
    pub fn from_shard(shard: dynamodbstreams::types::Shard) -> Option<Self> {
        let dynamodbstreams::types::Shard {
            shard_id,
            parent_shard_id,
            sequence_number_range,
            ..
        } = shard;

        shard_id.map(|id| Self {
            id,
            iterator: None,
            parent_shard_id,
            ending_sequence_number: sequence_number_range.and_then(|r| r.ending_sequence_number),
        })
    }

    /// Return the shard id.
    #[must_use]
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Return the shard iterator id.
    #[must_use]
    pub fn iterator(&self) -> Option<&str> {
        self.iterator.as_deref()
    }

    /// Return the parent shard id.
    #[must_use]
    pub fn parent_shard_id(&self) -> Option<&str> {
        self.parent_shard_id.as_deref()
    }

    /// Return [`Option<Shard>`] with passed shard iterator id.
    /// Setting None as the shard iterator means the shard drops because None shard iterator will
    /// get no records from the `DynamoDB` Table.
    #[must_use]
    pub fn set_iterator(self, iterator: Option<String>) -> Option<Self> {
        if iterator.is_some() {
            Some(Self { iterator, ..self })
        } else {
            None
        }
    }
}
