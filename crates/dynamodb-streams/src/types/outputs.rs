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
use super::Shard;

use aws_sdk_dynamodbstreams::types::Record;

#[derive(Debug, Clone)]
pub struct GetShardsOutput {
    pub shards: Vec<Shard>,

    /// The shard ID of the item where the operation stopped, inclusive of the previous result set.
    /// Use this value to start a new operation, excluding this value in the new request.
    pub last_shard_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GetRecordsOutput {
    /// The shard will be None, if the renewed shard iterator is None. Because the None shard
    /// iterator means no more records will be retrieved from the shard.
    pub shard: Option<Shard>,

    pub records: Vec<Record>,
}
