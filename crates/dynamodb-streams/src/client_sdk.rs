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
use crate::{Error, Result};
use aws_config::SdkConfig;
use aws_sdk_dynamodb::Client as DbClient;
use aws_sdk_dynamodbstreams::types::Record;
use aws_sdk_dynamodbstreams::{Client as StreamsClient, types::ShardIteratorType};

#[derive(Debug, Clone)]
pub struct SDKClient {
    db: DbClient,
    streams: StreamsClient,
    shard_record_limit: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct ApiShard {
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
    pub starting_sequence_number: Option<String>,
    pub ending_sequence_number: Option<String>, // None = still open
}

impl SDKClient {
    pub fn new(config: &SdkConfig, shard_record_limit: Option<i32>) -> Self {
        Self {
            db: DbClient::new(config),
            streams: StreamsClient::new(config),
            shard_record_limit,
        }
    }

    pub async fn get_stream_arn(&self, table_name: String) -> Result<String> {
        let table = self
            .db
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
            .map_err(|e| Error::SDKError {
                source: Box::new(e),
            })?
            .table
            .ok_or_else(|| Error::StreamNotFound {
                table_name: table_name.clone(),
            })?;

        table
            .latest_stream_arn
            .ok_or_else(|| Error::StreamNotFound { table_name })
    }

    async fn get_shards(
        &self,
        stream_arn: &str,
        exclusive_start_shard_id: Option<String>,
    ) -> Result<(Vec<ApiShard>, Option<String>)> {
        let description = self
            .streams
            .describe_stream()
            .stream_arn(stream_arn)
            .set_exclusive_start_shard_id(exclusive_start_shard_id)
            .send()
            .await
            .map_err(|e| Error::SDKError {
                source: Box::new(e),
            })?
            .stream_description
            .ok_or_else(|| Error::StreamDescriptionNotFound {
                stream_arn: stream_arn.to_string(),
            })?;

        let shards = description
            .shards
            .unwrap_or_default()
            .into_iter()
            .map(|s| ApiShard {
                shard_id: s.shard_id.unwrap_or_default(),
                parent_shard_id: s.parent_shard_id,
                starting_sequence_number: s
                    .sequence_number_range
                    .as_ref()
                    .and_then(|r| r.starting_sequence_number.clone()),
                ending_sequence_number: s
                    .sequence_number_range
                    .and_then(|r| r.ending_sequence_number),
            })
            .collect();

        Ok((shards, description.last_evaluated_shard_id))
    }

    pub async fn get_shard_iterator(
        &self,
        stream_arn: &str,
        shard_id: &str,
        shard_iterator_type: &ShardIteratorType,
        sequence_number: Option<String>,
    ) -> Result<Option<String>> {
        Ok(self
            .streams
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard_id)
            .shard_iterator_type(shard_iterator_type.clone())
            .set_sequence_number(sequence_number.clone())
            .send()
            .await
            .map_err(|e| Error::SDKError {
                source: Box::new(e),
            })?
            .shard_iterator)
    }

    pub async fn get_iterator_records(
        &self,
        iterator: &str,
    ) -> Result<(Option<String>, Vec<Record>)> {
        let output = self
            .streams
            .get_records()
            .shard_iterator(iterator)
            .set_limit(self.shard_record_limit)
            .send()
            .await
            .map_err(|e| Error::SDKError {
                source: Box::new(e),
            })?;

        Ok((
            output.next_shard_iterator,
            output.records.unwrap_or_default(),
        ))
    }

    pub async fn get_all_shards(&self, stream_arn: &str) -> Result<Vec<ApiShard>> {
        let mut all_shards = Vec::new();
        let mut last_shard_id = None;

        loop {
            let (shards, next_shard_id) = self.get_shards(stream_arn, last_shard_id).await?;
            all_shards.extend(shards);

            last_shard_id = next_shard_id;
            if last_shard_id.is_none() {
                break;
            }
        }

        Ok(all_shards)
    }
}
