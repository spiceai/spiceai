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
    error::Error,
    types::{GetRecordsOutput, GetShardsOutput, Shard},
};

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_dynamodb::Client as DbClient;
use aws_sdk_dynamodbstreams::{
    Client as StreamsClient,
    error::SdkError,
    operation::{
        get_records::{GetRecordsError, GetRecordsOutput as SdkGetRecordsOutput},
        get_shard_iterator::GetShardIteratorError,
    },
    types::ShardIteratorType,
};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct SDKClient {
    db: DbClient,
    streams: StreamsClient,
}

impl SDKClient {
    #[must_use]
    pub fn new(config: &SdkConfig) -> Self {
        Self {
            db: DbClient::new(config),
            streams: StreamsClient::new(config),
        }
    }
}

#[async_trait]
pub trait DynamodbSDKClient: Send + Sync {
    async fn get_stream_arn(&self, table_name: String) -> Result<String, Error>;

    async fn get_shards(
        &self,
        stream_arn: &str,
        exclusive_start_shard_id: Option<String>,
    ) -> Result<GetShardsOutput, Error>;

    async fn get_all_shards(&self, stream_arn: &str) -> Result<Vec<Shard>, Error> {
        let GetShardsOutput {
            mut shards,
            mut last_shard_id,
        } = self.get_shards(stream_arn, None).await?;

        while last_shard_id.is_some() {
            let mut output = self.get_shards(stream_arn, last_shard_id.take()).await?;
            shards.append(&mut output.shards);
            last_shard_id = output.last_shard_id;
        }

        Ok(shards)
    }

    async fn get_shard_with_iterator(
        &self,
        stream_arn: String,
        shard_id: &str,
        parent_shard_id: Option<&str>,
        shard_iterator_type: &ShardIteratorType,
        sequence_number: Option<String>,
    ) -> Result<Shard, Error>;

    async fn get_records(&self, shard: Shard) -> Result<GetRecordsOutput, Error>;
}

#[async_trait]
impl DynamodbSDKClient for SDKClient {
    async fn get_stream_arn(&self, table_name: String) -> Result<String, Error> {
        let table_name: String = table_name;

        self.db
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
            .map_err(|err| Error::SdkError {
                source: Box::new(err),
            })?
            .table
            .and_then(|table| table.latest_stream_arn)
            .ok_or(Error::NotFoundStream { table_name })
    }

    async fn get_shards(
        &self,
        stream_arn: &str,
        exclusive_start_shard_id: Option<String>,
    ) -> Result<GetShardsOutput, Error> {
        let stream_arn: String = stream_arn.into();

        self.streams
            .describe_stream()
            .stream_arn(&stream_arn)
            .set_exclusive_start_shard_id(exclusive_start_shard_id)
            .send()
            .await
            .map_err(|err| Error::SdkError {
                source: Box::new(err),
            })?
            .stream_description
            .map(|description| {
                let shards = description
                    .shards
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(Shard::from_shard)
                    .collect::<Vec<Shard>>();
                let last_shard_id = description.last_evaluated_shard_id;

                GetShardsOutput {
                    shards,
                    last_shard_id,
                }
            })
            .ok_or(Error::NotFoundStreamDescription { stream_arn })
    }

    async fn get_shard_with_iterator(
        &self,
        stream_arn: String,
        shard_id: &str,
        parent_shard_id: Option<&str>,
        shard_iterator_type: &ShardIteratorType,
        sequence_number: Option<String>,
    ) -> Result<Shard, Error> {
        let iterator = self
            .streams
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard_id)
            .shard_iterator_type(shard_iterator_type.clone())
            .set_sequence_number(sequence_number)
            .send()
            .await
            .map(|output| output.shard_iterator)
            .or_else(empty_iterator)?;

        Ok(Shard::new(
            shard_id.to_string(),
            parent_shard_id.map(std::string::ToString::to_string),
            iterator,
        ))
    }

    async fn get_records(&self, shard: Shard) -> Result<GetRecordsOutput, Error> {
        let iterator = shard.iterator().map(std::string::ToString::to_string);

        self.streams
            .get_records()
            .set_shard_iterator(iterator)
            .send()
            .await
            .or_else(empty_records)
            .map(|output| {
                let shard = shard.set_iterator(output.next_shard_iterator);
                let records = output.records.unwrap_or_default();

                GetRecordsOutput { shard, records }
            })
    }
}

fn empty_iterator(err: SdkError<GetShardIteratorError>) -> Result<Option<String>, Error> {
    use GetShardIteratorError::{ResourceNotFoundException, TrimmedDataAccessException};

    match err {
        SdkError::ServiceError(e) => {
            let e = e.into_err();
            match e {
                // Retrun Ok(None) if the response is either `ResourceNotFound` or `TrimmedDataAccess`
                // This means the shard will drop silently because returning None as shard iterator
                // id results in returning Ok(None) from `get_shard_with_iterator` method.
                ResourceNotFoundException(_) | TrimmedDataAccessException(_) => {
                    warn!("GetShardIterator operation failed due to {e}");
                    warn!("{:#?}", e);
                    Ok(None)
                }
                _ => Err(Error::SdkError {
                    source: Box::new(e),
                }),
            }
        }
        _ => Err(Error::SdkError {
            source: Box::new(err),
        }),
    }
}

fn empty_records(err: SdkError<GetRecordsError>) -> Result<SdkGetRecordsOutput, Error> {
    use GetRecordsError::{
        ExpiredIteratorException, LimitExceededException, ResourceNotFoundException,
        TrimmedDataAccessException,
    };

    match err {
        SdkError::ServiceError(e) => {
            let e = e.into_err();
            match e {
                // Retrun Ok with default SdkGetRecordsOutput if the response is one of
                // `ExpiredIterator`, `LimitExceeded`, `ResourceNotFound` and `TrimmedDataAccess`.
                // This means the shard will drop silently because returning None as shard iterator
                // id results in returning None as shard in GetRecordsOutput from `get_records` method.
                ExpiredIteratorException(_)
                | LimitExceededException(_)
                | ResourceNotFoundException(_)
                | TrimmedDataAccessException(_) => {
                    warn!("GetRecords operation failed due to {e}");
                    warn!("{:#?}", e);
                    Ok(SdkGetRecordsOutput::builder().build())
                }
                _ => Err(Error::SdkError {
                    source: Box::new(e),
                }),
            }
        }
        _ => Err(Error::SdkError {
            source: Box::new(err),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_runtime_api::{
        client::{orchestrator::HttpResponse, result::ServiceError},
        http::StatusCode,
    };
    use aws_smithy_types::body::SdkBody;

    #[test]
    fn empty_iterator_converts_some_errors_to_ok() {
        use aws_sdk_dynamodbstreams::types::error::*;

        let e = ResourceNotFoundException::builder()
            .message("error")
            .build();
        let err = service_error(GetShardIteratorError::ResourceNotFoundException(e));
        assert!(empty_iterator(err).is_ok());

        let e = InternalServerError::builder().message("error").build();
        let err = service_error(GetShardIteratorError::InternalServerError(e));
        assert!(empty_iterator(err).is_err());

        let e = TrimmedDataAccessException::builder()
            .message("error")
            .build();
        let err = service_error(GetShardIteratorError::TrimmedDataAccessException(e));
        assert!(empty_iterator(err).is_ok());
    }

    #[test]
    fn empty_records_converts_some_errors_to_ok() {
        use aws_sdk_dynamodbstreams::types::error::*;

        let e = ResourceNotFoundException::builder()
            .message("error")
            .build();
        let err = service_error(GetRecordsError::ResourceNotFoundException(e));
        assert!(empty_records(err).is_ok());

        let e = InternalServerError::builder().message("error").build();
        let err = service_error(GetRecordsError::InternalServerError(e));
        assert!(empty_records(err).is_err());

        let e = ExpiredIteratorException::builder().message("error").build();
        let err = service_error(GetRecordsError::ExpiredIteratorException(e));
        assert!(empty_records(err).is_ok());

        let e = LimitExceededException::builder().message("error").build();
        let err = service_error(GetRecordsError::LimitExceededException(e));
        assert!(empty_records(err).is_ok());

        let e = TrimmedDataAccessException::builder()
            .message("error")
            .build();
        let err = service_error(GetRecordsError::TrimmedDataAccessException(e));
        assert!(empty_records(err).is_ok());
    }

    fn service_error<E>(error: E) -> SdkError<E, HttpResponse> {
        let resp = HttpResponse::new(StatusCode::try_from(400).unwrap(), SdkBody::empty());
        let inner = ServiceError::builder().source(error).raw(resp).build();
        SdkError::ServiceError(inner)
    }
}
