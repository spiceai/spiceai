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

use async_trait::async_trait;
use aws_config::SdkConfig;

pub use aws_sdk_s3vectors::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        create_index::{CreateIndexError, CreateIndexInput, CreateIndexOutput},
        create_vector_bucket::{
            CreateVectorBucketError, CreateVectorBucketInput, CreateVectorBucketOutput,
        },
        delete_index::{DeleteIndexError, DeleteIndexInput, DeleteIndexOutput},
        delete_vector_bucket::{
            DeleteVectorBucketError, DeleteVectorBucketInput, DeleteVectorBucketOutput,
        },
        delete_vector_bucket_policy::{
            DeleteVectorBucketPolicyError, DeleteVectorBucketPolicyInput,
            DeleteVectorBucketPolicyOutput,
        },
        delete_vectors::{DeleteVectorsError, DeleteVectorsInput, DeleteVectorsOutput},
        get_index::{GetIndexError, GetIndexInput, GetIndexOutput},
        get_vector_bucket::{GetVectorBucketError, GetVectorBucketInput, GetVectorBucketOutput},
        get_vector_bucket_policy::{
            GetVectorBucketPolicyError, GetVectorBucketPolicyInput, GetVectorBucketPolicyOutput,
        },
        get_vectors::{GetVectorsError, GetVectorsInput, GetVectorsOutput},
        list_indexes::{ListIndexesError, ListIndexesInput, ListIndexesOutput},
        list_vector_buckets::{
            ListVectorBucketsError, ListVectorBucketsInput, ListVectorBucketsOutput,
        },
        list_vectors::{ListVectorsError, ListVectorsInput, ListVectorsOutput},
        put_vector_bucket_policy::{
            PutVectorBucketPolicyError, PutVectorBucketPolicyInput, PutVectorBucketPolicyOutput,
        },
        put_vectors::{PutVectorsError, PutVectorsInput, PutVectorsOutput},
        query_vectors::{QueryVectorsError, QueryVectorsInput, QueryVectorsOutput},
    },
    types::{
        DataType, DistanceMetric, IndexSummary, ListOutputVector, MetadataConfiguration,
        PutInputVector, QueryOutputVector, VectorData,
    },
};
pub use aws_smithy_types::{DateTime, Document, Number, error::operation::BuildError};

pub static LIST_VECTORS_MAX_RESULTS: usize = 500;
pub static PUT_VECTORS_MAX_ITEMS: usize = 500;

/// Wrapper for `aws_sdk_s3vectors::Client` that implements the `S3Vectors` trait
#[derive(Debug)]
pub struct Client {
    client: aws_sdk_s3vectors::Client,
}

impl Client {
    #[must_use]
    pub fn new(config: &SdkConfig) -> Self {
        Self {
            client: aws_sdk_s3vectors::Client::new(config),
        }
    }
}

pub mod mock;

#[async_trait]
impl S3Vectors for Client {
    async fn create_index(
        &self,
        input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError>> {
        self.client
            .create_index()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_data_type(input.data_type)
            .set_dimension(input.dimension)
            .set_distance_metric(input.distance_metric)
            .set_metadata_configuration(input.metadata_configuration)
            .send()
            .await
    }

    async fn create_vector_bucket(
        &self,
        input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError>> {
        self.client
            .create_vector_bucket()
            .set_vector_bucket_name(input.vector_bucket_name)
            .send()
            .await
    }

    async fn delete_index(
        &self,
        input: DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, SdkError<DeleteIndexError>> {
        self.client
            .delete_index()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .send()
            .await
    }

    async fn delete_vector_bucket(
        &self,
        input: DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, SdkError<DeleteVectorBucketError>> {
        self.client
            .delete_vector_bucket()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .send()
            .await
    }

    async fn delete_vector_bucket_policy(
        &self,
        input: DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, SdkError<DeleteVectorBucketPolicyError>> {
        self.client
            .delete_vector_bucket_policy()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .send()
            .await
    }

    async fn delete_vectors(
        &self,
        input: DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, SdkError<DeleteVectorsError>> {
        self.client
            .delete_vectors()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .set_keys(input.keys)
            .send()
            .await
    }

    async fn get_index(
        &self,
        input: GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError>> {
        self.client
            .get_index()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .send()
            .await
    }

    async fn get_vector_bucket(
        &self,
        input: GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError>> {
        self.client
            .get_vector_bucket()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .send()
            .await
    }

    async fn get_vector_bucket_policy(
        &self,
        input: GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, SdkError<GetVectorBucketPolicyError>> {
        self.client
            .get_vector_bucket_policy()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .send()
            .await
    }

    async fn get_vectors(
        &self,
        input: GetVectorsInput,
    ) -> Result<GetVectorsOutput, SdkError<GetVectorsError>> {
        self.client
            .get_vectors()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .set_keys(input.keys)
            .set_return_data(input.return_data)
            .set_return_metadata(input.return_metadata)
            .send()
            .await
    }

    async fn list_indexes(
        &self,
        input: ListIndexesInput,
    ) -> Result<ListIndexesOutput, SdkError<ListIndexesError>> {
        self.client
            .list_indexes()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .set_max_results(input.max_results)
            .set_next_token(input.next_token)
            .set_prefix(input.prefix)
            .send()
            .await
    }

    async fn list_vector_buckets(
        &self,
        input: ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, SdkError<ListVectorBucketsError>> {
        self.client
            .list_vector_buckets()
            .set_max_results(input.max_results)
            .set_next_token(input.next_token)
            .set_prefix(input.prefix)
            .send()
            .await
    }

    async fn list_vectors(
        &self,
        input: ListVectorsInput,
    ) -> Result<ListVectorsOutput, SdkError<ListVectorsError>> {
        self.client
            .list_vectors()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .set_max_results(input.max_results)
            .set_next_token(input.next_token)
            .set_segment_count(input.segment_count)
            .set_segment_index(input.segment_index)
            .set_return_data(input.return_data)
            .set_return_metadata(input.return_metadata)
            .send()
            .await
    }

    async fn put_vector_bucket_policy(
        &self,
        input: PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, SdkError<PutVectorBucketPolicyError>> {
        self.client
            .put_vector_bucket_policy()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .set_policy(input.policy)
            .send()
            .await
    }

    async fn put_vectors(
        &self,
        input: PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError>> {
        self.client
            .put_vectors()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .set_vectors(input.vectors)
            .send()
            .await
    }

    async fn query_vectors(
        &self,
        input: QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError>> {
        self.client
            .query_vectors()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .set_query_vector(input.query_vector)
            .set_top_k(input.top_k)
            .set_return_distance(input.return_distance)
            .set_return_metadata(input.return_metadata)
            .set_filter(input.filter)
            .send()
            .await
    }
}

/// Trait representing the capabilities of the Amazon S3 Vectors API. Amazon S3 Vectors clients implement this trait.
#[async_trait]
pub trait S3Vectors {
    async fn create_index(
        &self,
        input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError, HttpResponse>>;

    async fn create_vector_bucket(
        &self,
        input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError, HttpResponse>>;

    async fn delete_index(
        &self,
        input: DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, SdkError<DeleteIndexError, HttpResponse>>;

    async fn delete_vector_bucket(
        &self,
        input: DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, SdkError<DeleteVectorBucketError, HttpResponse>>;

    async fn delete_vector_bucket_policy(
        &self,
        input: DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, SdkError<DeleteVectorBucketPolicyError, HttpResponse>>;

    async fn delete_vectors(
        &self,
        input: DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, SdkError<DeleteVectorsError, HttpResponse>>;

    async fn get_index(
        &self,
        input: GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError, HttpResponse>>;

    async fn get_vector_bucket(
        &self,
        input: GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError, HttpResponse>>;

    async fn get_vector_bucket_policy(
        &self,
        input: GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, SdkError<GetVectorBucketPolicyError, HttpResponse>>;

    async fn get_vectors(
        &self,
        input: GetVectorsInput,
    ) -> Result<GetVectorsOutput, SdkError<GetVectorsError, HttpResponse>>;

    async fn list_indexes(
        &self,
        input: ListIndexesInput,
    ) -> Result<ListIndexesOutput, SdkError<ListIndexesError, HttpResponse>>;

    async fn list_vector_buckets(
        &self,
        input: ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, SdkError<ListVectorBucketsError, HttpResponse>>;

    async fn list_vectors(
        &self,
        input: ListVectorsInput,
    ) -> Result<ListVectorsOutput, SdkError<ListVectorsError, HttpResponse>>;

    async fn put_vector_bucket_policy(
        &self,
        input: PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, SdkError<PutVectorBucketPolicyError, HttpResponse>>;

    async fn put_vectors(
        &self,
        input: PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError, HttpResponse>>;

    async fn query_vectors(
        &self,
        input: QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError, HttpResponse>>;
}

#[cfg(test)]
pub mod tests {
    use core::panic;

    use aws_config::{BehaviorVersion, Region};
    use aws_sdk_s3vectors::{
        error::SdkError,
        types::{PutInputVector, VectorData},
    };

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    #[ignore = "reason unknown"]
    async fn test_s3_vectors() -> Result<(), String> {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let mut config_builder = config.into_builder();
        config_builder.set_region(Region::from_static("us-east-2"));
        let config = config_builder.build();
        let client = aws_sdk_s3vectors::Client::new(&config);

        match client
            .create_vector_bucket()
            .vector_bucket_name("demo")
            .send()
            .await
        {
            Ok(_) => (),
            Err(sdk_error) => match sdk_error {
                SdkError::ConstructionFailure(e) => panic!("Construction failure: {e:?}"),
                SdkError::TimeoutError(e) => panic!("Timeout error: {e:?}"),
                SdkError::DispatchFailure(e) => panic!("Dispatch failure: {e:?}"),
                SdkError::ResponseError(e) => panic!("Response error: {e:?}"),
                _ => match sdk_error.into_service_error() {
                    aws_sdk_s3vectors::operation::create_vector_bucket::CreateVectorBucketError::ConflictException(_) => (),
                    e => panic!("Unexpected error: {e}"),
                },
            },
        }

        match client
            .create_index()
            .data_type(aws_sdk_s3vectors::types::DataType::Float32)
            .dimension(3)
            .distance_metric(aws_sdk_s3vectors::types::DistanceMetric::Cosine)
            .index_name("test")
            .vector_bucket_name("demo")
            .send()
            .await
        {
            Ok(_) => (),
            Err(e) => match e.into_service_error() {
                aws_sdk_s3vectors::operation::create_index::CreateIndexError::ConflictException(
                    _,
                ) => (),
                e => panic!("Unexpected error: {e}"),
            },
        }

        let _ = client
            .put_vectors()
            .index_name("test")
            .vector_bucket_name("demo")
            .vectors(
                PutInputVector::builder()
                    .key("v1")
                    .data(VectorData::Float32(vec![1.0, 2.0, 3.0]))
                    .build()
                    .expect("valid vector"),
            )
            .vectors(
                PutInputVector::builder()
                    .key("v2")
                    .data(VectorData::Float32(vec![4.0, 5.0, 6.0]))
                    .build()
                    .expect("valid vector"),
            )
            .vectors(
                PutInputVector::builder()
                    .key("v3")
                    .data(VectorData::Float32(vec![7.0, 8.0, 9.0]))
                    .build()
                    .expect("valid vector"),
            )
            .vectors(
                PutInputVector::builder()
                    .key("v4")
                    .data(VectorData::Float32(vec![2.0, 2.0, 2.0]))
                    .build()
                    .expect("valid vector"),
            )
            .send()
            .await
            .expect("put_vectors");

        let response = client
            .query_vectors()
            .index_name("test")
            .vector_bucket_name("demo")
            .return_distance(true)
            .return_metadata(true)
            .top_k(2)
            .query_vector(VectorData::Float32(vec![4.0, 5.0, 3.0]))
            .send()
            .await
            .expect("query_vectors");

        println!("query_vectors={response:?}");

        let response = client
            .list_vectors()
            .index_name("test")
            .vector_bucket_name("demo")
            .return_data(true)
            .return_metadata(true)
            .max_results(20)
            .send()
            .await
            .expect("list_vectors");

        println!("list_vectors={response:?}");

        Ok(())
    }
}
