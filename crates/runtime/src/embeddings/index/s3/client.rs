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
use s3_vectors::{
    Client, CreateIndexError, CreateIndexInput, CreateIndexOutput, CreateVectorBucketError,
    CreateVectorBucketInput, CreateVectorBucketOutput, DeleteIndexError, DeleteIndexInput,
    DeleteIndexOutput, DeleteVectorBucketError, DeleteVectorBucketInput, DeleteVectorBucketOutput,
    DeleteVectorBucketPolicyError, DeleteVectorBucketPolicyInput, DeleteVectorBucketPolicyOutput,
    DeleteVectorsError, DeleteVectorsInput, DeleteVectorsOutput, GetIndexError, GetIndexInput,
    GetIndexOutput, GetVectorBucketError, GetVectorBucketInput, GetVectorBucketOutput,
    GetVectorBucketPolicyError, GetVectorBucketPolicyInput, GetVectorBucketPolicyOutput,
    GetVectorsError, GetVectorsInput, GetVectorsOutput, ListIndexesError, ListIndexesInput,
    ListIndexesOutput, ListVectorBucketsError, ListVectorBucketsInput, ListVectorBucketsOutput,
    ListVectorsError, ListVectorsInput, ListVectorsOutput, PutVectorBucketPolicyError,
    PutVectorBucketPolicyInput, PutVectorBucketPolicyOutput, PutVectorsError, PutVectorsInput,
    PutVectorsOutput, QueryVectorsError, QueryVectorsInput, QueryVectorsOutput, S3Vectors,
    SdkError,
};

use crate::timing::TimeMeasurement;

pub struct S3VectorClient {
    client: Client,
}
impl S3VectorClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}
#[async_trait]
impl S3Vectors for S3VectorClient {
    async fn create_index(
        &self,
        input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError>> {
        let _guard = TimeMeasurement::new(&super::metrics::create_index::LATENCY, &[]);
        super::metrics::create_index::REQUESTS.add(1, &[]);

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
            .inspect_err(|_| super::metrics::create_index::ERRORS.add(1, &[]))
    }

    async fn create_vector_bucket(
        &self,
        input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError>> {
        let _guard = TimeMeasurement::new(&super::metrics::create_vector_bucket::LATENCY, &[]);
        super::metrics::create_vector_bucket::REQUESTS.add(1, &[]);

        self.client
            .create_vector_bucket()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_encryption_configuration(input.encryption_configuration)
            .send()
            .await
            .inspect_err(|_| super::metrics::create_vector_bucket::ERRORS.add(1, &[]))
    }

    async fn delete_index(
        &self,
        input: DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, SdkError<DeleteIndexError>> {
        let _guard = TimeMeasurement::new(&super::metrics::delete_index::LATENCY, &[]);
        super::metrics::delete_index::REQUESTS.add(1, &[]);

        self.client
            .delete_index()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .send()
            .await
            .inspect_err(|_| super::metrics::delete_index::ERRORS.add(1, &[]))
    }

    async fn delete_vector_bucket(
        &self,
        input: DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, SdkError<DeleteVectorBucketError>> {
        let _guard = TimeMeasurement::new(&super::metrics::delete_vector_bucket::LATENCY, &[]);
        super::metrics::delete_vector_bucket::REQUESTS.add(1, &[]);

        self.client
            .delete_vector_bucket()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .send()
            .await
            .inspect_err(|_| super::metrics::delete_vector_bucket::ERRORS.add(1, &[]))
    }

    async fn delete_vector_bucket_policy(
        &self,
        input: DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, SdkError<DeleteVectorBucketPolicyError>> {
        let _guard =
            TimeMeasurement::new(&super::metrics::delete_vector_bucket_policy::LATENCY, &[]);
        super::metrics::delete_vector_bucket_policy::REQUESTS.add(1, &[]);

        self.client
            .delete_vector_bucket_policy()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .send()
            .await
            .inspect_err(|_| super::metrics::delete_vector_bucket_policy::ERRORS.add(1, &[]))
    }

    async fn delete_vectors(
        &self,
        input: DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, SdkError<DeleteVectorsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::delete_vectors::LATENCY, &[]);
        super::metrics::delete_vectors::REQUESTS.add(1, &[]);

        self.client
            .delete_vectors()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .set_keys(input.keys)
            .send()
            .await
            .inspect_err(|_| super::metrics::delete_vectors::ERRORS.add(1, &[]))
    }

    async fn get_vector_bucket_policy(
        &self,
        input: GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, SdkError<GetVectorBucketPolicyError>> {
        let _guard = TimeMeasurement::new(&super::metrics::get_vector_bucket_policy::LATENCY, &[]);
        super::metrics::get_vector_bucket_policy::REQUESTS.add(1, &[]);

        self.client
            .get_vector_bucket_policy()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .send()
            .await
            .inspect_err(|_| super::metrics::get_vector_bucket_policy::ERRORS.add(1, &[]))
    }

    async fn get_index(
        &self,
        input: GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError>> {
        let _guard = TimeMeasurement::new(&super::metrics::get_index::LATENCY, &[]);
        super::metrics::get_index::REQUESTS.add(1, &[]);

        self.client
            .get_index()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .send()
            .await
            .inspect_err(|_| super::metrics::get_index::ERRORS.add(1, &[]))
    }

    async fn get_vector_bucket(
        &self,
        input: GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError>> {
        let _guard = TimeMeasurement::new(&super::metrics::get_vector_bucket::LATENCY, &[]);
        super::metrics::get_vector_bucket::REQUESTS.add(1, &[]);

        self.client
            .get_vector_bucket()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .send()
            .await
            .inspect_err(|_| super::metrics::get_vector_bucket::ERRORS.add(1, &[]))
    }

    async fn get_vectors(
        &self,
        input: GetVectorsInput,
    ) -> Result<GetVectorsOutput, SdkError<GetVectorsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::get_vectors::LATENCY, &[]);
        super::metrics::get_vectors::REQUESTS.add(1, &[]);

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
            .inspect_err(|_| super::metrics::get_vectors::ERRORS.add(1, &[]))
    }

    async fn list_indexes(
        &self,
        input: ListIndexesInput,
    ) -> Result<ListIndexesOutput, SdkError<ListIndexesError>> {
        let _guard = TimeMeasurement::new(&super::metrics::list_indexes::LATENCY, &[]);
        super::metrics::list_indexes::REQUESTS.add(1, &[]);

        self.client
            .list_indexes()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .set_max_results(input.max_results)
            .set_next_token(input.next_token)
            .set_prefix(input.prefix)
            .send()
            .await
            .inspect_err(|_| super::metrics::list_indexes::ERRORS.add(1, &[]))
    }

    async fn list_vector_buckets(
        &self,
        input: ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, SdkError<ListVectorBucketsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::list_vector_buckets::LATENCY, &[]);
        super::metrics::list_vector_buckets::REQUESTS.add(1, &[]);

        self.client
            .list_vector_buckets()
            .set_max_results(input.max_results)
            .set_next_token(input.next_token)
            .set_prefix(input.prefix)
            .send()
            .await
            .inspect_err(|_| super::metrics::list_vector_buckets::ERRORS.add(1, &[]))
    }

    async fn list_vectors(
        &self,
        input: ListVectorsInput,
    ) -> Result<ListVectorsOutput, SdkError<ListVectorsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::list_vectors::LATENCY, &[]);
        super::metrics::list_vectors::REQUESTS.add(1, &[]);

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
            .inspect_err(|_| super::metrics::list_vectors::ERRORS.add(1, &[]))
    }

    async fn put_vector_bucket_policy(
        &self,
        input: PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, SdkError<PutVectorBucketPolicyError>> {
        let _guard = TimeMeasurement::new(&super::metrics::put_vector_bucket_policy::LATENCY, &[]);
        super::metrics::put_vector_bucket_policy::REQUESTS.add(1, &[]);

        self.client
            .put_vector_bucket_policy()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_vector_bucket_arn(input.vector_bucket_arn)
            .set_policy(input.policy)
            .send()
            .await
            .inspect_err(|_| super::metrics::put_vector_bucket_policy::ERRORS.add(1, &[]))
    }

    async fn put_vectors(
        &self,
        input: PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::put_vectors::LATENCY, &[]);
        super::metrics::put_vectors::REQUESTS.add(1, &[]);

        self.client
            .put_vectors()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .set_vectors(input.vectors)
            .send()
            .await
            .inspect_err(|_| super::metrics::put_vectors::ERRORS.add(1, &[]))
    }

    async fn query_vectors(
        &self,
        input: QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::query_vectors::LATENCY, &[]);
        super::metrics::query_vectors::REQUESTS.add(1, &[]);

        self.client
            .query_vectors()
            .set_vector_bucket_name(input.vector_bucket_name)
            .set_index_name(input.index_name)
            .set_index_arn(input.index_arn)
            .set_query_vector(input.query_vector)
            .set_top_k(input.top_k)
            .set_filter(input.filter)
            .set_return_metadata(input.return_metadata)
            .set_return_distance(input.return_distance)
            .send()
            .await
            .inspect_err(|_| super::metrics::query_vectors::ERRORS.add(1, &[]))
    }
}
