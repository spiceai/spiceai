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

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use aws_config::SdkConfig;

pub use aws_sdk_s3vectors::{
    config::http::{HttpRequest, HttpResponse},
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

/// Maximum number of results returned per page in a `QueryVectors` API call.
pub static QUERY_VECTORS_PAGE_SIZE: i32 = 100;

/// Maximum topK value for a paginated `QueryVectors` request.
pub static QUERY_VECTORS_MAX_TOPK: i32 = 10_000;

/// Interceptor that captures the `nextToken` from the `QueryVectors` response body.
#[derive(Debug)]
struct CaptureNextToken {
    captured: Arc<Mutex<Option<CapturedNextToken>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CapturedNextToken {
    Token(String),
    Missing,
    Failed(&'static str),
}

impl aws_smithy_runtime_api::client::interceptors::Intercept for CaptureNextToken {
    fn name(&self) -> &'static str {
        "CaptureNextToken"
    }

    fn modify_before_deserialization(
        &self,
        context: &mut aws_smithy_runtime_api::client::interceptors::context::BeforeDeserializationInterceptorContextMut<'_>,
        _runtime_components: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
        _cfg: &mut aws_smithy_types::config_bag::ConfigBag,
    ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
        let body = context.response().body();
        let captured = match body.bytes() {
            Some(bytes) => match serde_json::from_slice::<serde_json::Value>(bytes) {
                Ok(json) => json
                    .get("nextToken")
                    .and_then(|v| v.as_str())
                    .map_or(CapturedNextToken::Missing, |token| {
                        CapturedNextToken::Token(token.to_string())
                    }),
                Err(_) => CapturedNextToken::Failed(
                    "S3 Vectors response body is not valid JSON while reading nextToken",
                ),
            },
            None => CapturedNextToken::Failed(
                "S3 Vectors response body is unavailable while reading nextToken",
            ),
        };

        if let Ok(mut guard) = self.captured.lock() {
            *guard = Some(captured);
        }
        Ok(())
    }
}

fn inject_query_vectors_next_token(
    mut request: HttpRequest,
    token: &str,
) -> std::result::Result<HttpRequest, std::io::Error> {
    let body_bytes = request.body().bytes().ok_or_else(|| {
        std::io::Error::other("Failed to paginate S3 QueryVectors: request body is unavailable")
    })?;
    let mut json = serde_json::from_slice::<serde_json::Value>(body_bytes).map_err(|source| {
        std::io::Error::other(format!(
            "Failed to paginate S3 QueryVectors: request body is not valid JSON: {source}"
        ))
    })?;
    json["nextToken"] = serde_json::Value::String(token.to_string());

    let new_body = serde_json::to_vec(&json).map_err(|source| {
        std::io::Error::other(format!(
            "Failed to paginate S3 QueryVectors: request body could not be serialized: {source}"
        ))
    })?;
    let content_length = new_body.len();
    *request.body_mut() = aws_smithy_types::body::SdkBody::from(new_body);
    request
        .headers_mut()
        .insert("content-length", content_length.to_string());

    Ok(request)
}

fn query_vectors_page_size_usize() -> usize {
    usize::try_from(QUERY_VECTORS_PAGE_SIZE).unwrap_or(usize::MAX)
}

fn next_query_vectors_request_token(
    retrieved_vectors: usize,
    page_vectors: usize,
    requested_top_k: usize,
    response_token: CapturedNextToken,
    previous_token: Option<&str>,
) -> std::result::Result<Option<String>, &'static str> {
    if retrieved_vectors >= requested_top_k {
        return Ok(None);
    }

    match response_token {
        CapturedNextToken::Token(token) if token.is_empty() => {
            if page_vectors < query_vectors_page_size_usize() {
                Ok(None)
            } else {
                Err("S3 Vectors returned a full page with an empty nextToken")
            }
        }
        CapturedNextToken::Token(token) if Some(token.as_str()) == previous_token => {
            Err("S3 Vectors returned a duplicate nextToken")
        }
        CapturedNextToken::Token(token) => Ok(Some(token)),
        CapturedNextToken::Missing => Ok(None),
        CapturedNextToken::Failed(_) if page_vectors < query_vectors_page_size_usize() => Ok(None),
        CapturedNextToken::Failed(reason) => Err(reason),
    }
}

fn query_vectors_pagination_error(
    top_k: i32,
    retrieved_vectors: usize,
    reason: &str,
) -> SdkError<QueryVectorsError> {
    SdkError::construction_failure(format!(
        "Failed to paginate S3 QueryVectors for topK {top_k}: {reason}; retrieved {retrieved_vectors} results"
    ))
}

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
        input: &CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError>> {
        self.client
            .create_index()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_index_name(input.index_name.clone())
            .set_data_type(input.data_type.clone())
            .set_dimension(input.dimension)
            .set_distance_metric(input.distance_metric.clone())
            .set_metadata_configuration(input.metadata_configuration.clone())
            .send()
            .await
    }

    async fn create_vector_bucket(
        &self,
        input: &CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError>> {
        self.client
            .create_vector_bucket()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .send()
            .await
    }

    async fn delete_index(
        &self,
        input: &DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, SdkError<DeleteIndexError>> {
        self.client
            .delete_index()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_index_name(input.index_name.clone())
            .set_index_arn(input.index_arn.clone())
            .send()
            .await
    }

    async fn delete_vector_bucket(
        &self,
        input: &DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, SdkError<DeleteVectorBucketError>> {
        self.client
            .delete_vector_bucket()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_vector_bucket_arn(input.vector_bucket_arn.clone())
            .send()
            .await
    }

    async fn delete_vector_bucket_policy(
        &self,
        input: &DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, SdkError<DeleteVectorBucketPolicyError>> {
        self.client
            .delete_vector_bucket_policy()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_vector_bucket_arn(input.vector_bucket_arn.clone())
            .send()
            .await
    }

    async fn delete_vectors(
        &self,
        input: &DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, SdkError<DeleteVectorsError>> {
        self.client
            .delete_vectors()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_index_name(input.index_name.clone())
            .set_index_arn(input.index_arn.clone())
            .set_keys(input.keys.clone())
            .send()
            .await
    }

    async fn get_index(
        &self,
        input: &GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError>> {
        self.client
            .get_index()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_index_name(input.index_name.clone())
            .set_index_arn(input.index_arn.clone())
            .send()
            .await
    }

    async fn get_vector_bucket(
        &self,
        input: &GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError>> {
        self.client
            .get_vector_bucket()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_vector_bucket_arn(input.vector_bucket_arn.clone())
            .send()
            .await
    }

    async fn get_vector_bucket_policy(
        &self,
        input: &GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, SdkError<GetVectorBucketPolicyError>> {
        self.client
            .get_vector_bucket_policy()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_vector_bucket_arn(input.vector_bucket_arn.clone())
            .send()
            .await
    }

    async fn get_vectors(
        &self,
        input: &GetVectorsInput,
    ) -> Result<GetVectorsOutput, SdkError<GetVectorsError>> {
        self.client
            .get_vectors()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_index_name(input.index_name.clone())
            .set_index_arn(input.index_arn.clone())
            .set_keys(input.keys.clone())
            .set_return_data(input.return_data)
            .set_return_metadata(input.return_metadata)
            .send()
            .await
    }

    async fn list_indexes(
        &self,
        input: &ListIndexesInput,
    ) -> Result<ListIndexesOutput, SdkError<ListIndexesError>> {
        self.client
            .list_indexes()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_vector_bucket_arn(input.vector_bucket_arn.clone())
            .set_max_results(input.max_results)
            .set_next_token(input.next_token.clone())
            .set_prefix(input.prefix.clone())
            .send()
            .await
    }

    async fn list_vector_buckets(
        &self,
        input: &ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, SdkError<ListVectorBucketsError>> {
        self.client
            .list_vector_buckets()
            .set_max_results(input.max_results)
            .set_next_token(input.next_token.clone())
            .set_prefix(input.prefix.clone())
            .send()
            .await
    }

    async fn list_vectors(
        &self,
        input: &ListVectorsInput,
    ) -> Result<ListVectorsOutput, SdkError<ListVectorsError>> {
        self.client
            .list_vectors()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_index_name(input.index_name.clone())
            .set_index_arn(input.index_arn.clone())
            .set_max_results(input.max_results)
            .set_next_token(input.next_token.clone())
            .set_segment_count(input.segment_count)
            .set_segment_index(input.segment_index)
            .set_return_data(input.return_data)
            .set_return_metadata(input.return_metadata)
            .send()
            .await
    }

    async fn put_vector_bucket_policy(
        &self,
        input: &PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, SdkError<PutVectorBucketPolicyError>> {
        self.client
            .put_vector_bucket_policy()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_vector_bucket_arn(input.vector_bucket_arn.clone())
            .set_policy(input.policy.clone())
            .send()
            .await
    }

    async fn put_vectors(
        &self,
        input: &PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError>> {
        self.client
            .put_vectors()
            .set_vector_bucket_name(input.vector_bucket_name.clone())
            .set_index_name(input.index_name.clone())
            .set_index_arn(input.index_arn.clone())
            .set_vectors(input.vectors.clone())
            .send()
            .await
    }

    async fn query_vectors(
        &self,
        input: &QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError>> {
        // An explicit `top_k == 0` means the caller asked for zero results
        // (e.g., SQL `LIMIT 0`). Short-circuit to an empty response instead of
        // clamping to 1, which would break result correctness.
        if input.top_k == Some(0) {
            return QueryVectorsOutput::builder()
                .set_vectors(Some(Vec::new()))
                .build()
                .map_err(SdkError::construction_failure);
        }

        let top_k = input
            .top_k
            .unwrap_or(QUERY_VECTORS_PAGE_SIZE)
            .clamp(1, QUERY_VECTORS_MAX_TOPK);

        if top_k <= QUERY_VECTORS_PAGE_SIZE {
            return self
                .client
                .query_vectors()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_index_name(input.index_name.clone())
                .set_index_arn(input.index_arn.clone())
                .set_query_vector(input.query_vector.clone())
                .top_k(top_k)
                .set_return_distance(input.return_distance)
                .set_return_metadata(input.return_metadata)
                .set_filter(input.filter.clone())
                .send()
                .await;
        }

        // Paginated query: collect results across multiple pages
        let mut all_vectors = Vec::new();
        let mut next_token: Option<String> = None;
        let mut distance_metric = None;

        loop {
            let captured = Arc::new(Mutex::new(None::<CapturedNextToken>));
            let interceptor = CaptureNextToken {
                captured: Arc::clone(&captured),
            };

            let mut operation = self
                .client
                .query_vectors()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_index_name(input.index_name.clone())
                .set_index_arn(input.index_arn.clone())
                .set_query_vector(input.query_vector.clone())
                .top_k(top_k)
                .set_return_distance(input.return_distance)
                .set_return_metadata(input.return_metadata)
                .set_filter(input.filter.clone())
                .customize()
                .interceptor(interceptor);

            if let Some(token) = next_token.clone() {
                operation = operation
                    .map_request(move |request| inject_query_vectors_next_token(request, &token));
            }

            let result = operation.send().await;

            let output = result?;

            distance_metric = distance_metric.or(output.distance_metric);
            let page_vectors = output.vectors.len();
            all_vectors.extend(output.vectors);

            let response_token = captured
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap_or(CapturedNextToken::Failed(
                    "S3 Vectors response was not inspected for nextToken",
                ));

            let top_k_usize = usize::try_from(top_k).unwrap_or(usize::MAX);
            match next_query_vectors_request_token(
                all_vectors.len(),
                page_vectors,
                top_k_usize,
                response_token,
                next_token.as_deref(),
            ) {
                Ok(Some(token)) => next_token = Some(token),
                Ok(None) => break,
                Err(reason) => {
                    return Err(query_vectors_pagination_error(
                        top_k,
                        all_vectors.len(),
                        reason,
                    ));
                }
            }
        }

        let top_k_usize = usize::try_from(top_k).unwrap_or(usize::MAX);
        all_vectors.truncate(top_k_usize);

        QueryVectorsOutput::builder()
            .set_vectors(Some(all_vectors))
            .set_distance_metric(distance_metric)
            .build()
            .map_err(SdkError::construction_failure)
    }
}

/// Trait representing the capabilities of the Amazon S3 Vectors API. Amazon S3 Vectors clients implement this trait.
#[async_trait]
pub trait S3Vectors {
    async fn create_index(
        &self,
        input: &CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError>>;

    async fn create_vector_bucket(
        &self,
        input: &CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError>>;

    async fn delete_index(
        &self,
        input: &DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, SdkError<DeleteIndexError>>;

    async fn delete_vector_bucket(
        &self,
        input: &DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, SdkError<DeleteVectorBucketError>>;

    async fn delete_vector_bucket_policy(
        &self,
        input: &DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, SdkError<DeleteVectorBucketPolicyError>>;

    async fn delete_vectors(
        &self,
        input: &DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, SdkError<DeleteVectorsError>>;

    async fn get_index(
        &self,
        input: &GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError>>;

    async fn get_vector_bucket(
        &self,
        input: &GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError>>;

    async fn get_vector_bucket_policy(
        &self,
        input: &GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, SdkError<GetVectorBucketPolicyError>>;

    async fn get_vectors(
        &self,
        input: &GetVectorsInput,
    ) -> Result<GetVectorsOutput, SdkError<GetVectorsError>>;

    async fn list_indexes(
        &self,
        input: &ListIndexesInput,
    ) -> Result<ListIndexesOutput, SdkError<ListIndexesError>>;

    async fn list_vector_buckets(
        &self,
        input: &ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, SdkError<ListVectorBucketsError>>;

    async fn list_vectors(
        &self,
        input: &ListVectorsInput,
    ) -> Result<ListVectorsOutput, SdkError<ListVectorsError>>;

    async fn put_vector_bucket_policy(
        &self,
        input: &PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, SdkError<PutVectorBucketPolicyError>>;

    async fn put_vectors(
        &self,
        input: &PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError>>;

    async fn query_vectors(
        &self,
        input: &QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError>>;
}

#[cfg(test)]
pub mod tests {
    use core::panic;

    use aws_config::{BehaviorVersion, Region};
    use aws_sdk_s3vectors::{
        error::SdkError,
        types::{PutInputVector, VectorData},
    };

    use super::*;

    #[test]
    fn test_query_vectors_page_size_constant() {
        assert_eq!(QUERY_VECTORS_PAGE_SIZE, 100);
    }

    #[test]
    fn test_query_vectors_max_topk_constant() {
        assert_eq!(QUERY_VECTORS_MAX_TOPK, 10_000);
    }

    #[test]
    fn test_page_size_divides_max_topk() {
        // Max topK should be a multiple of page size for clean pagination
        assert_eq!(
            QUERY_VECTORS_MAX_TOPK % QUERY_VECTORS_PAGE_SIZE,
            0,
            "QUERY_VECTORS_MAX_TOPK should be a multiple of QUERY_VECTORS_PAGE_SIZE"
        );
    }

    #[test]
    fn test_page_size_less_than_max_topk() {
        assert!(
            QUERY_VECTORS_PAGE_SIZE < QUERY_VECTORS_MAX_TOPK,
            "QUERY_VECTORS_PAGE_SIZE must be less than QUERY_VECTORS_MAX_TOPK"
        );
    }

    #[test]
    fn test_capture_next_token_with_token() {
        use aws_smithy_runtime_api::client::interceptors::Intercept;
        use aws_smithy_runtime_api::http::StatusCode;

        let captured = Arc::new(Mutex::new(None::<CapturedNextToken>));
        let interceptor = CaptureNextToken {
            captured: Arc::clone(&captured),
        };

        // Simulate a response body with nextToken
        let body_json = serde_json::json!({
            "vectors": [],
            "nextToken": "abc123"
        });
        let body_bytes = serde_json::to_vec(&body_json).expect("serialize");
        let response = aws_smithy_runtime_api::client::orchestrator::HttpResponse::new(
            StatusCode::try_from(200).expect("200"),
            aws_smithy_types::body::SdkBody::from(body_bytes),
        );

        // Build an interceptor context to test
        let mut context =
            aws_smithy_runtime_api::client::interceptors::context::InterceptorContext::new(
                aws_smithy_runtime_api::client::interceptors::context::Input::doesnt_matter(),
            );
        context.set_request(aws_smithy_runtime_api::client::orchestrator::HttpRequest::empty());
        context.set_response(response);

        let mut cfg = aws_smithy_types::config_bag::ConfigBag::base();
        let rc = aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder::for_tests()
            .build()
            .expect("build runtime components");

        let mut ctx_mut = aws_smithy_runtime_api::client::interceptors::context::BeforeDeserializationInterceptorContextMut::from(&mut context);
        interceptor
            .modify_before_deserialization(&mut ctx_mut, &rc, &mut cfg)
            .expect("interceptor should succeed for valid nextToken");

        let token = captured.lock().expect("lock").clone();
        assert_eq!(token, Some(CapturedNextToken::Token("abc123".to_string())));
    }

    #[test]
    fn test_capture_next_token_without_token() {
        use aws_smithy_runtime_api::client::interceptors::Intercept;
        use aws_smithy_runtime_api::http::StatusCode;

        let captured = Arc::new(Mutex::new(None::<CapturedNextToken>));
        let interceptor = CaptureNextToken {
            captured: Arc::clone(&captured),
        };

        // Response body without nextToken (last page)
        let body_json = serde_json::json!({
            "vectors": [{"key": "v1"}]
        });
        let body_bytes = serde_json::to_vec(&body_json).expect("serialize");
        let response = aws_smithy_runtime_api::client::orchestrator::HttpResponse::new(
            StatusCode::try_from(200).expect("200"),
            aws_smithy_types::body::SdkBody::from(body_bytes),
        );

        let mut context =
            aws_smithy_runtime_api::client::interceptors::context::InterceptorContext::new(
                aws_smithy_runtime_api::client::interceptors::context::Input::doesnt_matter(),
            );
        context.set_request(aws_smithy_runtime_api::client::orchestrator::HttpRequest::empty());
        context.set_response(response);

        let mut cfg = aws_smithy_types::config_bag::ConfigBag::base();
        let rc = aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder::for_tests()
            .build()
            .expect("build runtime components");

        let mut ctx_mut = aws_smithy_runtime_api::client::interceptors::context::BeforeDeserializationInterceptorContextMut::from(&mut context);
        interceptor
            .modify_before_deserialization(&mut ctx_mut, &rc, &mut cfg)
            .expect("interceptor should succeed without nextToken");

        let token = captured.lock().expect("lock").clone();
        assert_eq!(token, Some(CapturedNextToken::Missing));
    }

    #[test]
    fn test_capture_next_token_with_null_token() {
        use aws_smithy_runtime_api::client::interceptors::Intercept;
        use aws_smithy_runtime_api::http::StatusCode;

        let captured = Arc::new(Mutex::new(None::<CapturedNextToken>));
        let interceptor = CaptureNextToken {
            captured: Arc::clone(&captured),
        };

        // nextToken is present but null
        let body_json = serde_json::json!({
            "vectors": [],
            "nextToken": null
        });
        let body_bytes = serde_json::to_vec(&body_json).expect("serialize");
        let response = aws_smithy_runtime_api::client::orchestrator::HttpResponse::new(
            StatusCode::try_from(200).expect("200"),
            aws_smithy_types::body::SdkBody::from(body_bytes),
        );

        let mut context =
            aws_smithy_runtime_api::client::interceptors::context::InterceptorContext::new(
                aws_smithy_runtime_api::client::interceptors::context::Input::doesnt_matter(),
            );
        context.set_request(aws_smithy_runtime_api::client::orchestrator::HttpRequest::empty());
        context.set_response(response);

        let mut cfg = aws_smithy_types::config_bag::ConfigBag::base();
        let rc = aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder::for_tests()
            .build()
            .expect("build runtime components");

        let mut ctx_mut = aws_smithy_runtime_api::client::interceptors::context::BeforeDeserializationInterceptorContextMut::from(&mut context);
        interceptor
            .modify_before_deserialization(&mut ctx_mut, &rc, &mut cfg)
            .expect("interceptor should succeed with null nextToken");

        // null is not a string, so as_str() returns None
        let token = captured.lock().expect("lock").clone();
        assert_eq!(token, Some(CapturedNextToken::Missing));
    }

    #[test]
    fn test_capture_next_token_with_empty_body() {
        use aws_smithy_runtime_api::client::interceptors::Intercept;
        use aws_smithy_runtime_api::http::StatusCode;

        let captured = Arc::new(Mutex::new(None::<CapturedNextToken>));
        let interceptor = CaptureNextToken {
            captured: Arc::clone(&captured),
        };

        // Empty body — should not panic
        let response = aws_smithy_runtime_api::client::orchestrator::HttpResponse::new(
            StatusCode::try_from(200).expect("200"),
            aws_smithy_types::body::SdkBody::empty(),
        );

        let mut context =
            aws_smithy_runtime_api::client::interceptors::context::InterceptorContext::new(
                aws_smithy_runtime_api::client::interceptors::context::Input::doesnt_matter(),
            );
        context.set_request(aws_smithy_runtime_api::client::orchestrator::HttpRequest::empty());
        context.set_response(response);

        let mut cfg = aws_smithy_types::config_bag::ConfigBag::base();
        let rc = aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder::for_tests()
            .build()
            .expect("build runtime components");

        let mut ctx_mut = aws_smithy_runtime_api::client::interceptors::context::BeforeDeserializationInterceptorContextMut::from(&mut context);
        interceptor
            .modify_before_deserialization(&mut ctx_mut, &rc, &mut cfg)
            .expect("interceptor should succeed with empty body");

        let token = captured.lock().expect("lock").clone();
        assert_eq!(
            token,
            Some(CapturedNextToken::Failed(
                "S3 Vectors response body is not valid JSON while reading nextToken"
            ))
        );
    }

    #[test]
    fn test_capture_next_token_with_invalid_json() {
        use aws_smithy_runtime_api::client::interceptors::Intercept;
        use aws_smithy_runtime_api::http::StatusCode;

        let captured = Arc::new(Mutex::new(None::<CapturedNextToken>));
        let interceptor = CaptureNextToken {
            captured: Arc::clone(&captured),
        };

        // Invalid JSON body — should not panic, should leave captured as None
        let response = aws_smithy_runtime_api::client::orchestrator::HttpResponse::new(
            StatusCode::try_from(200).expect("200"),
            aws_smithy_types::body::SdkBody::from("not valid json"),
        );

        let mut context =
            aws_smithy_runtime_api::client::interceptors::context::InterceptorContext::new(
                aws_smithy_runtime_api::client::interceptors::context::Input::doesnt_matter(),
            );
        context.set_request(aws_smithy_runtime_api::client::orchestrator::HttpRequest::empty());
        context.set_response(response);

        let mut cfg = aws_smithy_types::config_bag::ConfigBag::base();
        let rc = aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder::for_tests()
            .build()
            .expect("build runtime components");

        let mut ctx_mut = aws_smithy_runtime_api::client::interceptors::context::BeforeDeserializationInterceptorContextMut::from(&mut context);
        interceptor
            .modify_before_deserialization(&mut ctx_mut, &rc, &mut cfg)
            .expect("interceptor should succeed with invalid JSON");

        let token = captured.lock().expect("lock").clone();
        assert_eq!(
            token,
            Some(CapturedNextToken::Failed(
                "S3 Vectors response body is not valid JSON while reading nextToken"
            ))
        );
    }

    #[test]
    fn test_capture_next_token_with_empty_string_token() {
        use aws_smithy_runtime_api::client::interceptors::Intercept;
        use aws_smithy_runtime_api::http::StatusCode;

        let captured = Arc::new(Mutex::new(None::<CapturedNextToken>));
        let interceptor = CaptureNextToken {
            captured: Arc::clone(&captured),
        };

        // nextToken is an empty string
        let body_json = serde_json::json!({
            "vectors": [],
            "nextToken": ""
        });
        let body_bytes = serde_json::to_vec(&body_json).expect("serialize");
        let response = aws_smithy_runtime_api::client::orchestrator::HttpResponse::new(
            StatusCode::try_from(200).expect("200"),
            aws_smithy_types::body::SdkBody::from(body_bytes),
        );

        let mut context =
            aws_smithy_runtime_api::client::interceptors::context::InterceptorContext::new(
                aws_smithy_runtime_api::client::interceptors::context::Input::doesnt_matter(),
            );
        context.set_request(aws_smithy_runtime_api::client::orchestrator::HttpRequest::empty());
        context.set_response(response);

        let mut cfg = aws_smithy_types::config_bag::ConfigBag::base();
        let rc = aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder::for_tests()
            .build()
            .expect("build runtime components");

        let mut ctx_mut = aws_smithy_runtime_api::client::interceptors::context::BeforeDeserializationInterceptorContextMut::from(&mut context);
        interceptor
            .modify_before_deserialization(&mut ctx_mut, &rc, &mut cfg)
            .expect("interceptor should succeed with empty string nextToken");

        // Empty string is still a valid string — should be captured as Some("")
        let token = captured.lock().expect("lock").clone();
        assert_eq!(token, Some(CapturedNextToken::Token(String::new())));
    }

    #[test]
    fn test_inject_query_vectors_next_token() {
        let body_json = serde_json::json!({
            "indexName": "test-index",
            "queryVector": { "float32": [1.0, 2.0] },
            "topK": 200,
            "vectorBucketName": "test-bucket"
        });
        let body_bytes = serde_json::to_vec(&body_json).expect("serialize");
        let request = HttpRequest::new(aws_smithy_types::body::SdkBody::from(body_bytes));

        let request = inject_query_vectors_next_token(request, "token-1")
            .expect("nextToken injection succeeds");
        let body = request.body().bytes().expect("body bytes");
        let json = serde_json::from_slice::<serde_json::Value>(body).expect("valid JSON body");
        let content_length = request
            .headers()
            .get("content-length")
            .expect("content-length header");
        let expected_content_length = body.len().to_string();

        assert_eq!(
            json.get("nextToken").and_then(serde_json::Value::as_str),
            Some("token-1")
        );
        assert_eq!(content_length, expected_content_length.as_str());
    }

    #[test]
    fn test_inject_query_vectors_next_token_rejects_invalid_json() {
        let request = HttpRequest::new(aws_smithy_types::body::SdkBody::from("not JSON"));
        let error = inject_query_vectors_next_token(request, "token-1")
            .expect_err("invalid JSON should fail token injection");

        assert!(
            error.to_string().contains("request body is not valid JSON"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_next_query_vectors_request_token_complete_at_requested_top_k() {
        let next =
            next_query_vectors_request_token(200, 100, 200, CapturedNextToken::Missing, None)
                .expect("complete pagination should succeed");

        assert_eq!(next, None);
    }

    #[test]
    fn test_next_query_vectors_request_token_continues_with_token() {
        let next = next_query_vectors_request_token(
            100,
            query_vectors_page_size_usize(),
            200,
            CapturedNextToken::Token("token-1".to_string()),
            None,
        )
        .expect("pagination should continue with token");

        assert_eq!(next, Some("token-1".to_string()));
    }

    #[test]
    fn test_next_query_vectors_request_token_allows_short_final_page_without_token() {
        let next = next_query_vectors_request_token(
            150,
            50,
            200,
            CapturedNextToken::Missing,
            Some("token-1"),
        )
        .expect("short final page without token should complete");

        assert_eq!(next, None);
    }

    #[test]
    fn test_next_query_vectors_request_token_allows_full_final_page_without_token() {
        let next = next_query_vectors_request_token(
            100,
            query_vectors_page_size_usize(),
            200,
            CapturedNextToken::Missing,
            None,
        )
        .expect("full final page without token should complete");

        assert_eq!(next, None);
    }

    #[test]
    fn test_next_query_vectors_request_token_errors_on_full_page_capture_failure() {
        let error = next_query_vectors_request_token(
            100,
            query_vectors_page_size_usize(),
            200,
            CapturedNextToken::Failed("capture failed"),
            None,
        )
        .expect_err("full page with failed token capture should fail safely");

        assert_eq!(error, "capture failed");
    }

    #[test]
    fn test_next_query_vectors_request_token_errors_on_empty_token_after_full_page() {
        let error = next_query_vectors_request_token(
            100,
            query_vectors_page_size_usize(),
            200,
            CapturedNextToken::Token(String::new()),
            None,
        )
        .expect_err("empty token after full page should fail safely");

        assert_eq!(
            error,
            "S3 Vectors returned a full page with an empty nextToken"
        );
    }

    #[test]
    fn test_next_query_vectors_request_token_errors_on_duplicate_token() {
        let error = next_query_vectors_request_token(
            200,
            query_vectors_page_size_usize(),
            300,
            CapturedNextToken::Token("token-1".to_string()),
            Some("token-1"),
        )
        .expect_err("duplicate token should fail safely");

        assert_eq!(error, "S3 Vectors returned a duplicate nextToken");
    }

    #[tokio::test]
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
