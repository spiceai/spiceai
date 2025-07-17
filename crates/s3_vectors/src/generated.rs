// =================================================================
//
//                           * WARNING *
//
//                    This file is generated!
//
//  Changes made to this file will be overwritten. If changes are
//  required to the generated code, the service_crategen project
//  must be updated to generate the changes.
//
// =================================================================

use std::error::Error;
use std::fmt;
use std::str::FromStr;

use async_trait::async_trait;
use rusoto_core::credential::ProvideAwsCredentials;
use rusoto_core::request::{BufferedHttpResponse, DispatchSignedRequest};
use rusoto_core::{region, HttpClient};
use rusoto_core::{Client, RusotoError};

use rusoto_core::proto;
use rusoto_core::signature::SignedRequest;
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};

use crate::S3VectorsCredentialProvider;
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct CreateIndexInput {
    #[serde(rename = "dataType")]
    pub data_type: String,
    #[serde(rename = "dimension")]
    pub dimension: i64,
    #[serde(rename = "distanceMetric")]
    pub distance_metric: String,
    #[serde(rename = "indexName")]
    pub index_name: String,
    #[serde(rename = "metadataConfiguration")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_configuration: Option<MetadataConfiguration>,
    #[serde(rename = "vectorBucketArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct CreateIndexOutput {}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct CreateVectorBucketInput {
    #[serde(rename = "encryptionConfiguration")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_configuration: Option<EncryptionConfiguration>,
    #[serde(rename = "vectorBucketName")]
    pub vector_bucket_name: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct CreateVectorBucketOutput {}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct DeleteIndexInput {
    #[serde(rename = "indexArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<String>,
    #[serde(rename = "indexName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct DeleteIndexOutput {}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct DeleteVectorBucketInput {
    #[serde(rename = "vectorBucketArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct DeleteVectorBucketOutput {}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct DeleteVectorBucketPolicyInput {
    #[serde(rename = "vectorBucketArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct DeleteVectorBucketPolicyOutput {}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct DeleteVectorsInput {
    #[serde(rename = "indexArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<String>,
    #[serde(rename = "indexName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(rename = "keys")]
    pub keys: Vec<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct DeleteVectorsOutput {}

pub type Document = serde_json::Map<String, serde_json::Value>;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct EncryptionConfiguration {
    #[serde(rename = "kmsKeyArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kms_key_arn: Option<String>,
    #[serde(rename = "sseType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sse_type: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct GetIndexInput {
    #[serde(rename = "indexArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<String>,
    #[serde(rename = "indexName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct GetIndexOutput {
    #[serde(rename = "index")]
    pub index: Index,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct GetOutputVector {
    #[serde(rename = "data")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<VectorData>,
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "metadata")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<VectorMetadata>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct GetVectorBucketInput {
    #[serde(rename = "vectorBucketArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct GetVectorBucketOutput {
    #[serde(rename = "vectorBucket")]
    pub vector_bucket: VectorBucket,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct GetVectorBucketPolicyInput {
    #[serde(rename = "vectorBucketArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct GetVectorBucketPolicyOutput {
    #[serde(rename = "policy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct GetVectorsInput {
    #[serde(rename = "indexArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<String>,
    #[serde(rename = "indexName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(rename = "keys")]
    pub keys: Vec<String>,
    #[serde(rename = "returnData")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_data: Option<bool>,
    #[serde(rename = "returnMetadata")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_metadata: Option<bool>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct GetVectorsOutput {
    #[serde(rename = "vectors")]
    pub vectors: Vec<GetOutputVector>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct Index {
    #[serde(rename = "creationTime")]
    pub creation_time: f64,
    #[serde(rename = "dataType")]
    pub data_type: String,
    #[serde(rename = "dimension")]
    pub dimension: i64,
    #[serde(rename = "distanceMetric")]
    pub distance_metric: String,
    #[serde(rename = "indexArn")]
    pub index_arn: String,
    #[serde(rename = "indexName")]
    pub index_name: String,
    #[serde(rename = "metadataConfiguration")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_configuration: Option<MetadataConfiguration>,
    #[serde(rename = "vectorBucketName")]
    pub vector_bucket_name: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct IndexSummary {
    #[serde(rename = "creationTime")]
    pub creation_time: f64,
    #[serde(rename = "indexArn")]
    pub index_arn: String,
    #[serde(rename = "indexName")]
    pub index_name: String,
    #[serde(rename = "vectorBucketName")]
    pub vector_bucket_name: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct ListIndexesInput {
    #[serde(rename = "maxResults")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<i64>,
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    #[serde(rename = "prefix")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(rename = "vectorBucketArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct ListIndexesOutput {
    #[serde(rename = "indexes")]
    pub indexes: Vec<IndexSummary>,
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct ListOutputVector {
    #[serde(rename = "data")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<VectorData>,
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "metadata")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<VectorMetadata>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct ListVectorBucketsInput {
    #[serde(rename = "maxResults")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<i64>,
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    #[serde(rename = "prefix")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct ListVectorBucketsOutput {
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    #[serde(rename = "vectorBuckets")]
    pub vector_buckets: Vec<VectorBucketSummary>,
}

pub static LIST_VECTORS_MAX_RESULTS: usize = 500;
pub static PUT_VECTORS_MAX_ITEMS: usize = 500;

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct ListVectorsInput {
    #[serde(rename = "indexArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<String>,
    #[serde(rename = "indexName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(rename = "maxResults")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<i64>,
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    #[serde(rename = "returnData")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_data: Option<bool>,
    #[serde(rename = "returnMetadata")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_metadata: Option<bool>,
    #[serde(rename = "segmentCount")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_count: Option<i64>,
    #[serde(rename = "segmentIndex")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_index: Option<i64>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct ListVectorsOutput {
    #[serde(rename = "nextToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
    #[serde(rename = "vectors")]
    pub vectors: Vec<ListOutputVector>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct MetadataConfiguration {
    #[serde(rename = "nonFilterableMetadataKeys")]
    pub non_filterable_metadata_keys: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct PutInputVector {
    #[serde(rename = "data")]
    pub data: VectorData,
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "metadata")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<VectorMetadata>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct PutVectorBucketPolicyInput {
    #[serde(rename = "policy")]
    pub policy: String,
    #[serde(rename = "vectorBucketArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct PutVectorBucketPolicyOutput {}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct PutVectorsInput {
    #[serde(rename = "indexArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<String>,
    #[serde(rename = "indexName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
    #[serde(rename = "vectors")]
    pub vectors: Vec<PutInputVector>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct PutVectorsOutput {}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct QueryOutputVector {
    #[serde(rename = "data")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<VectorData>,
    #[serde(rename = "distance")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance: Option<f32>,
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "metadata")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<VectorMetadata>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[cfg_attr(feature = "deserialize_structs", derive(Deserialize))]
pub struct QueryVectorsInput {
    #[serde(rename = "filter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Document>,
    #[serde(rename = "indexArn")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<String>,
    #[serde(rename = "indexName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    #[serde(rename = "queryVector")]
    pub query_vector: VectorData,
    #[serde(rename = "returnData")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_data: Option<bool>,
    #[serde(rename = "returnDistance")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_distance: Option<bool>,
    #[serde(rename = "returnMetadata")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_metadata: Option<bool>,
    #[serde(rename = "topK")]
    pub top_k: i64,
    #[serde(rename = "vectorBucketName")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct QueryVectorsOutput {
    #[serde(rename = "vectors")]
    pub vectors: Vec<QueryOutputVector>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ValidationExceptionField {
    pub message: String,
    pub path: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct VectorBucket {
    #[serde(rename = "creationTime")]
    pub creation_time: f64,
    #[serde(rename = "encryptionConfiguration")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_configuration: Option<EncryptionConfiguration>,
    #[serde(rename = "sseType")]
    pub sse_type: Option<String>,
    #[serde(rename = "vectorBucketArn")]
    pub vector_bucket_arn: String,
    #[serde(rename = "vectorBucketName")]
    pub vector_bucket_name: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[cfg_attr(any(test, feature = "serialize_structs"), derive(Serialize))]
pub struct VectorBucketSummary {
    #[serde(rename = "creationTime")]
    pub creation_time: f64,
    #[serde(rename = "vectorBucketArn")]
    pub vector_bucket_arn: String,
    #[serde(rename = "vectorBucketName")]
    pub vector_bucket_name: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct VectorData {
    #[serde(rename = "float32")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub float_32: Option<Vec<f32>>,
}

// #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
// pub struct VectorMetadata {}
pub type VectorMetadata = serde_json::Map<String, serde_json::Value>;

/// Errors returned by `CreateIndex`
#[derive(Debug, PartialEq)]
pub enum CreateIndexError {
    AccessDenied(String),

    Conflict(String),

    InternalServer(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl CreateIndexError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<CreateIndexError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(CreateIndexError::AccessDenied(err.msg));
                }
                "ConflictException" => {
                    return RusotoError::Service(CreateIndexError::Conflict(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(CreateIndexError::InternalServer(err.msg));
                }
                "NotFoundException" => {
                    return RusotoError::Service(CreateIndexError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(CreateIndexError::ServiceQuotaExceeded(err.msg));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(CreateIndexError::ServiceUnavailable(err.msg));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(CreateIndexError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for CreateIndexError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CreateIndexError::AccessDenied(ref cause)
            | CreateIndexError::Conflict(ref cause)
            | CreateIndexError::InternalServer(ref cause)
            | CreateIndexError::NotFound(ref cause)
            | CreateIndexError::ServiceQuotaExceeded(ref cause)
            | CreateIndexError::TooManyRequests(ref cause)
            | CreateIndexError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for CreateIndexError {}
/// Errors returned by `CreateVectorBucket`
#[derive(Debug, PartialEq)]
pub enum CreateVectorBucketError {
    AccessDenied(String),

    Conflict(String),

    InternalServer(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl CreateVectorBucketError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<CreateVectorBucketError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(CreateVectorBucketError::AccessDenied(err.msg));
                }
                "ConflictException" => {
                    return RusotoError::Service(CreateVectorBucketError::Conflict(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(CreateVectorBucketError::InternalServer(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(CreateVectorBucketError::ServiceQuotaExceeded(
                        err.msg,
                    ));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(CreateVectorBucketError::ServiceUnavailable(
                        err.msg,
                    ));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(CreateVectorBucketError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for CreateVectorBucketError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CreateVectorBucketError::AccessDenied(ref cause)
            | CreateVectorBucketError::Conflict(ref cause)
            | CreateVectorBucketError::InternalServer(ref cause)
            | CreateVectorBucketError::ServiceQuotaExceeded(ref cause)
            | CreateVectorBucketError::TooManyRequests(ref cause)
            | CreateVectorBucketError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for CreateVectorBucketError {}
/// Errors returned by `DeleteIndex`
#[derive(Debug, PartialEq)]
pub enum DeleteIndexError {
    AccessDenied(String),

    InternalServer(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl DeleteIndexError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<DeleteIndexError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(DeleteIndexError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(DeleteIndexError::InternalServer(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(DeleteIndexError::ServiceQuotaExceeded(err.msg));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(DeleteIndexError::ServiceUnavailable(err.msg));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(DeleteIndexError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for DeleteIndexError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DeleteIndexError::AccessDenied(ref cause)
            | DeleteIndexError::InternalServer(ref cause)
            | DeleteIndexError::ServiceQuotaExceeded(ref cause)
            | DeleteIndexError::TooManyRequests(ref cause)
            | DeleteIndexError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for DeleteIndexError {}
/// Errors returned by `DeleteVectorBucket`
#[derive(Debug, PartialEq)]
pub enum DeleteVectorBucketError {
    AccessDenied(String),

    Conflict(String),

    InternalServer(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl DeleteVectorBucketError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<DeleteVectorBucketError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(DeleteVectorBucketError::AccessDenied(err.msg));
                }
                "ConflictException" => {
                    return RusotoError::Service(DeleteVectorBucketError::Conflict(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(DeleteVectorBucketError::InternalServer(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(DeleteVectorBucketError::ServiceQuotaExceeded(
                        err.msg,
                    ));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(DeleteVectorBucketError::ServiceUnavailable(
                        err.msg,
                    ));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(DeleteVectorBucketError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for DeleteVectorBucketError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DeleteVectorBucketError::AccessDenied(ref cause)
            | DeleteVectorBucketError::Conflict(ref cause)
            | DeleteVectorBucketError::InternalServer(ref cause)
            | DeleteVectorBucketError::ServiceQuotaExceeded(ref cause)
            | DeleteVectorBucketError::TooManyRequests(ref cause)
            | DeleteVectorBucketError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for DeleteVectorBucketError {}
/// Errors returned by `DeleteVectorBucketPolicy`
#[derive(Debug, PartialEq)]
pub enum DeleteVectorBucketPolicyError {
    AccessDenied(String),

    InternalServer(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl DeleteVectorBucketPolicyError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<DeleteVectorBucketPolicyError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(DeleteVectorBucketPolicyError::AccessDenied(
                        err.msg,
                    ));
                }
                "InternalServerException" => {
                    return RusotoError::Service(DeleteVectorBucketPolicyError::InternalServer(
                        err.msg,
                    ));
                }
                "NotFoundException" => {
                    return RusotoError::Service(DeleteVectorBucketPolicyError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(
                        DeleteVectorBucketPolicyError::ServiceQuotaExceeded(err.msg),
                    );
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(
                        DeleteVectorBucketPolicyError::ServiceUnavailable(err.msg),
                    );
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for DeleteVectorBucketPolicyError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DeleteVectorBucketPolicyError::AccessDenied(ref cause)
            | DeleteVectorBucketPolicyError::InternalServer(ref cause)
            | DeleteVectorBucketPolicyError::NotFound(ref cause)
            | DeleteVectorBucketPolicyError::ServiceQuotaExceeded(ref cause) => {
                write!(f, "{cause}")
            }
            DeleteVectorBucketPolicyError::TooManyRequests(ref cause)
            | DeleteVectorBucketPolicyError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for DeleteVectorBucketPolicyError {}
/// Errors returned by `DeleteVectors`
#[derive(Debug, PartialEq)]
pub enum DeleteVectorsError {
    AccessDenied(String),

    InternalServer(String),

    KmsDisabled(String),

    KmsInvalidKeyUsage(String),

    KmsInvalidState(String),

    KmsNotFound(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl DeleteVectorsError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<DeleteVectorsError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(DeleteVectorsError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(DeleteVectorsError::InternalServer(err.msg));
                }
                "KmsDisabledException" => {
                    return RusotoError::Service(DeleteVectorsError::KmsDisabled(err.msg));
                }
                "KmsInvalidKeyUsageException" => {
                    return RusotoError::Service(DeleteVectorsError::KmsInvalidKeyUsage(err.msg));
                }
                "KmsInvalidStateException" => {
                    return RusotoError::Service(DeleteVectorsError::KmsInvalidState(err.msg));
                }
                "KmsNotFoundException" => {
                    return RusotoError::Service(DeleteVectorsError::KmsNotFound(err.msg));
                }
                "NotFoundException" => {
                    return RusotoError::Service(DeleteVectorsError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(DeleteVectorsError::ServiceQuotaExceeded(err.msg));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(DeleteVectorsError::ServiceUnavailable(err.msg));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(DeleteVectorsError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for DeleteVectorsError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DeleteVectorsError::AccessDenied(ref cause)
            | DeleteVectorsError::InternalServer(ref cause)
            | DeleteVectorsError::KmsDisabled(ref cause)
            | DeleteVectorsError::KmsInvalidKeyUsage(ref cause)
            | DeleteVectorsError::KmsInvalidState(ref cause)
            | DeleteVectorsError::KmsNotFound(ref cause)
            | DeleteVectorsError::NotFound(ref cause)
            | DeleteVectorsError::ServiceQuotaExceeded(ref cause)
            | DeleteVectorsError::TooManyRequests(ref cause)
            | DeleteVectorsError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for DeleteVectorsError {}
/// Errors returned by `GetIndex`
#[derive(Debug, PartialEq)]
pub enum GetIndexError {
    AccessDenied(String),

    InternalServer(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl GetIndexError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<GetIndexError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(GetIndexError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(GetIndexError::InternalServer(err.msg));
                }
                "NotFoundException" => {
                    return RusotoError::Service(GetIndexError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(GetIndexError::ServiceQuotaExceeded(err.msg));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(GetIndexError::ServiceUnavailable(err.msg));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(GetIndexError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for GetIndexError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            GetIndexError::AccessDenied(ref cause)
            | GetIndexError::InternalServer(ref cause)
            | GetIndexError::NotFound(ref cause)
            | GetIndexError::ServiceQuotaExceeded(ref cause)
            | GetIndexError::TooManyRequests(ref cause)
            | GetIndexError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for GetIndexError {}
/// Errors returned by `GetVectorBucket`
#[derive(Debug, PartialEq)]
pub enum GetVectorBucketError {
    AccessDenied(String),

    InternalServer(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl GetVectorBucketError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<GetVectorBucketError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            tracing::debug!(
                "GetVectorBucketError from response error: type={}, msg={}",
                err.typ,
                err.msg
            );
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(GetVectorBucketError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(GetVectorBucketError::InternalServer(err.msg));
                }
                "NotFoundException" => {
                    return RusotoError::Service(GetVectorBucketError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(GetVectorBucketError::ServiceQuotaExceeded(
                        err.msg,
                    ));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(GetVectorBucketError::ServiceUnavailable(err.msg));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(GetVectorBucketError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for GetVectorBucketError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            GetVectorBucketError::AccessDenied(ref cause)
            | GetVectorBucketError::InternalServer(ref cause)
            | GetVectorBucketError::NotFound(ref cause)
            | GetVectorBucketError::ServiceQuotaExceeded(ref cause)
            | GetVectorBucketError::TooManyRequests(ref cause)
            | GetVectorBucketError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for GetVectorBucketError {}
/// Errors returned by `GetVectorBucketPolicy`
#[derive(Debug, PartialEq)]
pub enum GetVectorBucketPolicyError {
    AccessDenied(String),

    InternalServer(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl GetVectorBucketPolicyError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<GetVectorBucketPolicyError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(GetVectorBucketPolicyError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(GetVectorBucketPolicyError::InternalServer(
                        err.msg,
                    ));
                }
                "NotFoundException" => {
                    return RusotoError::Service(GetVectorBucketPolicyError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(GetVectorBucketPolicyError::ServiceQuotaExceeded(
                        err.msg,
                    ));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(GetVectorBucketPolicyError::ServiceUnavailable(
                        err.msg,
                    ));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for GetVectorBucketPolicyError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            GetVectorBucketPolicyError::AccessDenied(ref cause)
            | GetVectorBucketPolicyError::InternalServer(ref cause)
            | GetVectorBucketPolicyError::NotFound(ref cause)
            | GetVectorBucketPolicyError::ServiceQuotaExceeded(ref cause)
            | GetVectorBucketPolicyError::TooManyRequests(ref cause)
            | GetVectorBucketPolicyError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for GetVectorBucketPolicyError {}
/// Errors returned by `GetVectors`
#[derive(Debug, PartialEq)]
pub enum GetVectorsError {
    AccessDenied(String),

    InternalServer(String),

    KmsDisabled(String),

    KmsInvalidKeyUsage(String),

    KmsInvalidState(String),

    KmsNotFound(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl GetVectorsError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<GetVectorsError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(GetVectorsError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(GetVectorsError::InternalServer(err.msg));
                }
                "KmsDisabledException" => {
                    return RusotoError::Service(GetVectorsError::KmsDisabled(err.msg));
                }
                "KmsInvalidKeyUsageException" => {
                    return RusotoError::Service(GetVectorsError::KmsInvalidKeyUsage(err.msg));
                }
                "KmsInvalidStateException" => {
                    return RusotoError::Service(GetVectorsError::KmsInvalidState(err.msg));
                }
                "KmsNotFoundException" => {
                    return RusotoError::Service(GetVectorsError::KmsNotFound(err.msg));
                }
                "NotFoundException" => {
                    return RusotoError::Service(GetVectorsError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(GetVectorsError::ServiceQuotaExceeded(err.msg));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(GetVectorsError::ServiceUnavailable(err.msg));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(GetVectorsError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for GetVectorsError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            GetVectorsError::AccessDenied(ref cause)
            | GetVectorsError::InternalServer(ref cause)
            | GetVectorsError::KmsDisabled(ref cause)
            | GetVectorsError::KmsInvalidKeyUsage(ref cause)
            | GetVectorsError::KmsInvalidState(ref cause)
            | GetVectorsError::KmsNotFound(ref cause)
            | GetVectorsError::NotFound(ref cause)
            | GetVectorsError::ServiceQuotaExceeded(ref cause)
            | GetVectorsError::TooManyRequests(ref cause)
            | GetVectorsError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for GetVectorsError {}
/// Errors returned by `ListIndexes`
#[derive(Debug, PartialEq)]
pub enum ListIndexesError {
    AccessDenied(String),

    InternalServer(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl ListIndexesError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<ListIndexesError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(ListIndexesError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(ListIndexesError::InternalServer(err.msg));
                }
                "NotFoundException" => {
                    return RusotoError::Service(ListIndexesError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(ListIndexesError::ServiceQuotaExceeded(err.msg));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(ListIndexesError::ServiceUnavailable(err.msg));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(ListIndexesError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for ListIndexesError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ListIndexesError::AccessDenied(ref cause)
            | ListIndexesError::InternalServer(ref cause)
            | ListIndexesError::NotFound(ref cause)
            | ListIndexesError::ServiceQuotaExceeded(ref cause)
            | ListIndexesError::TooManyRequests(ref cause)
            | ListIndexesError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for ListIndexesError {}
/// Errors returned by `ListVectorBuckets`
#[derive(Debug, PartialEq)]
pub enum ListVectorBucketsError {
    AccessDenied(String),

    InternalServer(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl ListVectorBucketsError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<ListVectorBucketsError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(ListVectorBucketsError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(ListVectorBucketsError::InternalServer(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(ListVectorBucketsError::ServiceQuotaExceeded(
                        err.msg,
                    ));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(ListVectorBucketsError::ServiceUnavailable(
                        err.msg,
                    ));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for ListVectorBucketsError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ListVectorBucketsError::AccessDenied(ref cause)
            | ListVectorBucketsError::InternalServer(ref cause)
            | ListVectorBucketsError::ServiceQuotaExceeded(ref cause)
            | ListVectorBucketsError::TooManyRequests(ref cause)
            | ListVectorBucketsError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for ListVectorBucketsError {}
/// Errors returned by `ListVectors`
#[derive(Debug, PartialEq)]
pub enum ListVectorsError {
    AccessDenied(String),

    InternalServer(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl ListVectorsError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<ListVectorsError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(ListVectorsError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(ListVectorsError::InternalServer(err.msg));
                }
                "NotFoundException" => {
                    return RusotoError::Service(ListVectorsError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(ListVectorsError::ServiceQuotaExceeded(err.msg));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(ListVectorsError::ServiceUnavailable(err.msg));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(ListVectorsError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for ListVectorsError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ListVectorsError::AccessDenied(ref cause)
            | ListVectorsError::InternalServer(ref cause)
            | ListVectorsError::NotFound(ref cause)
            | ListVectorsError::ServiceQuotaExceeded(ref cause)
            | ListVectorsError::TooManyRequests(ref cause)
            | ListVectorsError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for ListVectorsError {}
/// Errors returned by `PutVectorBucketPolicy`
#[derive(Debug, PartialEq)]
pub enum PutVectorBucketPolicyError {
    AccessDenied(String),

    InternalServer(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl PutVectorBucketPolicyError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<PutVectorBucketPolicyError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(PutVectorBucketPolicyError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(PutVectorBucketPolicyError::InternalServer(
                        err.msg,
                    ));
                }
                "NotFoundException" => {
                    return RusotoError::Service(PutVectorBucketPolicyError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(PutVectorBucketPolicyError::ServiceQuotaExceeded(
                        err.msg,
                    ));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(PutVectorBucketPolicyError::ServiceUnavailable(
                        err.msg,
                    ));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for PutVectorBucketPolicyError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PutVectorBucketPolicyError::AccessDenied(ref cause)
            | PutVectorBucketPolicyError::InternalServer(ref cause)
            | PutVectorBucketPolicyError::NotFound(ref cause)
            | PutVectorBucketPolicyError::ServiceQuotaExceeded(ref cause)
            | PutVectorBucketPolicyError::TooManyRequests(ref cause)
            | PutVectorBucketPolicyError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for PutVectorBucketPolicyError {}
/// Errors returned by `PutVectors`
#[derive(Debug, PartialEq)]
pub enum PutVectorsError {
    AccessDenied(String),

    InternalServer(String),

    KmsDisabled(String),

    KmsInvalidKeyUsage(String),

    KmsInvalidState(String),

    KmsNotFound(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl PutVectorsError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<PutVectorsError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(PutVectorsError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(PutVectorsError::InternalServer(err.msg));
                }
                "KmsDisabledException" => {
                    return RusotoError::Service(PutVectorsError::KmsDisabled(err.msg));
                }
                "KmsInvalidKeyUsageException" => {
                    return RusotoError::Service(PutVectorsError::KmsInvalidKeyUsage(err.msg));
                }
                "KmsInvalidStateException" => {
                    return RusotoError::Service(PutVectorsError::KmsInvalidState(err.msg));
                }
                "KmsNotFoundException" => {
                    return RusotoError::Service(PutVectorsError::KmsNotFound(err.msg));
                }
                "NotFoundException" => {
                    return RusotoError::Service(PutVectorsError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(PutVectorsError::ServiceQuotaExceeded(err.msg));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(PutVectorsError::ServiceUnavailable(err.msg));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(PutVectorsError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for PutVectorsError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PutVectorsError::AccessDenied(ref cause)
            | PutVectorsError::InternalServer(ref cause)
            | PutVectorsError::KmsDisabled(ref cause)
            | PutVectorsError::KmsInvalidKeyUsage(ref cause)
            | PutVectorsError::KmsInvalidState(ref cause)
            | PutVectorsError::KmsNotFound(ref cause)
            | PutVectorsError::NotFound(ref cause)
            | PutVectorsError::ServiceQuotaExceeded(ref cause)
            | PutVectorsError::TooManyRequests(ref cause)
            | PutVectorsError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for PutVectorsError {}
/// Errors returned by `QueryVectors`
#[derive(Debug, PartialEq)]
pub enum QueryVectorsError {
    AccessDenied(String),

    InternalServer(String),

    KmsDisabled(String),

    KmsInvalidKeyUsage(String),

    KmsInvalidState(String),

    KmsNotFound(String),

    NotFound(String),

    ServiceQuotaExceeded(String),

    ServiceUnavailable(String),

    TooManyRequests(String),
}

impl QueryVectorsError {
    pub fn from_response(res: BufferedHttpResponse) -> RusotoError<QueryVectorsError> {
        if let Some(err) = proto::json::Error::parse_rest(&res) {
            match err.typ.as_str() {
                "AccessDeniedException" => {
                    return RusotoError::Service(QueryVectorsError::AccessDenied(err.msg));
                }
                "InternalServerException" => {
                    return RusotoError::Service(QueryVectorsError::InternalServer(err.msg));
                }
                "KmsDisabledException" => {
                    return RusotoError::Service(QueryVectorsError::KmsDisabled(err.msg));
                }
                "KmsInvalidKeyUsageException" => {
                    return RusotoError::Service(QueryVectorsError::KmsInvalidKeyUsage(err.msg));
                }
                "KmsInvalidStateException" => {
                    return RusotoError::Service(QueryVectorsError::KmsInvalidState(err.msg));
                }
                "KmsNotFoundException" => {
                    return RusotoError::Service(QueryVectorsError::KmsNotFound(err.msg));
                }
                "NotFoundException" => {
                    return RusotoError::Service(QueryVectorsError::NotFound(err.msg));
                }
                "ServiceQuotaExceededException" => {
                    return RusotoError::Service(QueryVectorsError::ServiceQuotaExceeded(err.msg));
                }
                "ServiceUnavailableException" => {
                    return RusotoError::Service(QueryVectorsError::ServiceUnavailable(err.msg));
                }
                "TooManyRequestsException" => {
                    return RusotoError::Service(QueryVectorsError::TooManyRequests(err.msg));
                }
                "ValidationException" => return RusotoError::Validation(err.msg),
                _ => {}
            }
        }
        RusotoError::Unknown(res)
    }
}
impl fmt::Display for QueryVectorsError {
    #[allow(unused_variables)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            QueryVectorsError::AccessDenied(ref cause)
            | QueryVectorsError::InternalServer(ref cause)
            | QueryVectorsError::KmsDisabled(ref cause)
            | QueryVectorsError::KmsInvalidKeyUsage(ref cause)
            | QueryVectorsError::KmsInvalidState(ref cause)
            | QueryVectorsError::KmsNotFound(ref cause)
            | QueryVectorsError::NotFound(ref cause)
            | QueryVectorsError::ServiceQuotaExceeded(ref cause)
            | QueryVectorsError::TooManyRequests(ref cause)
            | QueryVectorsError::ServiceUnavailable(ref cause) => write!(f, "{cause}"),
        }
    }
}
impl Error for QueryVectorsError {}
/// Trait representing the capabilities of the Amazon S3 Vectors API. Amazon S3 Vectors clients implement this trait.
#[async_trait]
pub trait S3Vectors {
    async fn create_index(
        &self,
        input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, RusotoError<CreateIndexError>>;

    async fn create_vector_bucket(
        &self,
        input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, RusotoError<CreateVectorBucketError>>;

    async fn delete_index(
        &self,
        input: DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, RusotoError<DeleteIndexError>>;

    async fn delete_vector_bucket(
        &self,
        input: DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, RusotoError<DeleteVectorBucketError>>;

    async fn delete_vector_bucket_policy(
        &self,
        input: DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, RusotoError<DeleteVectorBucketPolicyError>>;

    async fn delete_vectors(
        &self,
        input: DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, RusotoError<DeleteVectorsError>>;

    async fn get_index(
        &self,
        input: GetIndexInput,
    ) -> Result<GetIndexOutput, RusotoError<GetIndexError>>;

    async fn get_vector_bucket(
        &self,
        input: GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, RusotoError<GetVectorBucketError>>;

    async fn get_vector_bucket_policy(
        &self,
        input: GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, RusotoError<GetVectorBucketPolicyError>>;

    async fn get_vectors(
        &self,
        input: GetVectorsInput,
    ) -> Result<GetVectorsOutput, RusotoError<GetVectorsError>>;

    async fn list_indexes(
        &self,
        input: ListIndexesInput,
    ) -> Result<ListIndexesOutput, RusotoError<ListIndexesError>>;

    async fn list_vector_buckets(
        &self,
        input: ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, RusotoError<ListVectorBucketsError>>;

    async fn list_vectors(
        &self,
        input: ListVectorsInput,
    ) -> Result<ListVectorsOutput, RusotoError<ListVectorsError>>;

    async fn put_vector_bucket_policy(
        &self,
        input: PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, RusotoError<PutVectorBucketPolicyError>>;

    async fn put_vectors(
        &self,
        input: PutVectorsInput,
    ) -> Result<PutVectorsOutput, RusotoError<PutVectorsError>>;

    async fn query_vectors(
        &self,
        input: QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, RusotoError<QueryVectorsError>>;
}
/// A client for the Amazon S3 Vectors API.
#[derive(Clone)]
pub struct S3VectorsClient {
    client: Client,
    region: region::Region,
    endpoint: Option<String>,
}

impl S3VectorsClient {
    /// Generates the default S3 Vectors endpoint URL for the given region.
    fn generate_s3vectors_endpoint(region: &str) -> String {
        format!("s3vectors.{region}.api.aws")
    }
    /// Creates a client backed by the default tokio event loop.
    ///
    /// The client will use the default credentials provider and tls client.
    #[must_use]
    pub fn new(region: region::Region) -> S3VectorsClient {
        let endpoint = Some(Self::generate_s3vectors_endpoint(region.name()));
        S3VectorsClient {
            client: Client::shared(),
            region,
            endpoint,
        }
    }

    #[must_use]
    pub fn try_new(
        region: &str,
        credentials_provider: S3VectorsCredentialProvider,
    ) -> Option<S3VectorsClient> {
        let dispatcher = HttpClient::new().ok()?;
        Some(S3VectorsClient {
            client: Client::new_with(credentials_provider, dispatcher),
            region: region::Region::from_str(region).ok()?,
            endpoint: Some(Self::generate_s3vectors_endpoint(region)),
        })
    }

    #[must_use]
    pub fn try_new_with_endpoint(
        region: &str,
        credentials_provider: S3VectorsCredentialProvider,
        endpoint: &str,
    ) -> Option<S3VectorsClient> {
        let dispatcher = HttpClient::new().ok()?;
        Some(S3VectorsClient {
            client: Client::new_with(credentials_provider, dispatcher),
            region: region::Region::from_str(region).ok()?,
            endpoint: Some(endpoint.to_string()),
        })
    }

    pub fn new_with<P, D>(
        request_dispatcher: D,
        credentials_provider: P,
        region: region::Region,
    ) -> S3VectorsClient
    where
        P: ProvideAwsCredentials + Send + Sync + 'static,
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        let endpoint = Some(Self::generate_s3vectors_endpoint(region.name()));
        S3VectorsClient {
            client: Client::new_with(credentials_provider, request_dispatcher),
            region,
            endpoint,
        }
    }

    #[must_use]
    pub fn new_with_client(client: Client, region: region::Region) -> S3VectorsClient {
        let endpoint = Some(Self::generate_s3vectors_endpoint(region.name()));
        S3VectorsClient {
            client,
            region,
            endpoint,
        }
    }
}

#[async_trait]
impl S3Vectors for S3VectorsClient {
    async fn create_index(
        &self,
        input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, RusotoError<CreateIndexError>> {
        tracing::info!(
            "S3 Vectors creating index {}: bucket={:?}, distance_metric={}, dimensions={}",
            input.index_name,
            input.vector_bucket_name,
            input.distance_metric,
            input.dimension
        );
        let start = std::time::Instant::now();

        let request_uri = "/CreateIndex";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /CreateIndex request: {:?}", request);

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .inspect_err(|e| {
                tracing::debug!("Failed to dispatch /CreateIndex request: {e:?}");
            })
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<CreateIndexOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            tracing::info!(
                "S3 Vectors index {} created successfully in {:?}",
                input.index_name,
                start.elapsed()
            );

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /CreateIndex: status={}, response={:?}",
                response.status,
                response
            );
            Err(CreateIndexError::from_response(response))
        }
    }

    async fn create_vector_bucket(
        &self,
        input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, RusotoError<CreateVectorBucketError>> {
        tracing::info!(
            "S3 Vectors creating vector bucket: {}",
            input.vector_bucket_name
        );
        let start = std::time::Instant::now();

        let request_uri = "/CreateVectorBucket";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /CreateVectorBucket request: {:?}", request);

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .inspect_err(|e| {
                tracing::debug!("Failed to dispatch /CreateVectorBucket request: {e:?}");
            })
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<CreateVectorBucketOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            tracing::info!(
                "S3 Vectors vector bucket {} created successfully in {:?}",
                input.vector_bucket_name,
                start.elapsed()
            );

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /CreateVectorBucket: status={}, response={:?}",
                response.status,
                response
            );
            Err(CreateVectorBucketError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn delete_index(
        &self,
        input: DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, RusotoError<DeleteIndexError>> {
        let request_uri = "/DeleteIndex";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /DeleteIndex request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<DeleteIndexOutput, _>()?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            Err(DeleteIndexError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn delete_vector_bucket(
        &self,
        input: DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, RusotoError<DeleteVectorBucketError>> {
        let request_uri = "/DeleteVectorBucket";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /DeleteVectorBucket request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<DeleteVectorBucketOutput, _>()?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            Err(DeleteVectorBucketError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn delete_vector_bucket_policy(
        &self,
        input: DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, RusotoError<DeleteVectorBucketPolicyError>> {
        let request_uri = "/DeleteVectorBucketPolicy";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /GetVectorBucketPolicy request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<DeleteVectorBucketPolicyOutput, _>()?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            Err(DeleteVectorBucketPolicyError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn delete_vectors(
        &self,
        input: DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, RusotoError<DeleteVectorsError>> {
        let request_uri = "/DeleteVectors";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /DeleteVectors request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<DeleteVectorsOutput, _>()?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            Err(DeleteVectorsError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn get_index(
        &self,
        input: GetIndexInput,
    ) -> Result<GetIndexOutput, RusotoError<GetIndexError>> {
        let request_uri = "/GetIndex";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /GetIndex request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .inspect_err(|e| {
                tracing::debug!("Failed to dispatch /GetIndex request: {e:?}");
            })
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<GetIndexOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /GetIndex: status={}, response={:?}",
                response.status,
                response
            );
            Err(GetIndexError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn get_vector_bucket(
        &self,
        input: GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, RusotoError<GetVectorBucketError>> {
        let request_uri = "/GetVectorBucket";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /GetVectorBucket request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .inspect_err(|e| {
                tracing::debug!("Failed to dispatch /GetVectorBucket request: {e:?}");
            })
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<GetVectorBucketOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /GetVectorBucket: status={}, response={:?}",
                response.status,
                response
            );
            Err(GetVectorBucketError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn get_vector_bucket_policy(
        &self,
        input: GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, RusotoError<GetVectorBucketPolicyError>> {
        let request_uri = "/GetVectorBucketPolicy";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set configurable hostname with default
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /GetVectorBucketPolicy request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!("S3Vectors /GetVectorBucketPolicy response: {:?}", response);
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<GetVectorBucketPolicyOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /GetVectorBucketPolicy: status={}, response={:?}",
                response.status,
                response
            );
            Err(GetVectorBucketPolicyError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn get_vectors(
        &self,
        input: GetVectorsInput,
    ) -> Result<GetVectorsOutput, RusotoError<GetVectorsError>> {
        let request_uri = "/GetVectors";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /GetVectors request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<GetVectorsOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /GetVectors: status={}, response={:?}",
                response.status,
                response
            );
            Err(GetVectorsError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn list_indexes(
        &self,
        input: ListIndexesInput,
    ) -> Result<ListIndexesOutput, RusotoError<ListIndexesError>> {
        let request_uri = "/ListIndexes";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /ListIndexes request: {:?}", request);

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .inspect_err(|e| {
                tracing::debug!("Failed to dispatch /ListIndexes request: {e:?}");
            })
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<ListIndexesOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /ListIndexes: status={}, response={:?}",
                response.status,
                response
            );
            Err(ListIndexesError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn list_vector_buckets(
        &self,
        input: ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, RusotoError<ListVectorBucketsError>> {
        let request_uri = "/ListVectorBuckets";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /ListVectorBuckets request: {:?}", request);

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .inspect_err(|e| {
                tracing::debug!("Failed to dispatch /ListVectorBuckets request: {e:?}");
            })
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<ListVectorBucketsOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /ListVectorBuckets: status={}, response={:?}",
                response.status,
                response
            );
            Err(ListVectorBucketsError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn list_vectors(
        &self,
        input: ListVectorsInput,
    ) -> Result<ListVectorsOutput, RusotoError<ListVectorsError>> {
        let request_uri = "/ListVectors";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /ListVectors request: {:?}", request);

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .inspect_err(|e| {
                tracing::debug!("Failed to dispatch /ListVectors request: {e:?}");
            })
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<ListVectorsOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /ListVectors: status={}, response={:?}",
                response.status,
                response
            );
            Err(ListVectorsError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn put_vector_bucket_policy(
        &self,
        input: PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, RusotoError<PutVectorBucketPolicyError>> {
        let request_uri = "/PutVectorBucketPolicy";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /PutVectorBucketPolicy request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<PutVectorBucketPolicyOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /PutVectorBucketPolicy: status={}, response={:?}",
                response.status,
                response
            );
            Err(PutVectorBucketPolicyError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn put_vectors(
        &self,
        input: PutVectorsInput,
    ) -> Result<PutVectorsOutput, RusotoError<PutVectorsError>> {
        let request_uri = "/PutVectors";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /PutVectors request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .inspect_err(|e| {
                tracing::debug!("Failed to dispatch /PutVectors request: {e:?}");
            })
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<PutVectorsOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /PutVectors: status={}, response={:?}",
                response.status,
                response
            );
            Err(PutVectorsError::from_response(response))
        }
    }

    #[allow(unused_mut)]
    async fn query_vectors(
        &self,
        input: QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, RusotoError<QueryVectorsError>> {
        let request_uri = "/QueryVectors";

        let mut request = SignedRequest::new("POST", "s3vectors", &self.region, request_uri);
        request.set_content_type("application/json".to_owned());

        // Set custom endpoint if provided
        if let Some(ref endpoint) = self.endpoint {
            request.set_hostname(Some(endpoint.clone()));
        }

        let encoded = Some(serde_json::to_vec(&input).unwrap());
        request.set_payload(encoded);

        tracing::trace!("S3Vectors /QueryVectors request: {request:?}");

        let mut response = self
            .client
            .sign_and_dispatch(request)
            .await
            .inspect_err(|e| {
                tracing::debug!("Failed to dispatch /QueryVectors request: {e:?}");
            })
            .map_err(RusotoError::from)?;
        if response.status.as_u16() == 200 {
            let mut response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            let result = proto::json::ResponsePayload::new(&response)
                .deserialize::<QueryVectorsOutput, _>()
                .inspect_err(|_| {
                    tracing::debug!("Failed to deserialize response: {response:?}");
                })?;

            Ok(result)
        } else {
            let response = response.buffer().await.map_err(RusotoError::HttpDispatch)?;
            tracing::debug!(
                "Error response received for /QueryVectors: status={}, response={:?}",
                response.status,
                response
            );
            Err(QueryVectorsError::from_response(response))
        }
    }
}
