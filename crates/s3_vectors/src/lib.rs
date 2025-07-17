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
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/rusoto/rusoto/master/assets/logo-square.png"
)]
//! <p>Amazon S3 Vectors</p>
//!
//! If you're using the service, you're probably looking for [`S3VectorsClient`](struct.S3VectorsClient.html) and [`S3Vectors`](trait.S3Vectors.html).
#![allow(clippy::unwrap_used)]

pub mod custom;
mod generated;
use async_trait::async_trait;
use aws_config::{BehaviorVersion, SdkConfig};
pub use generated::*;
pub use rusoto_core::RusotoError;

use aws_credential_types::provider::ProvideCredentials;
use rusoto_core::credential::{AwsCredentials, CredentialsError, ProvideAwsCredentials};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum CredentialError {
    #[snafu(display("Failed to get S3 credentials from the environment"))]
    FailedToGetCredentials,
}

#[derive(Debug)]
pub struct S3VectorsCredentialProvider {
    credentials: aws_credential_types::provider::SharedCredentialsProvider,
}

impl S3VectorsCredentialProvider {
    #[must_use]
    pub fn new(credentials: aws_credential_types::provider::SharedCredentialsProvider) -> Self {
        Self { credentials }
    }

    /// Loads credentials from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials cannot be loaded from the environment.
    pub async fn from_env() -> Result<(Self, SdkConfig), CredentialError> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

        let credentials = config
            .credentials_provider()
            .ok_or(CredentialError::FailedToGetCredentials)?;

        Ok((Self { credentials }, config))
    }

    /// Loads credentials from the provided AWS SDK configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials cannot be loaded from the configuration.
    pub fn from_config(config: &SdkConfig) -> Result<Self, CredentialError> {
        let credentials = config
            .credentials_provider()
            .ok_or(CredentialError::FailedToGetCredentials)?;

        Ok(Self { credentials })
    }
}

#[async_trait]
impl ProvideAwsCredentials for S3VectorsCredentialProvider {
    /// Produce a new `AwsCredentials` future.
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        let creds = self
            .credentials
            .provide_credentials()
            .await
            .map_err(|e| CredentialsError::new(e.to_string()))?;

        Ok(AwsCredentials::new(
            creds.access_key_id(),
            creds.secret_access_key(),
            creds.session_token().map(ToString::to_string),
            creds.expiry().map(Into::into),
        ))
    }
}

#[cfg(test)]
pub mod tests {
    use crate::{
        generated, CreateIndexInput, CreateVectorBucketInput, DeleteIndexInput,
        DeleteVectorBucketInput, ListVectorsInput, PutInputVector, PutVectorsInput,
        QueryVectorsInput, S3Vectors, VectorData,
    };

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    #[ignore]
    async fn test_s3_vectors() -> Result<(), String> {
        std::env::var("AWS_SECRET_ACCESS_KEY").expect("need AWS_SECRET_ACCESS_KEY");
        std::env::var("AWS_ACCESS_KEY_ID").expect("need AWS_ACCESS_KEY_ID");
        std::env::var("AWS_SESSION_TOKEN").expect("need AWS_SESSION_TOKEN");

        let client = generated::S3VectorsClient::new(rusoto_core::Region::UsWest2);

        client
            .delete_index(DeleteIndexInput {
                vector_bucket_name: Some("spice-s3-jeadie-vectors-2".into()),
                index_arn: None,
                index_name: Some("test".into()),
            })
            .await
            .expect("delete_index");
        client
            .delete_vector_bucket(DeleteVectorBucketInput {
                vector_bucket_name: Some("spice-s3-jeadie-vectors-2".into()),
                vector_bucket_arn: None,
            })
            .await
            .expect("delete_vector_bucket");
        let _output = client
            .create_vector_bucket(CreateVectorBucketInput {
                encryption_configuration: None,
                vector_bucket_name: "spice-s3-jeadie-vectors-2".into(),
            })
            .await
            .expect("create_vector_bucket");

        let _bucket = client
            .create_index(CreateIndexInput {
                data_type: "float32".into(),
                dimension: 3,
                distance_metric: "cosine".into(),
                index_name: "test".into(),
                metadata_configuration: None,
                vector_bucket_name: Some("spice-s3-jeadie-vectors-2".into()),
                vector_bucket_arn: None,
            })
            .await
            .expect("create_index");

        let _ = client
            .put_vectors(PutVectorsInput {
                index_name: Some("test".into()),
                index_arn: None,
                vector_bucket_name: Some("spice-s3-jeadie-vectors-2".into()),
                vectors: vec![
                    PutInputVector {
                        data: VectorData {
                            float_32: vec![1.0, 2.0, 3.0],
                        },
                        key: "v1".into(),
                        metadata: None,
                    },
                    PutInputVector {
                        data: VectorData {
                            float_32: vec![4.0, 5.0, 6.0],
                        },
                        key: "v2".into(),
                        metadata: None,
                    },
                    PutInputVector {
                        data: VectorData {
                            float_32: vec![7.0, 8.0, 9.0],
                        },
                        key: "v3".into(),
                        metadata: None,
                    },
                    PutInputVector {
                        data: VectorData {
                            float_32: vec![2.0, 2.0, 2.0],
                        },
                        key: "v4".into(),
                        metadata: None,
                    },
                ],
            })
            .await
            .expect("put_vectors");

        let response = client
            .query_vectors(QueryVectorsInput {
                filter: None,
                index_name: Some("test".into()),
                index_arn: None,
                vector_bucket_name: Some("spice-s3-jeadie-vectors-2".into()),
                query_vector: VectorData {
                    float_32: vec![4.0, 5.0, 3.0],
                },
                return_data: Some(true),
                return_distance: Some(true),
                return_metadata: Some(true),
                top_k: 2,
            })
            .await
            .expect("query_vectors");

        let v = serde_json::to_string(&response).expect("cant JSON");
        println!("query_vectors={v:?}");

        let response = client
            .list_vectors(ListVectorsInput {
                index_name: Some("test".into()),
                index_arn: None,
                vector_bucket_name: Some("spice-s3-jeadie-vectors-2".into()),
                return_data: Some(true),
                return_metadata: Some(true),
                max_results: Some(20),
                next_token: None,
                segment_count: None,
                segment_index: None,
            })
            .await
            .expect("lsit_vectors");

        let v = serde_json::to_string(&response).expect("cant JSON");
        println!("list_vectors={v:?}");

        Ok(())
    }
}
