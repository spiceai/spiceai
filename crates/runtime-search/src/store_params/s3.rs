/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use std::str::FromStr;

use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// Distance metric for S3 Vectors similarity search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3DistanceMetric {
    Euclidean,
    Cosine,
}

impl S3DistanceMetric {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            S3DistanceMetric::Euclidean => "euclidean",
            S3DistanceMetric::Cosine => "cosine",
        }
    }
}

impl FromStr for S3DistanceMetric {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "euclidean" => Ok(S3DistanceMetric::Euclidean),
            "cosine" => Ok(S3DistanceMetric::Cosine),
            other => Err(format!("must be one of: euclidean, cosine. Found {other}")),
        }
    }
}

/// IAM role credential source for AWS authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsIamRoleSource {
    Auto,
    Metadata,
    Env,
}

impl FromStr for AwsIamRoleSource {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(AwsIamRoleSource::Auto),
            "metadata" => Ok(AwsIamRoleSource::Metadata),
            "env" => Ok(AwsIamRoleSource::Env),
            other => Err(format!(
                "must be one of: auto, metadata, env. Found {other}"
            )),
        }
    }
}

/// Typed parameters for the S3 Vectors engine, deserialized from
/// `vector_engine.params` after secret resolution.
///
/// Declares every accepted key — including the AWS auth keys — so it owns
/// validation and typo warnings for the whole component. Credential
/// *resolution* stays on the runtime's `Parameters`-based AWS auth path, so
/// the auth fields carry no `autoload_secret`: a credential present only in a
/// secret store is `None` here and supplied by the auth layer.
#[derive(Debug, TypedParams)]
#[params(prefix = "s3_vectors")]
pub struct S3VectorsParams {
    /// The S3 bucket name to use for the S3 Vectors index.
    pub bucket: Option<String>,
    /// The distance metric to be used for similarity search. One of: euclidean | cosine.
    pub distance_metric: Option<S3DistanceMetric>,
    /// The duration to wait prior to receiving the first response byte, in time unit format. E.g. 30s, 1m.
    #[param(runtime, parse_with = fundu::parse_duration)]
    pub client_timeout: Option<std::time::Duration>,
    /// The S3 Vectors bucket ARN to use for the S3 Vectors index.
    pub arn: Option<String>,
    /// The S3 Vectors index name to use within the bucket.
    pub index: Option<String>,
    /// The AWS region to use.
    pub aws_region: Option<String>,
    /// The AWS access key ID to use.
    pub aws_access_key_id: Option<SecretString>,
    /// The AWS secret access key to use.
    pub aws_secret_access_key: Option<SecretString>,
    /// The AWS session token to use.
    pub aws_session_token: Option<SecretString>,
    /// IAM role credential source. 'auto' uses the default AWS credential chain, 'metadata' uses only instance/container metadata (IMDS, ECS, EKS/IRSA), 'env' uses only environment variables.
    pub aws_iam_role_source: Option<AwsIamRoleSource>,
    /// Cache duration for listing S3 vector indexes (minimum: 5s). Defaults to list on every query.
    #[param(parse_with = fundu::parse_duration)]
    pub index_poll_interval: Option<std::time::Duration>,
    /// The number of rows to chunk record batches into for individual processing. Used to control memory usage during writes.
    #[param(default = "100000")]
    pub batch_write_rows: usize,
    /// If true, during periods where write throughput exceeds S3 vector rate limits, create and spill to a separate physical index. Incompatible with vector partitioning.
    pub spill_writes: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_parameters::typed::{ParamsError, TypedParams as _};
    use runtime_secrets::Secrets;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    async fn try_s3_params(values: &[(&str, &str)]) -> Result<S3VectorsParams, ParamsError> {
        S3VectorsParams::try_from_params(
            "AWS S3 Vectors store",
            values
                .iter()
                .map(|(key, value)| ((*key).to_string(), SecretString::from((*value).to_string())))
                .collect(),
            &Arc::new(RwLock::new(Secrets::default())),
        )
        .await
    }

    #[tokio::test]
    async fn typed_params_apply_defaults_and_parse_typed_fields() {
        let typed = try_s3_params(&[
            ("s3_vectors_bucket", "my-bucket"),
            ("s3_vectors_distance_metric", "cosine"),
            ("client_timeout", "30s"),
            ("s3_vectors_index_poll_interval", "1m"),
        ])
        .await
        .expect("S3 Vectors parameters should be valid");

        assert_eq!(typed.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(typed.distance_metric, Some(S3DistanceMetric::Cosine));
        assert_eq!(
            typed.client_timeout,
            Some(std::time::Duration::from_secs(30))
        );
        assert_eq!(
            typed.index_poll_interval,
            Some(std::time::Duration::from_mins(1))
        );
        assert_eq!(typed.batch_write_rows, 100_000);
        assert_eq!(typed.spill_writes, None);
    }

    #[tokio::test]
    async fn typed_params_reject_invalid_distance_metric() {
        let err = try_s3_params(&[("s3_vectors_distance_metric", "manhattan")])
            .await
            .expect_err("invalid distance metric should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 's3_vectors_distance_metric'"),
            "unexpected message: {err}"
        );
    }

    #[tokio::test]
    async fn typed_params_reject_malformed_batch_write_rows() {
        let err = try_s3_params(&[("s3_vectors_batch_write_rows", "lots")])
            .await
            .expect_err("malformed batch_write_rows should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 's3_vectors_batch_write_rows'"),
            "unexpected message: {err}"
        );
    }

    #[tokio::test]
    async fn typed_params_reject_invalid_iam_role_source() {
        let err = try_s3_params(&[("s3_vectors_aws_iam_role_source", "imds")])
            .await
            .expect_err("invalid aws_iam_role_source should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 's3_vectors_aws_iam_role_source'"),
            "unexpected message: {err}"
        );
    }
}
