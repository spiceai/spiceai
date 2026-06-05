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
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, ParameterSpec, Parameters,
    listing::{self, ListingTableConnector},
    parameters::{
        self, Validator,
        aws::{AuthValidator, RegionValidator, S3EndpointValidator},
    },
};

use crate::{
    Runtime, component::dataset::Dataset, dataconnector::listing::LISTING_TABLE_PARAMETERS,
};

use datafusion::parquet::arrow::async_reader::ObjectVersionType;
use snafu::prelude::*;
use std::any::Any;
use std::clone::Clone;
use std::future::Future;
use std::pin::Pin;
use std::string::String;
use std::sync::{Arc, LazyLock};
use url::Url;

static PREFIX: &str = "s3";

static VALIDATORS: LazyLock<
    Vec<Box<dyn Validator<Error = parameters::aws::Error> + Send + Sync + 'static>>,
> = LazyLock::new(|| {
    vec![
        Box::new(S3EndpointValidator),
        Box::new(RegionValidator),
        Box::new(AuthValidator),
    ]
});

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "S3 auth method 'key' requires an AWS access secret. Specify an access secret with the `s3_secret` parameter. For details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"
    ))]
    NoAccessSecret,

    #[snafu(display(
        "S3 auth method 'key' requires an AWS access key. Specify an access key with the `s3_key` parameter. For details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"
    ))]
    NoAccessKey,

    #[snafu(display(
        "Unsupported S3 auth method '{method}'. Use 'public', 'iam_role', or 'key' for `s3_auth` parameter. For details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"
    ))]
    UnsupportedAuthenticationMethod { method: String },

    #[snafu(display(
        "The '{parameter}' parameter requires `s3_auth` set to '{auth}'. For details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"
    ))]
    InvalidAuthParameterCombination { parameter: String, auth: String },

    #[snafu(display(
        "The `s3_endpoint` parameter must be a HTTP/S URL, but '{endpoint}' was provided. For details, visit: https://spiceai.org/docs/components/data-connectors/s3#params"
    ))]
    InvalidEndpoint { endpoint: String },

    #[snafu(display(
        "The `s3_region` parameter must be a valid AWS region code, but '{region}' was provided. For details, visit: https://spiceai.org/docs/components/data-connectors/s3#params"
    ))]
    InvalidRegion { region: String },

    #[snafu(display(
        "The `s3_region` parameter requires a lowercase AWS region code, but '{region}' was provided. Spice will automatically convert the region code to lowercase. For details, visit: https://spiceai.org/docs/components/data-connectors/s3#params"
    ))]
    InvalidRegionCorrected { region: String },

    #[snafu(display(
        "IAM role authentication failed. Are you sure you're running in an environment with an IAM role? {source} For details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"
    ))]
    InvalidIAMRoleAuthentication {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "The '{endpoint}' is a HTTP URL, but `allow_http` is not enabled. Set the parameter `allow_http: true` and retry. For details, visit: https://spiceai.org/docs/components/data-connectors/s3#params"
    ))]
    InsecureEndpointWithoutAllowHTTP { endpoint: String },
}

pub struct S3 {
    pub params: Parameters,
    pub runtime: Option<Runtime>,
    pub tokio_io_runtime: tokio::runtime::Handle,
}

impl std::fmt::Debug for S3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3(params: {:?})", self.params)
    }
}

#[derive(Default, Copy, Clone)]
pub struct S3Factory {}

impl S3Factory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

pub const S3_DOCS: &str = "https://spiceai.org/docs/components/data-connectors/s3";

pub static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = Vec::new();
    all_parameters.extend_from_slice(&[
            ParameterSpec::component("region")
                .description("AWS region for the S3 bucket (e.g. 'us-east-1').")
                .examples(&["us-east-1", "eu-west-1"])
                .help_link(S3_DOCS)
                .secret(),
            ParameterSpec::component("endpoint")
                .description("Custom S3-compatible endpoint URL. Leave empty for AWS S3.")
                .examples(&["https://s3.us-east-1.amazonaws.com", "http://minio.local:9000"])
                .help_link(S3_DOCS)
                .secret(),
            ParameterSpec::component("url_style")
                .description("Controls S3 URL addressing style. Supported values: 'vhost' and 'path'. When not set, auto-detected from the endpoint.")
                .one_of(&["vhost", "path"])
                .help_link(S3_DOCS),
            ParameterSpec::component("key")
                .description("AWS access key ID (used when auth='key').")
                .help_link(S3_DOCS)
                .secret(),
            ParameterSpec::component("secret")
                .description("AWS secret access key (used when auth='key').")
                .help_link(S3_DOCS)
                .secret(),
            ParameterSpec::component("session_token")
                .description("Temporary AWS session token (used with STS credentials).")
                .help_link(S3_DOCS)
                .secret(),
            ParameterSpec::component("auth")
                .description("Configures the authentication method for S3. Supported methods are: public (i.e. no auth), iam_role, key.")
                .one_of(&["public", "iam_role", "key"])
                .examples(&["iam_role", "key"])
                .help_link(S3_DOCS)
                .secret(),
            ParameterSpec::component("iam_role_source")
                .description("IAM role credential source (used when auth is 'iam_role' or unset, i.e. default IAM-based auth). 'auto' uses the default AWS credential chain, 'metadata' uses only instance/container metadata (IMDS, ECS, EKS/IRSA), 'env' uses only environment variables.")
                .one_of(&["auto", "metadata", "env"])
                .help_link(S3_DOCS),
            ParameterSpec::component("versioning")
                .description("Enables S3 object versioning support when set to 'enabled'. Defaults to 'enabled'.")
                .default("enabled")
                .help_link(S3_DOCS),
            ParameterSpec::runtime("client_timeout")
                .description("The timeout setting for S3 client.")
                .examples(&["30s"])
                .help_link(S3_DOCS),
            ParameterSpec::runtime("allow_http")
                .description("Allow HTTP protocol for S3 endpoint.")
                .is_boolean()
                .help_link(S3_DOCS),
        ]);
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

impl DataConnectorFactory for S3Factory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        if let Some(endpoint) = params.parameters.get("endpoint").expose().ok()
            && endpoint.ends_with('/')
        {
            tracing::warn!("Trimming trailing '/' from S3 endpoint {endpoint}");
            params.parameters.insert(
                "endpoint".to_string(),
                endpoint.trim_end_matches('/').to_string().into(),
            );
        }

        if let Some(versioning) = params.parameters.get("versioning").expose().ok()
            && !matches!(versioning, "enabled" | "disabled")
        {
            tracing::warn!(
                "Invalid S3 versioning setting '{versioning}'. Defaulting to 'enabled'."
            );
            params
                .parameters
                .insert("versioning".to_string(), "enabled".to_string().into());
        }

        Box::pin(async move {
            for validator in VALIDATORS.iter() {
                validator.validate(&mut params).await?;
            }

            // Initialize AWS SDK credentials for IAM role authentication.
            // Skip initialization only for 'public' and 'key' auth methods which use
            // explicit credentials. When auth is unset, attempt to load credentials from
            // the environment (including IRSA web identity tokens, ECS container credentials,
            // and IMDS) so that IAM-based access works by default.
            if let Some("public" | "key") = params.parameters.get("auth").expose().ok() {
                // Skip AWS SDK initialization - use explicit auth method directly
            } else {
                let iam_role_source = params.parameters.get("iam_role_source").expose().ok();
                if let Some("metadata" | "env") = iam_role_source {
                    // Restricted IAM role source - build a custom config instead
                    // of using the global SDK config. The object store registry
                    // will handle the restricted source when building credentials.
                } else {
                    let region = params
                        .parameters
                        .get("region")
                        .expose()
                        .ok()
                        .map(ToString::to_string);
                    // Always probe the default AWS credential chain so env-only setups
                    // (IRSA, EC2 IMDS, AWS_ACCESS_KEY_ID/AWS_REGION env vars, etc.) keep
                    // working when the user did not put `region` into spicepod params.
                    // The bridge logs internal probe failures at debug level, so we only
                    // surface a WARN here when the user explicitly configured a `region`
                    // (signalling AWS auth intent) but no credentials were resolved.
                    if let Err(err) = aws_sdk_credential_bridge::get_or_init_sdk_config_with_region(
                        region.as_deref(),
                    )
                    .await
                    {
                        tracing::warn!(
                            "Unable to initialize AWS credentials for S3 connector: {err}"
                        );
                    } else if region.is_some()
                        && aws_sdk_credential_bridge::get_sdk_config().is_none()
                    {
                        tracing::warn!(
                            "S3 connector configured with `region` but no AWS credentials were resolved from the environment; falling back to anonymous access. Set `auth: public` to silence this warning, or configure AWS credentials (env vars, ~/.aws/credentials, IAM role)."
                        );
                    }
                }
            }

            let s3 = S3 {
                params: params.parameters,
                runtime: params.runtime.map(Arc::unwrap_or_clone),
                tokio_io_runtime: params.io_runtime,
            };
            Ok(Arc::new(s3) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        PREFIX
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}

impl std::fmt::Display for S3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{PREFIX}")
    }
}

impl ListingTableConnector for S3 {
    fn object_versioning_type(&self) -> Option<ObjectVersionType> {
        if self.params.get("versioning").expose().ok() == Some("disabled") {
            return None;
        }

        Some(ObjectVersionType::Version)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_tokio_io_runtime(&self) -> tokio::runtime::Handle {
        self.tokio_io_runtime.clone()
    }

    fn get_object_store_url(
        &self,
        dataset: &Dataset,
        url: Option<&str>,
    ) -> DataConnectorResult<Url> {
        let url = url.unwrap_or(dataset.from.as_str());
        let mut s3_url =
            Url::parse(url)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("The specified URL is not valid: {url}. Ensure the URL is valid and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/{PREFIX}#from"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;

        s3_url.set_fragment(Some(&listing::build_fragments(
            &self.params,
            vec![
                "region",
                "endpoint",
                "url_style",
                "key",
                "secret",
                "client_timeout",
                "allow_http",
                "auth",
                "session_token",
                "iam_role_source",
            ],
        )));

        Ok(s3_url)
    }

    fn get_runtime(&self) -> Option<Runtime> {
        self.runtime.clone()
    }

    fn handle_object_store_error(
        &self,
        dataset: &Dataset,
        error: object_store::Error,
    ) -> DataConnectorError {
        match error {
            object_store::Error::Generic { source, .. } => {
                if self.params.get("auth").expose().ok() == Some("iam_role") {
                    let err = Error::InvalidIAMRoleAuthentication { source };

                    DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: format!("{err}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source: err.into(),
                    }
                } else {
                    DataConnectorError::UnableToConnectInternal {
                        dataconnector: format!("{self}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source,
                    }
                }
            }
            error => DataConnectorError::UnableToConnectInternal {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                source: error.into(),
            },
        }
    }
}

register_data_connector!("s3", S3Factory);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        builder::RuntimeBuilder,
        component::dataset::{Dataset, builder::DatasetBuilder},
    };
    use app::AppBuilder;
    use runtime_secrets::Secrets;
    use tokio::sync::RwLock;

    fn create_test_connector(params: Parameters) -> S3 {
        S3 {
            params,
            runtime: None,
            tokio_io_runtime: tokio::runtime::Handle::current(),
        }
    }

    async fn create_test_parameters(params: Vec<(String, secrecy::SecretString)>) -> Parameters {
        Parameters::try_new(
            "s3_test",
            params,
            PREFIX,
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS.as_ref(),
        )
        .await
        .expect("valid S3 test parameters")
    }

    async fn create_test_dataset(from: &str) -> Dataset {
        DatasetBuilder::try_new(from.to_string(), "test")
            .expect("dataset builder should be created")
            .with_app(Arc::new(AppBuilder::new("test").build()))
            .with_runtime(Arc::new(RuntimeBuilder::new().build().await))
            .build()
            .expect("dataset should be built")
    }

    #[tokio::test]
    async fn test_url_style_not_set_omits_fragment() {
        let params = create_test_parameters(vec![]).await;
        let connector = create_test_connector(params);
        let dataset = create_test_dataset("s3://spiceai-public-datasets/taxi_small_samples/").await;

        let object_store_url = connector
            .get_object_store_url(&dataset, None)
            .expect("object store URL should be constructed");

        // When url_style is not set, it should not appear in fragments (auto-detect at runtime)
        let fragment = object_store_url.fragment().unwrap_or("");
        assert!(
            !fragment.contains("url_style"),
            "url_style should not be in fragment when not set, got: {fragment}"
        );
    }

    #[tokio::test]
    async fn test_url_style_vhost_is_included_in_fragments() {
        let params = create_test_parameters(vec![(
            "s3_url_style".to_string(),
            "vhost".to_string().into(),
        )])
        .await;
        let connector = create_test_connector(params);
        let dataset = create_test_dataset("s3://spiceai-public-datasets/taxi_small_samples/").await;

        let object_store_url = connector
            .get_object_store_url(&dataset, None)
            .expect("object store URL should be constructed");

        assert_eq!(object_store_url.fragment(), Some("url_style=vhost"));
    }

    #[tokio::test]
    async fn test_url_style_path_is_included_in_fragments() {
        let params = create_test_parameters(vec![(
            "s3_url_style".to_string(),
            "path".to_string().into(),
        )])
        .await;
        let connector = create_test_connector(params);
        let dataset = create_test_dataset("s3://spiceai-public-datasets/taxi_small_samples/").await;

        let object_store_url = connector
            .get_object_store_url(&dataset, None)
            .expect("object store URL should be constructed");

        assert_eq!(object_store_url.fragment(), Some("url_style=path"));
    }

    #[tokio::test]
    async fn test_iam_role_source_included_in_fragments_when_set() {
        let params = create_test_parameters(vec![(
            "s3_iam_role_source".to_string(),
            "metadata".to_string().into(),
        )])
        .await;
        let connector = create_test_connector(params);
        let dataset = create_test_dataset("s3://spiceai-public-datasets/taxi_small_samples/").await;

        let object_store_url = connector
            .get_object_store_url(&dataset, None)
            .expect("object store URL should be constructed");

        assert_eq!(
            object_store_url.fragment(),
            Some("iam_role_source=metadata")
        );
    }

    #[tokio::test]
    async fn test_iam_role_source_omitted_from_fragments_when_unset() {
        let params = create_test_parameters(vec![]).await;
        let connector = create_test_connector(params);
        let dataset = create_test_dataset("s3://spiceai-public-datasets/taxi_small_samples/").await;

        let object_store_url = connector
            .get_object_store_url(&dataset, None)
            .expect("object store URL should be constructed");

        let fragment = object_store_url.fragment().unwrap_or("");
        assert!(
            !fragment.contains("iam_role_source"),
            "iam_role_source should not be in fragment when not set, got: {fragment}"
        );
    }
}
