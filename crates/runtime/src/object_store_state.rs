/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use aws_sdk_credential_bridge::object_store_builder::S3ObjectStoreBuilder;
use object_store::ObjectStore;
use runtime_object_store::build_azure_object_store;
use runtime_object_store::registry::SpiceObjectStoreRegistry;
use runtime_parameters::{ParameterSpec, Parameters};
use runtime_secrets::{Secrets, get_params_with_secrets};
use secrecy::ExposeSecret;
use snafu::prelude::*;
use spicepod::param::Params;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use url::Url;

static S3_PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    vec![
        ParameterSpec::component("region").secret(),
        ParameterSpec::component("endpoint").secret(),
        ParameterSpec::component("key").secret(),
        ParameterSpec::component("secret").secret(),
        ParameterSpec::component("session_token").secret(),
        ParameterSpec::component("auth")
            .description("Configures the authentication method for S3. Supported methods are: iam_role, key.")
            .default("iam_role")
            .one_of(&["iam_role", "key"])
            .secret(),
        ParameterSpec::runtime("client_timeout").description("The timeout setting for S3 client."),
        ParameterSpec::runtime("allow_http").description("Allow HTTP protocol for S3 endpoint."),
    ]
});

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("Failed to parse {usage} location {location}: {source}"))]
    InvalidStateLocation {
        usage: &'static str,
        location: String,
        source: url::ParseError,
    },

    #[snafu(display("Failed to initialize {usage} object store for {location}: {source}"))]
    ObjectStoreInit {
        usage: &'static str,
        location: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to build S3 object store for {usage} at {location}: {source}"))]
    S3ObjectStoreInit {
        usage: &'static str,
        location: String,
        source: aws_sdk_credential_bridge::object_store_builder::S3ObjectStoreBuilderError,
    },

    #[snafu(display("Failed to validate S3 parameters for {usage}: {source}"))]
    S3ParameterValidation {
        usage: &'static str,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to parse {usage} file location {location}: URL is not a local file path"
    ))]
    InvalidFileLocation {
        usage: &'static str,
        location: String,
    },

    #[snafu(display("Failed to initialize local filesystem for {usage} at {location}: {source}"))]
    LocalFileSystemInit {
        usage: &'static str,
        location: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn build_object_store(
    secrets: Arc<RwLock<Secrets>>,
    io_runtime: Handle,
    state_location: &str,
    params: Option<&Params>,
    usage: &'static str,
) -> Result<(Arc<dyn ObjectStore>, String)> {
    let url = Url::parse(state_location).context(InvalidStateLocationSnafu {
        usage,
        location: state_location,
    })?;

    if url.scheme() == "file" {
        let local_path = url
            .to_file_path()
            .map_err(|()| Error::InvalidFileLocation {
                usage,
                location: state_location.to_string(),
            })?;

        tokio::fs::create_dir_all(&local_path)
            .await
            .map_err(|source| Error::LocalFileSystemInit {
                usage,
                location: local_path.display().to_string(),
                source: Box::new(source),
            })?;

        let store: Arc<dyn ObjectStore> = Arc::new(
            object_store_occ::LocalConditionalPut::new(&local_path).map_err(|source| {
                Error::LocalFileSystemInit {
                    usage,
                    location: local_path.display().to_string(),
                    source: Box::new(source),
                }
            })?,
        );

        return Ok((store, String::new()));
    }

    let base_prefix = if matches!(url.scheme(), "abfs" | "abfss") {
        String::new()
    } else {
        url.path().trim_matches('/').to_string()
    };

    let store: Arc<dyn ObjectStore> = if url.scheme() == "s3" {
        let params = params.map(Params::as_string_map);
        let s3_params = build_s3_parameters(Arc::clone(&secrets), params.as_ref(), usage).await?;

        S3ObjectStoreBuilder::from_url(&url, io_runtime)
            .context(S3ObjectStoreInitSnafu {
                usage,
                location: url.to_string(),
            })?
            .with_secret_params(&s3_params.to_secret_map())
            .context(S3ObjectStoreInitSnafu {
                usage,
                location: url.to_string(),
            })?
            .build()
            .await
            .context(S3ObjectStoreInitSnafu {
                usage,
                location: url.to_string(),
            })?
    } else if matches!(url.scheme(), "abfs" | "abfss") {
        let params = params.map(Params::as_string_map);
        let azure_params = build_secret_resolved_parameters(secrets, params.as_ref()).await;
        build_azure_object_store(&url, &azure_params, io_runtime).context(ObjectStoreInitSnafu {
            usage,
            location: url.to_string(),
        })?
    } else {
        let registry = SpiceObjectStoreRegistry::new(io_runtime);
        datafusion::execution::object_store::ObjectStoreRegistry::get_store(&registry, &url)
            .context(ObjectStoreInitSnafu {
                usage,
                location: url.to_string(),
            })?
    };

    Ok((store, base_prefix))
}

async fn build_s3_parameters(
    secrets: Arc<RwLock<Secrets>>,
    params: Option<&HashMap<String, String>>,
    usage: &'static str,
) -> Result<Parameters> {
    let default_params = || Parameters::new(vec![], "s3", &S3_PARAMETERS);
    match params {
        Some(params) => {
            let secret_params = get_params_with_secrets(Arc::clone(&secrets), params).await;
            Parameters::try_new(
                usage,
                secret_params.into_iter().collect(),
                "s3",
                secrets,
                &S3_PARAMETERS,
            )
            .await
            .map_err(|source| Error::S3ParameterValidation { usage, source })
        }
        None => Ok(default_params()),
    }
}

async fn build_secret_resolved_parameters(
    secrets: Arc<RwLock<Secrets>>,
    params: Option<&HashMap<String, String>>,
) -> HashMap<String, String> {
    match params {
        Some(params) => get_params_with_secrets(secrets, params)
            .await
            .into_iter()
            .map(|(key, value)| (key, value.expose_secret().to_string()))
            .collect(),
        None => HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use object_store_occ::{ObjectState, UpdateResult, WriteResult};
    use runtime_secrets::ClusterSecretExpander;
    use secrecy::SecretString;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestState {
        value: u64,
    }

    struct TestSecretExpander;

    #[async_trait]
    impl ClusterSecretExpander for TestSecretExpander {
        async fn expand_secret(
            &self,
            executor_id: &str,
            key: &str,
        ) -> std::result::Result<SecretString, String> {
            Ok(SecretString::from(format!("{executor_id}:{key}")))
        }
    }

    fn secrets() -> Arc<RwLock<Secrets>> {
        Arc::new(RwLock::new(Secrets::new()))
    }

    #[tokio::test]
    async fn build_object_store_file_location_creates_occ_capable_store() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state_dir = temp_dir.path().join("nested/state");
        let state_location = format!("file://{}", state_dir.display());

        let (store, prefix) = build_object_store(
            secrets(),
            Handle::current(),
            &state_location,
            None,
            "test state",
        )
        .await
        .expect("file state store should build");

        assert_eq!(prefix, "");
        assert!(state_dir.is_dir());

        let first_writer: ObjectState<TestState> = ObjectState::new(Arc::clone(&store));
        let second_writer: ObjectState<TestState> = ObjectState::new(store);

        let initial = TestState { value: 1 };
        assert_eq!(
            first_writer
                .insert_or_update("origin", &initial)
                .await
                .expect("initial write failed"),
            WriteResult::Inserted
        );

        assert_eq!(
            second_writer
                .get("origin")
                .await
                .expect("second writer get failed"),
            Some(initial)
        );

        let remote_update = TestState { value: 2 };
        assert_eq!(
            first_writer
                .update("origin", &remote_update)
                .await
                .expect("first writer update failed"),
            UpdateResult::Ok
        );

        let stale_update = TestState { value: 3 };
        let result = second_writer
            .insert_or_update("origin", &stale_update)
            .await
            .expect("stale write failed");

        match result {
            WriteResult::Conflict { current } => assert_eq!(current, remote_update),
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn build_object_store_file_location_handles_localhost_and_percent_encoding() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state_dir = temp_dir.path().join("nested state");
        let state_location = format!("file://localhost{}", state_dir.display()).replace(' ', "%20");

        let (_, prefix) = build_object_store(
            secrets(),
            Handle::current(),
            &state_location,
            None,
            "test state",
        )
        .await
        .expect("file state store should build");

        assert_eq!(prefix, "");
        assert!(state_dir.is_dir());
    }

    #[tokio::test]
    async fn build_object_store_rejects_invalid_location() {
        let err = build_object_store(
            secrets(),
            Handle::current(),
            "not a url",
            None,
            "test state",
        )
        .await
        .expect_err("invalid URL should fail");

        assert!(matches!(err, Error::InvalidStateLocation { .. }));
    }

    #[tokio::test]
    async fn build_secret_resolved_parameters_expands_secret_references() {
        let secrets = Arc::new(RwLock::new(Secrets::new_for_cluster_executor(
            Box::new(TestSecretExpander),
            "executor-1".to_string(),
        )));
        let mut params = HashMap::new();
        params.insert("account".to_string(), "account".to_string());
        params.insert(
            "access_key".to_string(),
            "${ secrets:AZURE_ACCESS_KEY }".to_string(),
        );

        let resolved = build_secret_resolved_parameters(secrets, Some(&params)).await;

        assert_eq!(resolved.get("account"), Some(&"account".to_string()));
        assert_eq!(
            resolved.get("access_key"),
            Some(&"executor-1:AZURE_ACCESS_KEY".to_string())
        );
    }

    #[tokio::test]
    async fn build_s3_parameters_expands_secret_references() {
        let secrets = Arc::new(RwLock::new(Secrets::new_for_cluster_executor(
            Box::new(TestSecretExpander),
            "executor-1".to_string(),
        )));
        let mut params = HashMap::new();
        params.insert("s3_auth".to_string(), "key".to_string());
        params.insert("s3_key".to_string(), "${ secrets:S3_KEY }".to_string());
        params.insert(
            "s3_secret".to_string(),
            "${ secrets:S3_SECRET }".to_string(),
        );

        let s3_params = build_s3_parameters(secrets, Some(&params), "test state")
            .await
            .expect("S3 parameters should validate");
        let secret_map = s3_params.to_secret_map();

        assert_eq!(
            secret_map.get("key").map(ExposeSecret::expose_secret),
            Some("executor-1:S3_KEY")
        );
        assert_eq!(
            secret_map.get("secret").map(ExposeSecret::expose_secret),
            Some("executor-1:S3_SECRET")
        );
    }

    #[tokio::test]
    async fn build_s3_parameters_rejects_invalid_values() {
        let mut params = HashMap::new();
        params.insert("s3_auth".to_string(), "not-valid".to_string());

        let err = build_s3_parameters(secrets(), Some(&params), "test state")
            .await
            .expect_err("invalid S3 parameters should fail");

        assert!(matches!(err, Error::S3ParameterValidation { .. }));
    }

    #[tokio::test]
    async fn build_object_store_accepts_s3_location_with_params() {
        let mut params = HashMap::new();
        params.insert("allow_http".to_string(), "true".to_string());
        params.insert(
            "s3_endpoint".to_string(),
            "http://localhost:9000".to_string(),
        );
        params.insert("s3_key".to_string(), "AKID".to_string());
        params.insert("s3_region".to_string(), "us-east-1".to_string());
        params.insert("s3_secret".to_string(), "SECRET".to_string());
        let params = Params::from_string_map(params);

        let result = build_object_store(
            secrets(),
            Handle::current(),
            "s3://spice-state/runtime/rate-control/",
            Some(&params),
            "test state",
        )
        .await;

        assert!(result.is_ok(), "s3 state store should build: {result:?}");
        let (_, prefix) = result.expect("s3 state store should build");
        assert_eq!(prefix, "runtime/rate-control");
    }

    #[tokio::test]
    async fn build_object_store_accepts_abfs_location_with_params() {
        let mut params = HashMap::new();
        params.insert("use_emulator".to_string(), "true".to_string());
        let params = Params::from_string_map(params);

        let result = build_object_store(
            secrets(),
            Handle::current(),
            "abfs://testcontainer/runtime/rate-control/",
            Some(&params),
            "test state",
        )
        .await;

        assert!(result.is_ok(), "abfs state store should build: {result:?}");
        let (_, prefix) = result.expect("abfs state store should build");
        assert_eq!(prefix, "");
    }

    #[tokio::test]
    async fn build_object_store_accepts_abfss_location_with_params() {
        let mut params = HashMap::new();
        params.insert("account".to_string(), "account".to_string());
        params.insert("access_key".to_string(), "ZHVtbXlrZXk=".to_string());
        let params = Params::from_string_map(params);

        let result = build_object_store(
            secrets(),
            Handle::current(),
            "abfss://container@account.dfs.core.windows.net/runtime/rate-control/",
            Some(&params),
            "test state",
        )
        .await;

        assert!(result.is_ok(), "abfss state store should build: {result:?}");
        let (_, prefix) = result.expect("abfss state store should build");
        assert_eq!(prefix, "");
    }
}
