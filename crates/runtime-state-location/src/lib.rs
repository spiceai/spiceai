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

//! Builds the object store behind a location where Spice keeps shared state:
//! the cluster scheduler, source rate control, SQL results-cache warmup and
//! acceleration snapshots all resolve their location to a store here.
//!
//! The features still differ in a few ways, kept as they were configured
//! before they shared this builder; [`StateFeature`] lists each one.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::{Arc, LazyLock};

use aws_sdk_credential_bridge::object_store_builder::S3ObjectStoreBuilder;
use object_store::ObjectStore;
use runtime_object_store::registry::SpiceObjectStoreRegistry;
use runtime_object_store::{build_azure_object_store, build_gcs_object_store};
use runtime_parameters::{ParameterSpec, Parameters};
use runtime_secrets::{Secrets, get_params_with_secrets};
use secrecy::ExposeSecret;
use snafu::prelude::*;
use spicepod::component::runtime::{StateLocation, StateSource};
use spicepod::component::snapshot::SNAPSHOTS_STATE_FOLDER;
use spicepod::param::Params;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use url::Url;

mod check;

pub use check::{
    CheckError, FORMAT_FILE, FORMAT_VERSION, RUNTIME_STATE_DOCS, StateLocationCheck,
    check_location,
};

fn s3_parameters() -> Vec<ParameterSpec> {
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
}

static S3_PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(s3_parameters);

/// The `params` key, without its `s3_` prefix, that names the SQS queue receiving a
/// snapshot location's S3 event notifications.
pub const SNAPSHOT_QUEUE_URL_PARAM: &str = "queue_url";

/// A snapshot location's S3 parameters: the state location's, and the queue that
/// receives its S3 event notifications.
static SNAPSHOT_S3_PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut parameters = s3_parameters();
    parameters.push(
        ParameterSpec::component(SNAPSHOT_QUEUE_URL_PARAM)
            .description("The URL of an SQS queue that receives the snapshot location's S3 event notifications. Datasets with `refresh_mode: snapshot` reload as soon as a new snapshot is published instead of waiting for `refresh_check_interval`.")
            .secret(),
    );
    parameters
});

/// The GCS data connector's object-store parameters, so a `gs://` location takes the
/// same names (`gcs_skip_signature`, `gcs_service_account_path`, ...).
static GCS_PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    vec![
        ParameterSpec::component("service_account_path").secret(),
        ParameterSpec::component("service_account_key").secret(),
        ParameterSpec::component("application_default_credentials").is_boolean(),
        ParameterSpec::component("skip_signature").is_boolean(),
        ParameterSpec::component("max_retries"),
        ParameterSpec::component("retry_timeout"),
        ParameterSpec::component("backoff_initial_duration"),
        ParameterSpec::component("backoff_max_duration"),
        ParameterSpec::component("backoff_base"),
        ParameterSpec::runtime("client_timeout"),
        ParameterSpec::runtime("allow_http").is_boolean(),
    ]
});

#[derive(Debug, Snafu)]
pub enum Error {
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

    #[snafu(display("Failed to initialize {usage} object store for {location}: {source}"))]
    ObjectStoreParse {
        usage: &'static str,
        location: String,
        source: object_store::Error,
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

    #[snafu(display("Failed to validate GCS parameters for {usage}: {source}"))]
    GcsParameterValidation {
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

    #[snafu(display(
        "Failed to parse {usage} location {location}: a Google Cloud Storage location needs a bucket, as in gs://bucket/path"
    ))]
    InvalidGcsLocation {
        usage: &'static str,
        location: String,
    },

    #[snafu(display("Failed to initialize local filesystem for {usage} at {location}: {source}"))]
    CreateLocalDirectory {
        usage: &'static str,
        location: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to initialize local filesystem for {usage} at {location}: {source}"))]
    LocalFileSystemInit {
        usage: &'static str,
        location: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A runtime feature that keeps shared state in a state location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StateFeature {
    Scheduler,
    RateControl,
    ResultsCacheWarmup,
    Snapshots,
    /// The startup check of a `runtime.state` location, which keeps
    /// `format.json` and its probe object in the location's `_spice/` folder.
    StartupCheck,
}

impl StateFeature {
    /// The folder the feature writes under in a `runtime.state` location.
    fn folder(self) -> &'static str {
        match self {
            Self::Scheduler => "cluster",
            Self::RateControl => "rate-control",
            Self::ResultsCacheWarmup => "results-cache",
            Self::Snapshots => SNAPSHOTS_STATE_FOLDER,
            Self::StartupCheck => "_spice",
        }
    }

    fn usage(self) -> &'static str {
        match self {
            Self::Scheduler => "scheduler state",
            Self::RateControl => "rate-control state",
            Self::ResultsCacheWarmup => "SQL results cache warmup state",
            Self::Snapshots => "snapshot",
            Self::StartupCheck => "shared state location check",
        }
    }

    /// The feature's name in log messages.
    #[must_use]
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Scheduler => "the cluster scheduler",
            Self::RateControl => "source rate control",
            Self::ResultsCacheWarmup => "SQL results cache warmup",
            Self::Snapshots => "acceleration snapshots",
            Self::StartupCheck => "the startup check",
        }
    }

    /// The S3 parameters the feature's `params` are validated against.
    fn s3_parameters(self) -> &'static [ParameterSpec] {
        match self {
            Self::Scheduler | Self::RateControl | Self::ResultsCacheWarmup | Self::StartupCheck => {
                &S3_PARAMETERS
            }
            Self::Snapshots => &SNAPSHOT_S3_PARAMETERS,
        }
    }

    /// Whether a `gs://` location's `params` take the GCS data connector's names
    /// and are validated. Snapshot `params` take the GCS store's own names
    /// (`skip_signature`, ...) as written, unless they use the connector's `gcs_`
    /// names, as `runtime.state.params` do; see [`uses_gcs_connector_names`].
    fn validates_gcs_parameters(self) -> bool {
        !matches!(self, Self::Snapshots)
    }

    /// Whether the feature's own location setting keeps an `abfs://` or `abfss://`
    /// URL path. The scheduler, rate control and warmup settings keep their state
    /// at the container root, where earlier versions wrote it.
    fn keeps_abfs_path_in_own_setting(self) -> bool {
        matches!(self, Self::Snapshots)
    }
}

/// Builds the object store for `feature` at `state` and returns it with the
/// key prefix the feature writes under.
///
/// A `runtime.state` location puts each feature in its own folder below the
/// location's path. A feature's own location setting puts the feature at the
/// location's path; for the scheduler, rate control and warmup on `abfs://` and
/// `abfss://` that is the container root, whatever path the URL has.
///
/// `s3://`, `abfs://`, `abfss://`, `gs://` and `file://` locations build the
/// same store for every feature. A snapshot location with another scheme is
/// built by `object_store`'s URL parsing; other features resolve it through the
/// runtime's object-store registry.
///
/// # Errors
///
/// Returns an error when the location is not a valid URL, its `params` are
/// invalid, or the store cannot be built.
pub async fn build_object_store(
    secrets: Arc<RwLock<Secrets>>,
    io_runtime: Handle,
    state: &StateLocation,
    feature: StateFeature,
) -> Result<(Arc<dyn ObjectStore>, String)> {
    let usage = feature.usage();
    let state_location = state.location.as_str();
    let params = state.params.as_ref();
    let url = Url::parse(state_location).context(InvalidStateLocationSnafu {
        usage,
        location: state_location,
    })?;
    let feature_prefix = |location_prefix: &str| match state.source {
        StateSource::RuntimeState if location_prefix.is_empty() => feature.folder().to_string(),
        StateSource::RuntimeState => format!("{location_prefix}/{}", feature.folder()),
        StateSource::FeatureSetting => location_prefix.to_string(),
    };

    if url.scheme() == "file" {
        let local_path = url
            .to_file_path()
            .map_err(|()| Error::InvalidFileLocation {
                usage,
                location: state_location.to_string(),
            })?;

        tokio::fs::create_dir_all(&local_path)
            .await
            .context(CreateLocalDirectorySnafu {
                usage,
                location: local_path.display().to_string(),
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

        return Ok((store, feature_prefix("")));
    }

    let base_prefix = if matches!(url.scheme(), "abfs" | "abfss")
        && state.source == StateSource::FeatureSetting
        && !feature.keeps_abfs_path_in_own_setting()
    {
        String::new()
    } else {
        feature_prefix(url.path().trim_matches('/'))
    };

    let store: Arc<dyn ObjectStore> = if url.scheme() == "s3" {
        let params = params.map(Params::as_string_map);
        let s3_params = build_s3_parameters(Arc::clone(&secrets), params.as_ref(), feature).await?;

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
    } else if matches!(url.scheme(), "gs" | "gcs") {
        let bucket = url.host_str().ok_or_else(|| Error::InvalidGcsLocation {
            usage,
            location: url.to_string(),
        })?;
        let params = params.map(Params::as_string_map);
        let gcs_params = if feature.validates_gcs_parameters()
            || params.as_ref().is_some_and(uses_gcs_connector_names)
        {
            build_gcs_parameters(secrets, params.as_ref(), usage).await?
        } else {
            build_secret_resolved_parameters(secrets, params.as_ref()).await
        };
        build_gcs_object_store(bucket, &gcs_params, io_runtime).context(ObjectStoreInitSnafu {
            usage,
            location: url.to_string(),
        })?
    } else if feature == StateFeature::Snapshots {
        object_store::parse_url(&url)
            .context(ObjectStoreParseSnafu {
                usage,
                location: url.to_string(),
            })?
            .0
            .into()
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

/// Validates an `s3://` location's `params` against `feature`'s S3 parameters,
/// resolving `${ secrets:… }` references.
///
/// An invalid parameter is an error, not a reason to fall back to the defaults:
/// the defaults drop every other parameter too, the endpoint, the region and the
/// keys, so the store would silently point somewhere other than configured.
///
/// # Errors
///
/// Returns [`Error::S3ParameterValidation`] when a parameter is invalid.
pub async fn build_s3_parameters<S: BuildHasher>(
    secrets: Arc<RwLock<Secrets>>,
    params: Option<&HashMap<String, String, S>>,
    feature: StateFeature,
) -> Result<Parameters> {
    let usage = feature.usage();
    let s3_parameters = feature.s3_parameters();
    match params {
        Some(params) => {
            let params: HashMap<String, String> = params
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            let secret_params = get_params_with_secrets(Arc::clone(&secrets), &params).await;
            Parameters::try_new(
                usage,
                secret_params.into_iter().collect(),
                "s3",
                secrets,
                s3_parameters,
            )
            .await
            .map_err(|source| Error::S3ParameterValidation { usage, source })
        }
        None => Ok(Parameters::new(vec![], "s3", s3_parameters)),
    }
}

/// Whether `params` name a GCS setting the way the GCS data connector does
/// (`gcs_skip_signature`), which the GCS store does not read until they are
/// validated and mapped to its own names.
fn uses_gcs_connector_names(params: &HashMap<String, String>) -> bool {
    params.keys().any(|key| key.starts_with("gcs_"))
}

/// Resolves a `gs://` location's `params`, given under the GCS data connector's
/// names, to the names [`build_gcs_object_store`] reads.
async fn build_gcs_parameters(
    secrets: Arc<RwLock<Secrets>>,
    params: Option<&HashMap<String, String>>,
    usage: &'static str,
) -> Result<HashMap<String, String>> {
    let Some(params) = params else {
        return Ok(HashMap::new());
    };
    let secret_params = get_params_with_secrets(Arc::clone(&secrets), params).await;
    let params = Parameters::try_new(
        usage,
        secret_params.into_iter().collect(),
        "gcs",
        secrets,
        &GCS_PARAMETERS,
    )
    .await
    .map_err(|source| Error::GcsParameterValidation { usage, source })?;
    // Same contract as the GCS data connector: `build_gcs_object_store` uses only one
    // method, so a second one would be silently ignored.
    runtime_parameters::gcs::validate_auth(&params).map_err(|source| {
        Error::GcsParameterValidation {
            usage,
            source: Box::new(source),
        }
    })?;
    Ok(params
        .to_secret_map()
        .into_iter()
        .map(|(key, value)| (key, value.expose_secret().to_string()))
        .collect())
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
        ) -> std::result::Result<Option<SecretString>, String> {
            Ok(Some(SecretString::from(format!("{executor_id}:{key}"))))
        }
    }

    fn secrets() -> Arc<RwLock<Secrets>> {
        Arc::new(RwLock::new(Secrets::new()))
    }

    fn feature_setting(location: &str, params: Option<&Params>) -> StateLocation {
        StateLocation {
            location: location.to_string(),
            params: params.cloned(),
            source: StateSource::FeatureSetting,
        }
    }

    fn runtime_state(location: &str, params: &[(&str, &str)]) -> StateLocation {
        StateLocation {
            location: location.to_string(),
            params: Some(Params::from_string_map(
                params
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect(),
            )),
            source: StateSource::RuntimeState,
        }
    }

    /// A `runtime.state` `file://` location gives each feature its own folder.
    #[tokio::test]
    async fn runtime_state_file_location_writes_each_feature_to_its_folder() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state_location = format!("file://{}", temp_dir.path().display());

        for (feature, folder) in [
            (StateFeature::Scheduler, "cluster"),
            (StateFeature::RateControl, "rate-control"),
            (StateFeature::ResultsCacheWarmup, "results-cache"),
            (StateFeature::Snapshots, "snapshots"),
        ] {
            let (store, prefix) = build_object_store(
                secrets(),
                Handle::current(),
                &runtime_state(&state_location, &[]),
                feature,
            )
            .await
            .expect("file state store should build");
            let writer: ObjectState<TestState> =
                ObjectState::new(store).with_prefix(format!("{prefix}/"));
            writer
                .insert("origin", &TestState { value: 1 })
                .await
                .expect("write state");

            let written = temp_dir.path().join(folder).join("origin.json");
            assert!(
                written.is_file(),
                "{feature:?} must write {}",
                written.display()
            );
        }
        let mut root: Vec<_> = std::fs::read_dir(temp_dir.path())
            .expect("list state root")
            .map(|entry| entry.expect("state root entry").file_name())
            .filter(|name| name != ".spice-object-store-occ.lock")
            .collect();
        root.sort();
        assert_eq!(
            root,
            ["cluster", "rate-control", "results-cache", "snapshots"]
        );
    }

    const S3_KEY_PARAMS: &[(&str, &str)] = &[
        ("s3_endpoint", "http://localhost:9000"),
        ("s3_key", "AKID"),
        ("s3_secret", "SECRET"),
        ("s3_region", "us-east-1"),
    ];

    /// A `runtime.state` location keeps its URL path, for `abfs://` and
    /// `abfss://` too, and adds the feature's folder below it.
    #[tokio::test]
    async fn runtime_state_location_prefix_is_path_and_feature_folder() {
        for (state, feature, expected_prefix) in [
            (
                runtime_state("s3://bucket/spice/", S3_KEY_PARAMS),
                StateFeature::Scheduler,
                "spice/cluster",
            ),
            (
                runtime_state("s3://bucket", S3_KEY_PARAMS),
                StateFeature::RateControl,
                "rate-control",
            ),
            (
                runtime_state("abfs://container/spice/state/", &[("use_emulator", "true")]),
                StateFeature::ResultsCacheWarmup,
                "spice/state/results-cache",
            ),
            (
                runtime_state(
                    "abfss://container@account.dfs.core.windows.net/spice/",
                    &[("account", "account"), ("access_key", "ZHVtbXlrZXk=")],
                ),
                StateFeature::Scheduler,
                "spice/cluster",
            ),
            (
                runtime_state("gs://bucket/spice/", &[("gcs_skip_signature", "true")]),
                StateFeature::RateControl,
                "spice/rate-control",
            ),
        ] {
            let (_, prefix) = build_object_store(secrets(), Handle::current(), &state, feature)
                .await
                .expect("runtime.state store should build");
            assert_eq!(
                prefix, expected_prefix,
                "{} for {feature:?}",
                state.location
            );
        }
    }

    #[tokio::test]
    async fn build_object_store_file_location_creates_occ_capable_store() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let state_dir = temp_dir.path().join("nested/state");
        let state_location = format!("file://{}", state_dir.display());

        let (store, prefix) = build_object_store(
            secrets(),
            Handle::current(),
            &feature_setting(&state_location, None),
            StateFeature::RateControl,
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
            &feature_setting(&state_location, None),
            StateFeature::RateControl,
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
            &feature_setting("not a url", None),
            StateFeature::RateControl,
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

        let s3_params = build_s3_parameters(secrets, Some(&params), StateFeature::RateControl)
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

        let err = build_s3_parameters(secrets(), Some(&params), StateFeature::RateControl)
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
            &feature_setting("s3://spice-state/runtime/rate-control/", Some(&params)),
            StateFeature::RateControl,
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
            &feature_setting("abfs://testcontainer/runtime/rate-control/", Some(&params)),
            StateFeature::RateControl,
        )
        .await;

        assert!(result.is_ok(), "abfs state store should build: {result:?}");
        let (_, prefix) = result.expect("abfs state store should build");
        assert_eq!(prefix, "");
    }

    /// A `gs://` location honors the GCS data connector's parameter names (and keeps
    /// its path as the prefix) instead of building a store from the environment alone.
    #[tokio::test]
    async fn build_object_store_accepts_gcs_location_with_params() {
        let params = Params::from_string_map(HashMap::from([(
            "gcs_skip_signature".to_string(),
            "true".to_string(),
        )]));

        let (store, prefix) = build_object_store(
            secrets(),
            Handle::current(),
            &feature_setting("gs://state-bucket/runtime/rate-control/", Some(&params)),
            StateFeature::RateControl,
        )
        .await
        .expect("gcs state store should build");
        assert_eq!(prefix, "runtime/rate-control");
        // The store has no accessor for its signing mode; its `Debug` output carries
        // the configuration it was built with.
        assert!(
            format!("{store:?}").contains("skip_signature: true"),
            "`gcs_skip_signature: true` must build an unsigned store: {store:?}"
        );

        let invalid = build_object_store(
            secrets(),
            Handle::current(),
            &feature_setting(
                "gs://state-bucket/",
                Some(&Params::from_string_map(HashMap::from([(
                    "gcs_skip_signature".to_string(),
                    "not-a-bool".to_string(),
                )]))),
            ),
            StateFeature::RateControl,
        )
        .await
        .expect_err("an invalid GCS parameter is reported, not ignored");
        assert_eq!(
            invalid.to_string(),
            "Failed to validate GCS parameters for rate-control state: Invalid configuration for rate-control state. 'gcs_skip_signature' parameter must be one of: true, false. Found not-a-bool."
        );
    }

    /// A `gs://` location takes one authentication method, as the GCS data connector
    /// does: with `gcs_skip_signature: true` the store would silently ignore a key.
    #[tokio::test]
    async fn build_object_store_rejects_gcs_location_with_two_auth_methods() {
        let params = Params::from_string_map(HashMap::from([
            ("gcs_skip_signature".to_string(), "true".to_string()),
            (
                "gcs_service_account_key".to_string(),
                r#"{"client_email": "state@example.com", "private_key": "", "private_key_id": ""}"#
                    .to_string(),
            ),
        ]));

        let err = build_object_store(
            secrets(),
            Handle::current(),
            &feature_setting("gs://state-bucket/runtime/rate-control/", Some(&params)),
            StateFeature::RateControl,
        )
        .await
        .expect_err("two GCS authentication methods are reported, not resolved silently");
        assert_eq!(
            err.to_string(),
            "Failed to validate GCS parameters for rate-control state: Multiple authentication methods were provided. Specify only one of the following: gcs_service_account_path, gcs_service_account_key, gcs_application_default_credentials, or gcs_skip_signature. For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#auth"
        );
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
            &feature_setting(
                "abfss://container@account.dfs.core.windows.net/runtime/rate-control/",
                Some(&params),
            ),
            StateFeature::RateControl,
        )
        .await;

        assert!(result.is_ok(), "abfss state store should build: {result:?}");
        let (_, prefix) = result.expect("abfss state store should build");
        assert_eq!(prefix, "");
    }

    /// A snapshot location keeps its URL path as the prefix for every scheme,
    /// `abfs://` included, where the scheduler's own setting uses the container root.
    #[tokio::test]
    async fn snapshot_location_prefix_is_its_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let file_location = format!("file://{}/snapshots/", temp_dir.path().display());
        let abfs_params = Params::from_string_map(HashMap::from([(
            "use_emulator".to_string(),
            "true".to_string(),
        )]));
        let s3_params = Params::from_string_map(
            S3_KEY_PARAMS
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        );

        for (state, expected_prefix) in [
            (
                feature_setting("s3://bucket/spice/snapshots/", Some(&s3_params)),
                "spice/snapshots",
            ),
            (
                feature_setting("abfs://container/spice/snapshots/", Some(&abfs_params)),
                "spice/snapshots",
            ),
            (feature_setting("gs://bucket/snapshots", None), "snapshots"),
            (feature_setting(&file_location, None), ""),
        ] {
            let (_, prefix) = build_object_store(
                secrets(),
                Handle::current(),
                &state,
                StateFeature::Snapshots,
            )
            .await
            .expect("snapshot store should build");
            assert_eq!(prefix, expected_prefix, "{}", state.location);
        }

        let (_, scheduler_prefix) = build_object_store(
            secrets(),
            Handle::current(),
            &feature_setting("abfs://container/spice/snapshots/", Some(&abfs_params)),
            StateFeature::Scheduler,
        )
        .await
        .expect("scheduler store should build");
        assert_eq!(scheduler_prefix, "");
    }

    /// A snapshot location's S3 `params` take the queue that receives its S3 event
    /// notifications.
    #[tokio::test]
    async fn snapshot_s3_parameters_take_the_notification_queue() {
        let params = HashMap::from([(
            "s3_queue_url".to_string(),
            "https://sqs.us-east-1.amazonaws.com/123456789012/snapshots".to_string(),
        )]);

        let parameters = build_s3_parameters(secrets(), Some(&params), StateFeature::Snapshots)
            .await
            .expect("snapshot S3 parameters should validate");

        assert_eq!(
            parameters.get(SNAPSHOT_QUEUE_URL_PARAM).expose().ok(),
            Some("https://sqs.us-east-1.amazonaws.com/123456789012/snapshots")
        );
    }

    /// A snapshot location's `gs://` `params` reach the GCS store as written when
    /// they use its own names. With the GCS data connector's `gcs_` names, which
    /// a location inherited from `runtime.state` has, they are validated and
    /// mapped as for every other state location, not ignored.
    #[tokio::test]
    async fn snapshot_gcs_parameters_take_store_or_connector_names() {
        for (key, value) in [("skip_signature", "true"), ("gcs_skip_signature", "true")] {
            let params =
                Params::from_string_map(HashMap::from([(key.to_string(), value.to_string())]));
            let (store, _) = build_object_store(
                secrets(),
                Handle::current(),
                &feature_setting("gs://snapshot-bucket/snapshots/", Some(&params)),
                StateFeature::Snapshots,
            )
            .await
            .expect("snapshot GCS store should build");
            assert!(
                format!("{store:?}").contains("skip_signature: true"),
                "`{key}: true` must build an unsigned store: {store:?}"
            );
        }

        let not_a_bool = Params::from_string_map(HashMap::from([(
            "gcs_skip_signature".to_string(),
            "not-a-bool".to_string(),
        )]));
        let err = build_object_store(
            secrets(),
            Handle::current(),
            &feature_setting("gs://snapshot-bucket/snapshots/", Some(&not_a_bool)),
            StateFeature::Snapshots,
        )
        .await
        .expect_err("an invalid GCS connector parameter is reported, not ignored");
        assert_eq!(
            err.to_string(),
            "Failed to validate GCS parameters for snapshot: Invalid configuration for snapshot. 'gcs_skip_signature' parameter must be one of: true, false. Found not-a-bool."
        );
    }
}
