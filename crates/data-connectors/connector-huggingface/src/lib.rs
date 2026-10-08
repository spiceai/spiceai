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

//! The Hugging Face data connector: `from: hf://datasets/<owner>/<dataset>[@<revision>]/<path>`
//! reads the Parquet, CSV, TSV, JSON and ORC files of a dataset on the Hugging Face Hub.
//!
//! Each scan reads one commit of the dataset (see [`table`]), and the files are read through
//! the Hub's own API, so private and gated datasets work with an `hf_token`.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Weak};

use async_trait::async_trait;
use data_connector_api::listing::{
    LISTING_TABLE_PARAMETERS, ListingTableConnector, detect_file_extension_from_path,
    detect_file_extension_from_url_or_path,
};
use data_connector_api::{
    ConnectorComponent, ConnectorContext, ConnectorParams, DataConnector, DataConnectorError,
    DataConnectorFactory, DataConnectorResult, NewDataConnectorResult,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::ObjectStore;
use parking_lot::Mutex;
use runtime_component::dataset::DatasetSpec;
use runtime_parameters::{ExposedParamLookup, ParameterSpec, Parameters};
use secrecy::SecretString;
use snafu::prelude::*;
use tokio::runtime::Handle;
use url::Url;

pub mod hub;
pub mod location;
pub mod store;
pub mod table;
#[cfg(test)]
mod tests;

use hub::{Commit, EntryKind, Hub, HubConfig};
use location::DatasetLocation;
use table::HuggingFaceTable;

// `register_data_connector!` names `linkme` unqualified.
use data_connector_api::linkme;

/// The name used to identify this connector in configuration: `from: hf://...`.
pub const CONNECTOR_NAME: &str = "hf";
const DOCS_URL: &str = "https://spiceai.org/docs/components/data-connectors/huggingface";

/// File formats a dataset's format is inferred from, when neither the location nor
/// `file_format` names one.
const INFERABLE_FORMATS: &[&str] = &[
    "parquet", "csv", "tsv", "json", "jsonl", "ndjson", "ldjson", "orc",
];

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display(
        "`hf_endpoint` {endpoint:?} is not a valid endpoint: use an https:// URL such as '{}' (http:// is accepted only for a loopback address). See: {DOCS_URL}",
        hub::DEFAULT_ENDPOINT
    ))]
    InvalidEndpoint { endpoint: String },

    #[snafu(display("{source}"))]
    Hub { source: hub::Error },
}

static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = vec![
        ParameterSpec::component("token")
            .description("A Hugging Face User Access Token, to read private and gated datasets.")
            .secret(),
        ParameterSpec::component("endpoint")
            .description(
                "The Hugging Face Hub endpoint, to read through a mirror or proxy of the Hub.",
            )
            .default(hub::DEFAULT_ENDPOINT),
    ];
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

/// Hub clients by configuration fingerprint, so datasets read with the same endpoint and token
/// share one client and its caches.
static HUBS: LazyLock<Mutex<HashMap<String, Weak<Hub>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn shared_hub(config: HubConfig, io_runtime: Handle) -> Result<Arc<Hub>, hub::Error> {
    let fingerprint = config.fingerprint();
    let mut hubs = HUBS.lock();
    if let Some(hub) = hubs.get(&fingerprint).and_then(Weak::upgrade) {
        return Ok(hub);
    }
    hubs.retain(|_, hub| hub.strong_count() > 0);
    let hub = Arc::new(Hub::new(config, io_runtime)?);
    hubs.insert(fingerprint, Arc::downgrade(&hub));
    Ok(hub)
}

/// Parses `hf_endpoint`. The token is sent to it, so it must be https unless it is loopback.
fn parse_endpoint(endpoint: &str) -> Result<Url, Error> {
    let invalid = || Error::InvalidEndpoint {
        endpoint: endpoint.to_string(),
    };
    let url = Url::parse(endpoint).map_err(|_| invalid())?;
    let loopback = match url.host() {
        Some(url::Host::Domain(domain)) => domain == "localhost",
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    };
    let valid = url.has_host()
        && url.query().is_none()
        && url.fragment().is_none()
        && url.username().is_empty()
        && url.password().is_none()
        && (url.scheme() == "https" || (url.scheme() == "http" && loopback));
    ensure!(valid, InvalidEndpointSnafu { endpoint });
    Ok(url)
}

#[derive(Default, Debug, Copy, Clone)]
pub struct HuggingFaceFactory {}

impl HuggingFaceFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for HuggingFaceFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        _context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
        Box::pin(async move {
            let endpoint = match params.parameters.get("endpoint").expose() {
                ExposedParamLookup::Present(endpoint) => parse_endpoint(endpoint)?,
                ExposedParamLookup::Absent(_) => parse_endpoint(hub::DEFAULT_ENDPOINT)?,
            };
            let token: Option<SecretString> = params.parameters.get("token").ok().cloned();
            let hub = shared_hub(HubConfig { endpoint, token }, params.io_runtime.clone())
                .context(HubSnafu)?;
            Ok(Arc::new(HuggingFace {
                params: params.parameters,
                hub,
                io_runtime: params.io_runtime,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "hf"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}

/// Reads the datasets of one Hub endpoint with one token.
pub struct HuggingFace {
    params: Parameters,
    hub: Arc<Hub>,
    io_runtime: Handle,
}

impl fmt::Debug for HuggingFace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HuggingFace")
            .field("hub", &self.hub)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for HuggingFace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{CONNECTOR_NAME}")
    }
}

impl HuggingFace {
    /// The dataset's table: the files `from` selects, read at the commit each scan resolves.
    async fn table(&self, dataset: &DatasetSpec) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let location = Self::location(dataset)?;
        // Resolving the revision first checks that the dataset exists and can be read, and
        // pins the schema to one commit.
        let commit = self
            .hub
            .commit(location.repo(), location.revision())
            .await
            .map_err(|source| hub_error(dataset, source))?;
        store::store().register(location.repo().clone(), Arc::clone(&self.hub));

        let mut params = self.params.clone();
        if let Some(extension) = self
            .inferred_file_extension(dataset, &location, &commit)
            .await?
        {
            tracing::debug!(
                "Dataset {} reads the '.{extension}' files of Hugging Face dataset '{}'",
                dataset.name,
                location.repo()
            );
            params.insert("file_extension".to_string(), SecretString::from(extension));
        }

        let listing_url = table::listing_url(&location, &commit.sha).map_err(|source| {
            DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(source),
            }
        })?;
        let listing = HuggingFaceListing {
            params,
            io_runtime: self.io_runtime.clone(),
            location: location.clone(),
            commit: commit.sha.clone(),
        };

        let (file_format, extension) = listing.get_file_format_and_extension(dataset).await?;
        let Some(file_format) = file_format else {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: format!(
                    "`file_format` '{}' is not supported for Hugging Face datasets: use parquet, csv, tsv, json, jsonl or orc. See: {DOCS_URL}",
                    extension.trim_start_matches('.')
                ),
            });
        };
        let template = listing
            .listing_table_template(dataset, listing_url.as_ref(), &extension, file_format)
            .await?;
        let table =
            HuggingFaceTable::try_new(location, Arc::clone(&self.hub), template, commit.sha)
                .map_err(|source| DataConnectorError::UnableToGetReadProvider {
                    dataconnector: CONNECTOR_NAME.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(source),
                })?;
        Ok(Arc::new(table))
    }

    fn location(dataset: &DatasetSpec) -> DataConnectorResult<DatasetLocation> {
        DatasetLocation::parse(&dataset.from).map_err(|source| {
            DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: source.to_string(),
            }
        })
    }

    /// Infers the file extension to list when neither the location nor `file_format` or
    /// `file_extension` names one: the one data format among the files the location selects.
    async fn inferred_file_extension(
        &self,
        dataset: &DatasetSpec,
        location: &DatasetLocation,
        commit: &Commit,
    ) -> DataConnectorResult<Option<String>> {
        let named = |key: &str| {
            matches!(
                self.params.get(key).expose(),
                ExposedParamLookup::Present(_)
            )
        };
        let from_names_format = detect_file_extension_from_url_or_path(&dataset.from)
            .is_some_and(|extension| extension.format_extension.is_some());
        if named("file_format") || named("file_extension") || from_names_format {
            return Ok(None);
        }

        let (folder, glob) = match location.glob() {
            Some((folder, glob)) => (
                folder,
                Some(glob::Pattern::new(glob).map_err(|e| {
                    DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: CONNECTOR_NAME.to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        message: format!("Invalid glob '{glob}' in `from`: {e}"),
                    }
                })?),
            ),
            None => (location.path(), None),
        };
        let entries = self
            .hub
            .list(location.repo(), &commit.sha, folder, true)
            .await
            .map_err(|source| hub_error(dataset, source))?;

        // Extensions of the selected files, by whether they are a data format.
        let mut data_extensions = BTreeSet::new();
        let mut other_extensions = BTreeMap::new();
        for entry in entries.iter().filter(|entry| entry.kind == EntryKind::File) {
            let relative = entry
                .path
                .strip_prefix(folder)
                .map_or(entry.path.as_str(), |rest| rest.trim_start_matches('/'));
            if glob.as_ref().is_some_and(|glob| !glob.matches(relative)) {
                continue;
            }
            let Some(extension) = detect_file_extension_from_path(&entry.path) else {
                continue;
            };
            match extension.format_extension.as_deref() {
                Some(format) if INFERABLE_FORMATS.contains(&format) => {
                    data_extensions
                        .insert(extension.file_extension.trim_start_matches('.').to_string());
                }
                _ => {
                    other_extensions
                        .entry(extension.file_extension)
                        .or_insert(entry.path.clone());
                }
            }
        }

        let selected = if location.path().is_empty() {
            format!("dataset '{}'", location.repo())
        } else {
            format!("'{}' in dataset '{}'", location.path(), location.repo())
        };
        match data_extensions.len() {
            1 => Ok(data_extensions.pop_first()),
            0 if other_extensions.is_empty() => {
                Err(DataConnectorError::ObjectStoreNoFilesAvailable {
                    dataconnector: CONNECTOR_NAME.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    message: format!(
                        "No files were found under {selected} at commit {}. Check the path in `from`.",
                        commit.sha
                    ),
                })
            }
            0 => Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: format!(
                    "No Parquet, CSV, TSV, JSON or ORC files were found under {selected} (found {}). Point `from` at the dataset's data files, or read the Hub's Parquet conversion of it with '@~parquet'. See: {DOCS_URL}",
                    other_extensions
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }),
            _ => Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: format!(
                    "{selected} holds files of more than one format ({}). Narrow `from` to one format's files, for example with a glob such as '*.parquet', or set `file_format`. See: {DOCS_URL}",
                    data_extensions
                        .iter()
                        .map(|extension| format!(".{extension}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }),
        }
    }
}

/// Maps a Hub error during dataset registration to the connector error a user acts on.
fn hub_error(dataset: &DatasetSpec, source: hub::Error) -> DataConnectorError {
    let dataconnector = CONNECTOR_NAME.to_string();
    let connector_component = ConnectorComponent::from(dataset);
    match source {
        hub::Error::RepoNotFound { .. }
        | hub::Error::RevisionNotFound { .. }
        | hub::Error::InvalidToken => DataConnectorError::InvalidConfigurationNoSource {
            dataconnector,
            connector_component,
            message: source.to_string(),
        },
        hub::Error::Gated { .. } | hub::Error::TokenRejected { .. } => {
            DataConnectorError::InsufficientPermissions {
                dataconnector,
                connector_component,
                source: Box::new(source),
            }
        }
        hub::Error::RateLimited { .. } => DataConnectorError::RateLimited {
            dataconnector,
            connector_component,
            source: Box::new(source),
        },
        source => DataConnectorError::UnableToConnectInternal {
            dataconnector,
            connector_component,
            source: Box::new(source),
        },
    }
}

#[async_trait]
impl DataConnector for HuggingFace {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        self.table(dataset).await
    }

    async fn metadata_provider(
        &self,
        dataset: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        dataset.has_metadata_table.then(|| {
            Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: format!(
                    "`has_metadata_table` is not supported for Hugging Face datasets; remove it from the dataset. See: {DOCS_URL}"
                ),
            })
        })
    }

    async fn register_object_stores(
        &self,
        dataset: &DatasetSpec,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> DataConnectorResult<()> {
        let location = Self::location(dataset)?;
        let store = store::store();
        store.register(location.repo().clone(), Arc::clone(&self.hub));
        runtime_env.register_object_store(&store::STORE_URL, store as Arc<dyn ObjectStore>);
        Ok(())
    }
}

/// The listing-table machinery for one dataset at one commit: file format and options, schema
/// inference and partition discovery.
#[derive(Debug)]
struct HuggingFaceListing {
    params: Parameters,
    io_runtime: Handle,
    location: DatasetLocation,
    commit: String,
}

impl fmt::Display for HuggingFaceListing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{CONNECTOR_NAME}")
    }
}

impl ListingTableConnector for HuggingFaceListing {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// The dataset's location at the commit, or `schema_source_path`'s at the same commit.
    fn get_object_store_url(
        &self,
        dataset: &DatasetSpec,
        url: Option<&str>,
    ) -> DataConnectorResult<Url> {
        let location = match url {
            None => self.location.clone(),
            Some(url) => DatasetLocation::parse(url)
                .map_err(|source| DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: CONNECTOR_NAME.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    message: source.to_string(),
                })
                .and_then(|location| {
                    if location.repo() == self.location.repo() {
                        Ok(location)
                    } else {
                        Err(DataConnectorError::InvalidConfigurationNoSource {
                            dataconnector: CONNECTOR_NAME.to_string(),
                            connector_component: ConnectorComponent::from(dataset),
                            message: format!(
                                "`schema_source_path` must name a path in dataset '{}', the dataset `from` reads",
                                self.location.repo()
                            ),
                        })
                    }
                })?,
        };
        table::listing_url(&location, &self.commit)
            .map(|url| {
                <datafusion::datasource::listing::ListingTableUrl as AsRef<Url>>::as_ref(&url)
                    .clone()
            })
            .map_err(
                |source| DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: CONNECTOR_NAME.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(source),
                },
            )
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_tokio_io_runtime(&self) -> Handle {
        self.io_runtime.clone()
    }

    fn get_session_context(&self) -> SessionContext {
        let ctx = SessionContext::new_with_config_rt(
            runtime_datafusion::session_config::get_df_default_config().set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            ),
            runtime_object_store::registry::default_runtime_env(self.io_runtime.clone()),
        );
        ctx.runtime_env()
            .register_object_store(&store::STORE_URL, store::store() as Arc<dyn ObjectStore>);
        ctx
    }

    fn get_object_store(
        &self,
        _dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn ObjectStore>> {
        Ok(store::store() as Arc<dyn ObjectStore>)
    }

    fn handle_object_store_error(
        &self,
        dataset: &DatasetSpec,
        error: object_store::Error,
    ) -> DataConnectorError {
        let dataconnector = CONNECTOR_NAME.to_string();
        let connector_component = ConnectorComponent::from(dataset);
        match error {
            object_store::Error::Unauthenticated { source, .. }
            | object_store::Error::PermissionDenied { source, .. } => {
                DataConnectorError::InsufficientPermissions {
                    dataconnector,
                    connector_component,
                    source,
                }
            }
            object_store::Error::NotFound { source, .. } => {
                DataConnectorError::UnableToGetReadProvider {
                    dataconnector,
                    connector_component,
                    source,
                }
            }
            error => DataConnectorError::UnableToConnectInternal {
                dataconnector,
                connector_component,
                source: error.into(),
            },
        }
    }
}

/// Returns a new instance of the Hugging Face connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    HuggingFaceFactory::new_arc()
}

// Self-register into `data-connector-api`'s linkme `DATA_CONNECTOR_REGISTRATIONS` slice. Any
// binary/tool that should see this connector must force-link the crate
// (`use connector_huggingface as _;`) -- a plain Cargo dependency won't link the slice static.
// See `register_data_connector!` docs.
data_connector_api::register_data_connector!(
    register_huggingface_connector,
    HUGGINGFACE_CONNECTOR_REGISTRATION,
    CONNECTOR_NAME,
    HuggingFaceFactory
);
