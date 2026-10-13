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
use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Weak};

use async_trait::async_trait;
use data_connector_api::listing::{
    LISTING_TABLE_PARAMETERS, ListingTableConnector, detect_file_extension_from_path,
    detect_file_extension_from_url_or_path, file_matches_extension,
};
use data_connector_api::{
    ConnectorComponent, ConnectorContext, ConnectorParams, DataConnector, DataConnectorError,
    DataConnectorFactory, DataConnectorResult, NewDataConnectorResult,
};
use datafusion::config::CsvOptions;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::{
    csv::CsvFormat, file_compression_type::FileCompressionType,
};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::{FutureExt, StreamExt, TryStreamExt};
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
use store::HuggingFaceStore;
use table::{CommitCheck, HuggingFaceTable};

// `register_data_connector!` names `linkme` unqualified.
use data_connector_api::linkme;

/// The name used to identify this connector in configuration: `from: hf://...`.
pub const CONNECTOR_NAME: &str = "hf";
const DOCS_URL: &str =
    "https://github.com/spiceai/spiceai/blob/trunk/docs/features/huggingface-connector.md";

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

/// A CSV or TSV file that cannot be read with the dataset's columns.
#[derive(Debug, Snafu)]
enum ColumnsError {
    #[snafu(display(
        "File '{file}' of Hugging Face dataset '{repo}' at commit {commit} has columns ({found}), but the dataset's columns are ({columns}). CSV and TSV files are read by column position, so the file cannot be read with the dataset's columns. Narrow `from` to files with the dataset's columns, or restart Spice to register the columns of a dataset that changed. See: {DOCS_URL}"
    ))]
    Differ {
        repo: location::RepoId,
        commit: String,
        file: String,
        found: String,
        columns: String,
    },

    #[snafu(display(
        "Failed to read the header of '{file}' of Hugging Face dataset '{repo}' at commit {commit}, so its columns cannot be checked against the dataset's: {reason}"
    ))]
    Unreadable {
        repo: location::RepoId,
        commit: String,
        file: String,
        reason: String,
    },
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

/// The store reading the public Hub without a token, shared by every dataset that does.
static PUBLIC_STORE: LazyLock<Mutex<Weak<HuggingFaceStore>>> =
    LazyLock::new(|| Mutex::new(Weak::new()));

/// The store `component` reads with `config`: the shared public store, or one of its own (see
/// [`store::store_url`]), so datasets read with different tokens or endpoints never share a
/// client.
fn store_for(
    config: HubConfig,
    component: &str,
    io_runtime: Handle,
) -> Result<Arc<HuggingFaceStore>, hub::Error> {
    if !store::is_public(&config) {
        let hub = Arc::new(Hub::new(config, io_runtime)?);
        return Ok(Arc::new(HuggingFaceStore::new(hub, component)));
    }
    let mut public = PUBLIC_STORE.lock();
    if let Some(store) = public.upgrade() {
        return Ok(store);
    }
    let hub = Arc::new(Hub::new(config, io_runtime)?);
    let store = Arc::new(HuggingFaceStore::new(hub, component));
    *public = Arc::downgrade(&store);
    Ok(store)
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
            let component = match &params.component {
                ConnectorComponent::Dataset(dataset) => dataset.name.to_string(),
                ConnectorComponent::Catalog(catalog) => catalog.name.clone(),
            };
            let store = store_for(
                HubConfig { endpoint, token },
                &component,
                params.io_runtime.clone(),
            )
            .context(HubSnafu)?;
            Ok(Arc::new(HuggingFace {
                params: params.parameters,
                store,
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
    store: Arc<HuggingFaceStore>,
    io_runtime: Handle,
}

impl fmt::Debug for HuggingFace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HuggingFace")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for HuggingFace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{CONNECTOR_NAME}")
    }
}

impl HuggingFace {
    fn hub(&self) -> &Arc<Hub> {
        self.store.hub()
    }

    /// The dataset's table: the files `from` selects, read at the commit each scan resolves.
    async fn table(&self, dataset: &DatasetSpec) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let location = Self::location(dataset)?;
        // Resolving the revision first checks that the dataset exists and can be read, and
        // pins the schema to one commit.
        let commit = self
            .hub()
            .commit(location.repo(), location.revision())
            .await
            .map_err(|source| hub_error(dataset, source))?;

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

        let listing_url =
            table::listing_url(self.store.url(), &location, &commit.sha).map_err(|source| {
                DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: CONNECTOR_NAME.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(source),
                }
            })?;
        let listing = HuggingFaceListing {
            params,
            io_runtime: self.io_runtime.clone(),
            store: Arc::clone(&self.store),
            location: location.clone(),
            commit: commit.sha.clone(),
        };

        // The format comes from the path inside the repository, never from the revision or
        // the dataset's name.
        let mut format_view = dataset.clone();
        format_view.from = location.path_url();
        let (file_format, extension) = listing.get_file_format_and_extension(&format_view).await?;
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
        let files = selected_files(self.hub(), dataset, &location, &commit.sha, &extension).await?;
        // The repository API answers for a gated dataset without access; only its files refuse.
        // Reading a byte of one selected file now fails registration with the Hub's reason,
        // rather than a schema-inference failure that would be retried.
        if let Some(path) = files.last() {
            self.hub()
                .read(
                    location.repo(),
                    &commit.sha,
                    path,
                    Some(object_store::GetRange::Bounded(0..1)),
                )
                .await
                .map_err(|source| hub_error(dataset, source))?;
        }
        let schema_dataset = schema_dataset(dataset, &location, &commit.sha, &files);
        let template = listing
            .listing_table_template(
                &schema_dataset,
                listing_url.as_ref(),
                &extension,
                Arc::clone(&file_format),
            )
            .await?;

        // CSV and TSV are read by column position: every file must have the dataset's columns,
        // in order, at registration and at every commit a moving revision reaches.
        let commit_check = match (file_format.as_ref() as &dyn Any).downcast_ref::<CsvFormat>() {
            Some(csv) => {
                let check = PositionalCheck {
                    store: Arc::clone(&self.store),
                    dataset: dataset.clone(),
                    location: location.clone(),
                    extension: extension.clone(),
                    options: csv.options().clone(),
                    columns: column_names(template.file_schema()),
                };
                check.verify(&commit.sha, &files).await.map_err(|error| {
                    DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: CONNECTOR_NAME.to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        message: error.to_string(),
                    }
                })?;
                Some(check.into_commit_check())
            }
            None => None,
        };
        let table = HuggingFaceTable::try_new(
            location,
            Arc::clone(&self.store),
            template,
            commit.sha,
            commit_check,
        )
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
        let from_names_format = detect_file_extension_from_url_or_path(&location.path_url())
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
            .hub()
            .list(location.repo(), &commit.sha, folder, true)
            .await
            .map_err(|source| hub_error(dataset, source))?;

        // Extensions of the selected files, by whether they are a data format.
        let mut data_extensions = BTreeSet::new();
        let mut other_extensions = BTreeSet::new();
        let folder_prefix = if folder.is_empty() {
            String::new()
        } else {
            format!("{folder}/")
        };
        for entry in entries.iter().filter(|entry| entry.kind == EntryKind::File) {
            let Some(relative) = entry.path.strip_prefix(&folder_prefix) else {
                continue;
            };
            if glob.as_ref().is_some_and(|glob| !glob.matches(relative)) {
                continue;
            }
            let extension = detect_file_extension_from_path(&entry.path);
            match extension
                .as_ref()
                .and_then(|e| e.format_extension.as_deref())
            {
                Some(format) if INFERABLE_FORMATS.contains(&format) => {
                    let extension = extension.as_ref().map_or("", |e| e.file_extension.as_str());
                    data_extensions.insert(extension.trim_start_matches('.').to_string());
                }
                _ => {
                    other_extensions.insert(extension.map_or_else(
                        || "files without an extension".to_string(),
                        |e| e.file_extension,
                    ));
                }
            }
        }

        let selected = if location.path().is_empty() {
            format!("dataset '{}'", location.repo())
        } else {
            format!("'{}' in dataset '{}'", location.path(), location.repo())
        };
        // A single file without an extension lists nothing: its format must be named.
        if entries.is_empty() && !location.is_folder() && glob.is_none() {
            let entry = self
                .hub()
                .entry(location.repo(), &commit.sha, location.path())
                .await
                .map_err(|source| hub_error(dataset, source))?;
            if entry.is_some_and(|entry| entry.kind == EntryKind::File) {
                return Err(DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: CONNECTOR_NAME.to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    message: format!(
                        "{selected} has no file extension to infer its format from. Set `file_format` to parquet, csv, tsv, json, jsonl or orc. See: {DOCS_URL}"
                    ),
                });
            }
        }
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
                    other_extensions.into_iter().collect::<Vec<_>>().join(", ")
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

/// The dataset as schema inference sees it. A location with a glob infers its schema from a
/// file the glob selects — the listing alone would infer it from any file in the folder —
/// unless the dataset names its own `schema_source_path`.
fn schema_dataset(
    dataset: &DatasetSpec,
    location: &DatasetLocation,
    commit: &str,
    files: &[String],
) -> DatasetSpec {
    let mut dataset = dataset.clone();
    if location.glob().is_some()
        && !dataset.params.contains_key("schema_source_path")
        // The last selected file, as the listing infers from the newest one and every file of
        // a commit has the commit's date.
        && let Some(source) = files.last()
    {
        let repo = location.repo();
        dataset.params.insert(
            "schema_source_path".to_string(),
            format!(
                "{}://datasets/{}/{}@{commit}/{source}",
                location::SCHEME,
                repo.owner(),
                repo.name()
            ),
        );
    }
    dataset
}

/// The data files the location selects at `commit`, sorted: the file itself, or the files of
/// the folder or glob with the listing extension. Empty when it selects none, which schema
/// inference then reports.
async fn selected_files(
    hub: &Hub,
    dataset: &DatasetSpec,
    location: &DatasetLocation,
    commit: &str,
    extension: &str,
) -> DataConnectorResult<Vec<String>> {
    if !location.is_folder() && location.glob().is_none() {
        let entry = hub
            .entry(location.repo(), commit, location.path())
            .await
            .map_err(|source| hub_error(dataset, source))?;
        match entry {
            Some(entry) if entry.kind == EntryKind::File => return Ok(vec![entry.path]),
            // A folder named without a trailing `/` is listed like one.
            Some(_) => {}
            None => return Ok(Vec::new()),
        }
    }
    let (folder, pattern) = match location.glob() {
        Some((folder, glob)) => (folder, glob::Pattern::new(glob).ok()),
        None => (location.path(), None),
    };
    let folder_prefix = if folder.is_empty() {
        String::new()
    } else {
        format!("{folder}/")
    };
    let entries = hub
        .list(location.repo(), commit, folder, true)
        .await
        .map_err(|source| hub_error(dataset, source))?;
    let mut files: Vec<String> = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
        .filter(|entry| {
            entry
                .path
                .strip_prefix(&folder_prefix)
                .is_some_and(|relative| {
                    pattern
                        .as_ref()
                        .is_none_or(|pattern| pattern.matches(relative))
                })
        })
        .filter(|entry| {
            object_store::path::Path::parse(&entry.path)
                .is_ok_and(|path| file_matches_extension(&path, extension))
        })
        .map(|entry| entry.path.clone())
        .collect();
    files.sort();
    Ok(files)
}

/// The bytes read from the start of a CSV or TSV file to find its header.
const MAX_HEADER_BYTES: u64 = 1024 * 1024;
/// Headers read at once when checking a dataset's files.
const HEADER_READ_CONCURRENCY: usize = 16;

/// Finds where the first record of a CSV or TSV file ends, across the chunks of a download: at
/// a record terminator outside quotes, as the CSV reader splits records, so a quoted column
/// name may span lines.
struct RecordScanner {
    quote: u8,
    escape: Option<u8>,
    terminator: Option<u8>,
    in_quotes: bool,
    escaped: bool,
    scanned: usize,
}

impl RecordScanner {
    fn new(quote: u8, escape: Option<u8>, terminator: Option<u8>) -> Self {
        Self {
            quote,
            escape,
            terminator,
            in_quotes: false,
            escaped: false,
            scanned: 0,
        }
    }

    /// The end (exclusive) of the first record within `bytes`, which grows between calls, or
    /// `None` while the record continues past them. Without a configured terminator a record
    /// ends at `\n` or `\r`, as the CSV reader accepts either.
    fn record_end(&mut self, bytes: &[u8]) -> Option<usize> {
        for (index, &byte) in bytes.iter().enumerate().skip(self.scanned) {
            if self.escaped {
                self.escaped = false;
                continue;
            }
            if self.in_quotes && Some(byte) == self.escape && self.escape != Some(self.quote) {
                self.escaped = true;
                continue;
            }
            if byte == self.quote {
                // A doubled quote inside a quoted field toggles twice: still quoted.
                self.in_quotes = !self.in_quotes;
                continue;
            }
            let ends = match self.terminator {
                Some(terminator) => byte == terminator,
                None => byte == b'\n' || byte == b'\r',
            };
            if ends && !self.in_quotes {
                self.scanned = index + 1;
                return Some(index + 1);
            }
        }
        self.scanned = bytes.len();
        None
    }
}

/// Checks that CSV or TSV files have a dataset's columns, in order. Those formats are read by
/// column position, so a file whose columns are reordered or renamed would otherwise put its
/// values under the wrong names.
#[derive(Clone)]
struct PositionalCheck {
    store: Arc<HuggingFaceStore>,
    dataset: DatasetSpec,
    location: DatasetLocation,
    extension: String,
    options: CsvOptions,
    columns: Vec<String>,
}

impl PositionalCheck {
    /// Reads the header of each file and compares it with the dataset's columns.
    async fn verify(&self, commit: &str, files: &[String]) -> Result<(), Box<ColumnsError>> {
        if self.options.has_header == Some(false) {
            // Without a header every file is positional by definition: nothing to compare.
            return Ok(());
        }
        let headers: Vec<(String, Result<Vec<String>, String>)> =
            futures::stream::iter(files.iter().cloned())
                .map(|file| async move {
                    let header = self.header(commit, &file).await;
                    (file, header)
                })
                .buffered(HEADER_READ_CONCURRENCY)
                .collect()
                .await;
        for (file, header) in headers {
            let found = header.map_err(|reason| {
                Box::new(ColumnsError::Unreadable {
                    repo: self.location.repo().clone(),
                    commit: commit.to_string(),
                    file: file.clone(),
                    reason,
                })
            })?;
            if found != self.columns {
                return Err(Box::new(ColumnsError::Differ {
                    repo: self.location.repo().clone(),
                    commit: commit.to_string(),
                    file,
                    found: found.join(", "),
                    columns: self.columns.join(", "),
                }));
            }
        }
        Ok(())
    }

    /// The column names in the header of `file` at `commit`.
    async fn header(&self, commit: &str, file: &str) -> Result<Vec<String>, String> {
        let repo = self.location.repo();
        let path = object_store::path::Path::parse(format!(
            "{}/{}@{commit}/{file}",
            repo.owner(),
            repo.name()
        ))
        .map_err(|e| e.to_string())?;
        let read = self
            .store
            .get_opts(
                &path,
                object_store::GetOptions {
                    range: Some(object_store::GetRange::Bounded(0..MAX_HEADER_BYTES)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        let stream = read.into_stream().map_err(DataFusionError::from).boxed();
        let mut decoded = FileCompressionType::from(self.options.compression)
            .convert_stream(stream)
            .map_err(|e| e.to_string())?;
        let mut scanner = RecordScanner::new(
            self.options.quote,
            self.options.escape,
            self.options.terminator,
        );
        let mut bytes = Vec::new();
        let mut complete = false;
        while let Some(chunk) = decoded.next().await {
            let chunk = chunk.map_err(|e| e.to_string())?;
            bytes.extend_from_slice(&chunk);
            if let Some(end) = scanner.record_end(&bytes) {
                bytes.truncate(end);
                complete = true;
                break;
            }
        }
        if !complete && bytes.len() as u64 >= MAX_HEADER_BYTES {
            return Err(format!(
                "its first record is longer than {MAX_HEADER_BYTES} bytes"
            ));
        }
        let mut format = datafusion::arrow::csv::reader::Format::default()
            .with_header(true)
            .with_delimiter(self.options.delimiter)
            .with_quote(self.options.quote);
        if let Some(escape) = self.options.escape {
            format = format.with_escape(escape);
        }
        if let Some(terminator) = self.options.terminator {
            format = format.with_terminator(terminator);
        }
        let (schema, _) = format
            .infer_schema(std::io::Cursor::new(bytes), Some(0))
            .map_err(|e| e.to_string())?;
        Ok(column_names(&Arc::new(schema)))
    }

    fn into_commit_check(self) -> CommitCheck {
        Arc::new(move |commit: String| {
            let check = self.clone();
            async move {
                let files = selected_files(
                    check.store.hub(),
                    &check.dataset,
                    &check.location,
                    &commit,
                    &check.extension,
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                check
                    .verify(&commit, &files)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            }
            .boxed()
        })
    }
}

fn column_names(schema: &datafusion::arrow::datatypes::SchemaRef) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

/// Maps a Hub error during dataset registration to the connector error a user acts on.
fn hub_error(dataset: &DatasetSpec, source: hub::Error) -> DataConnectorError {
    let dataconnector = CONNECTOR_NAME.to_string();
    let connector_component = ConnectorComponent::from(dataset);
    match source {
        hub::Error::RepoNotFound { .. }
        | hub::Error::RevisionNotFound { .. }
        | hub::Error::InvalidToken
        | hub::Error::InvalidEndpoint { .. } => DataConnectorError::InvalidConfigurationNoSource {
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
        // The physical plans an executor runs name objects under the store's URL.
        Self::location(dataset)?;
        runtime_env.register_object_store(
            self.store.url(),
            Arc::clone(&self.store) as Arc<dyn ObjectStore>,
        );
        Ok(())
    }
}

/// The listing-table machinery for one dataset at one commit: file format and options, schema
/// inference and partition discovery.
#[derive(Debug, Clone)]
struct HuggingFaceListing {
    params: Parameters,
    io_runtime: Handle,
    store: Arc<HuggingFaceStore>,
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
        table::listing_url(self.store.url(), &location, &self.commit)
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
        ctx.runtime_env().register_object_store(
            self.store.url(),
            Arc::clone(&self.store) as Arc<dyn ObjectStore>,
        );
        ctx
    }

    fn get_object_store(
        &self,
        _dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn ObjectStore>> {
        Ok(Arc::clone(&self.store) as Arc<dyn ObjectStore>)
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

#[cfg(test)]
mod record_scanner_tests {
    use super::RecordScanner;

    fn end(bytes: &[u8], escape: Option<u8>, terminator: Option<u8>) -> Option<usize> {
        RecordScanner::new(b'"', escape, terminator).record_end(bytes)
    }

    #[test]
    fn the_first_record_ends_at_a_terminator_outside_quotes() {
        assert_eq!(end(b"id,name\n1,a\n", None, None), Some(8));
        assert_eq!(end(b"id,name\r\n1,a\r\n", None, None), Some(8));
        // A quoted column name spanning lines, and a doubled quote inside one.
        assert_eq!(end(b"id,\"first\nname\"\n1,a\n", None, None), Some(16));
        assert_eq!(
            end(b"id,\"say \"\"hi\"\"\nthere\"\n1\n", None, None),
            Some(22)
        );
        // An escaped quote does not close the field.
        assert_eq!(end(b"id,\"a\\\"\nb\"\n1\n", Some(b'\\'), None), Some(11));
        // A configured terminator.
        assert_eq!(end(b"id,name|1,a|", None, Some(b'|')), Some(8));
        assert_eq!(end(b"id,name\nmore", None, Some(b'|')), None);
        assert_eq!(end(b"id,\"open", None, None), None);
    }

    #[test]
    fn the_scan_continues_across_chunks() {
        let mut scanner = RecordScanner::new(b'"', None, None);
        let mut bytes = b"id,\"first".to_vec();
        assert_eq!(scanner.record_end(&bytes), None);
        bytes.extend_from_slice(b"\nname\",x");
        assert_eq!(scanner.record_end(&bytes), None);
        bytes.extend_from_slice(b"\n1,2,3\n");
        assert_eq!(scanner.record_end(&bytes), Some(18));
    }
}
