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

//! SharePoint/OneDrive data connector.
//!
//! Two routing modes are selected by the dataset's `from:` URL:
//!
//! - **Legacy / metadata listing**: `from: sharepoint:me/root` or
//!   `from: sharepoint:driveId:{id}/path:{path}`. Returns a
//!   [`data_components::sharepoint::table::SharepointTableProvider`] — one
//!   row per drive item with optional file-content column. Good for PDF/PPTX
//!   metadata workflows where each row represents a document.
//!
//! - **Object-store tabular / blob**: `from: sharepoint://me/Documents/...`.
//!   Delegates to [`SharepointListingConnector`] which implements
//!   [`runtime::dataconnector::listing::ListingTableConnector`]. DataFusion's
//!   `ListingTable` provides `SELECT`, `INSERT INTO`, `COPY TO`, `COPY FROM`
//!   for CSV/JSON/Parquet; binary formats (PDF, PPTX, etc.) go through the
//!   `ObjectStore` as raw bytes. Writes create new versions by default —
//!   configurable via `sharepoint_conflict_behavior`.

#![expect(
    clippy::doc_markdown,
    reason = "prose-frequent identifiers (SharePoint, DataFusion, OneDrive) are clearer without backticks"
)]

use async_trait::async_trait;
use data_components::sharepoint::auth::{SharepointAuth, saml::SamlBearerConfig};
use data_components::sharepoint::client::SharepointClient;
use data_components::sharepoint::object_store::{
    ConflictBehavior, DriveKind, SharepointObjectStore, SharepointObjectStoreConfig,
};
use data_components::sharepoint::table::SharepointTableProvider;
use data_components::sharepoint::url::DriveRef;
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::parquet::arrow::async_reader::ObjectVersionType;
use document_parse::DocumentParser;
use graph_rs_sdk::GraphClient;
use runtime::Runtime;
use runtime::component::dataset::Dataset;
use runtime::dataconnector::listing::ListingTableConnector;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime::parameters::{ParameterSpec, Parameters};
use secrecy::SecretString;
use snafu::{ResultExt, Snafu};
use std::any::Any;
use std::fmt::{self, Display};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

/// Name used to identify this connector in configuration (the `<name>:` prefix
/// in `from:` values and in the factory registry).
pub const CONNECTOR_NAME: &str = "sharepoint";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: {parameter}. Specify a value. For details, visit: https://spiceai.org/docs/components/data-connectors/sharepoint#parameters"
    ))]
    MissingParameter { parameter: String },

    #[snafu(display(
        "No authentication was specified. Provide one of: client_secret, bearer_token, auth_code, refresh_token, device_code, or saml_assertion. For details, visit: https://spiceai.org/docs/components/data-connectors/sharepoint#auth"
    ))]
    InvalidAuthentication,

    #[snafu(display(
        "Multiple authentication methods were specified. Provide exactly one of: client_secret, bearer_token, auth_code, refresh_token, device_code, or saml_assertion. For details, visit: https://spiceai.org/docs/components/data-connectors/sharepoint#auth"
    ))]
    DuplicateAuthentication,

    #[snafu(display("Failed to build GraphClient: {source}"))]
    AuthBuild {
        source: data_components::sharepoint::auth::Error,
    },

    #[snafu(display(
        "Invalid sharepoint_conflict_behavior '{value}' — expected 'replace', 'fail', or 'rename'. Defaults to 'replace' (creates a new version on overwrite)."
    ))]
    InvalidConflictBehavior { value: String },

    #[snafu(display(
        "Invalid sharepoint_max_put_bytes '{value}' — expected an unsigned integer (bytes)."
    ))]
    InvalidMaxPutBytes { value: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Top-level SharePoint connector. Owns a shared [`GraphClient`] and dispatches
/// `from:` URLs to either the legacy [`SharepointTableProvider`] or a
/// [`SharepointListingConnector`] wrapping the object-store path.
pub struct Sharepoint {
    client: Arc<GraphClient>,
    params: Parameters,
    tokio_io_runtime: tokio::runtime::Handle,
    runtime: Option<Runtime>,
}

impl fmt::Debug for Sharepoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sharepoint")
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

impl Display for Sharepoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(CONNECTOR_NAME)
    }
}

impl Sharepoint {
    async fn new(
        params: Parameters,
        tokio_io_runtime: tokio::runtime::Handle,
        runtime: Option<Runtime>,
    ) -> Result<Self> {
        let auth = build_auth_from_params(&params)?;
        let client = auth.build_graph_client().await.context(AuthBuildSnafu)?;
        Ok(Self {
            client,
            params,
            tokio_io_runtime,
            runtime,
        })
    }

    async fn get_formatter(&self, dataset: &Dataset) -> Option<Arc<dyn DocumentParser>> {
        let file_format = dataset.params.get("file_format")?;
        document_parse::get_parser_factory(file_format)
            .await
            .map(|factory| factory.default())
    }

    /// Whether this dataset uses the new `sharepoint://` URL scheme (routed
    /// through `ObjectStore` + `ListingTable`) or the legacy compact
    /// `sharepoint:…` syntax (routed to [`SharepointTableProvider`]).
    ///
    /// URL schemes are case-insensitive, so we parse and compare on scheme
    /// and authority rather than a raw prefix match — `SharePoint://me/…`
    /// should route the same as `sharepoint://me/…`.
    fn uses_object_store(dataset: &Dataset) -> bool {
        match Url::parse(&dataset.from) {
            Ok(u) => u.scheme().eq_ignore_ascii_case(CONNECTOR_NAME) && u.has_authority(),
            Err(_) => false,
        }
    }

    /// Build the helper connector that backs `sharepoint://` datasets.
    ///
    /// The blanket [`ListingTableConnector::read_provider`] impl creates a
    /// fresh [`SessionContext`] whose [`RuntimeEnv`] doesn't know how to
    /// build a `sharepoint://` store from a URL, so we hand the helper
    /// connector everything it needs to register a [`SharepointObjectStore`]
    /// on that fresh env (see its `get_session_context` /
    /// `get_object_store` overrides).
    fn listing_connector(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<SharepointListingConnector> {
        let (store_url, kind, config) = parse_object_store_components(&self.params, dataset)?;
        Ok(SharepointListingConnector {
            client: Arc::clone(&self.client),
            store_url,
            kind,
            config,
            params: self.params.clone(),
            tokio_io_runtime: self.tokio_io_runtime.clone(),
            runtime: self.runtime.clone(),
        })
    }
}

/// Register a [`SharepointObjectStore`] on `runtime_env` under the
/// scheme+authority key DataFusion's registry actually looks up.
///
/// `ListingTableUrl::object_store()` returns the canonical registry key
/// (e.g. `sharepoint://drives/`), with the path stripped. Registering
/// under the full dataset URL would prevent re-use across paths and
/// cause lookups for the same authority+different path to miss.
fn register_sharepoint_store(
    runtime_env: &Arc<RuntimeEnv>,
    store_url: &Url,
    store: Arc<SharepointObjectStore>,
) {
    use datafusion::datasource::listing::ListingTableUrl;
    match ListingTableUrl::parse(store_url.as_str()) {
        Ok(ltu) => {
            runtime_env.register_object_store(ltu.object_store().as_ref(), store);
        }
        Err(e) => {
            // `store_url` was already validated as a parseable URL upstream
            // (see `parse_object_store_components`), so this branch is a
            // defensive fallback. Register under the full URL so the caller
            // still gets a working store.
            tracing::warn!(
                store_url = %store_url,
                error = %e,
                "Failed to derive ObjectStore registry key from SharePoint URL; registering under full URL"
            );
            runtime_env.register_object_store(store_url, store);
        }
    }
}

/// Parse the dataset URL and connector params into the components needed to
/// build a [`SharepointObjectStore`]. Used by both
/// [`Sharepoint::register_object_stores`] and
/// [`Sharepoint::listing_connector`] so the two paths agree on URL,
/// drive-kind routing, and config.
fn parse_object_store_components(
    params: &Parameters,
    dataset: &Dataset,
) -> DataConnectorResult<(Url, Option<DriveKind>, SharepointObjectStoreConfig)> {
    let store_url =
        Url::parse(&dataset.from).map_err(|e| DataConnectorError::InvalidConfiguration {
            dataconnector: CONNECTOR_NAME.to_string(),
            message: format!(
                "'{}' is not a valid sharepoint:// URL. See https://spiceai.org/docs/components/data-connectors/sharepoint#from",
                dataset.from
            ),
            connector_component: ConnectorComponent::from(dataset),
            source: Box::new(e),
        })?;
    let sp_url =
        data_components::sharepoint::url::SharepointUrl::from_url(&store_url).map_err(|e| {
            DataConnectorError::InvalidConfiguration {
                dataconnector: CONNECTOR_NAME.to_string(),
                message: format!("{e}"),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            }
        })?;
    let kind = match sp_url.drive {
        DriveRef::Me => None,
        DriveRef::Drive(_) => Some(DriveKind::Drives),
        DriveRef::Site(_) => Some(DriveKind::Sites),
        DriveRef::User(_) => Some(DriveKind::Users),
        DriveRef::Group(_) => Some(DriveKind::Groups),
    };
    let conflict_behavior =
        parse_conflict_behavior(params).map_err(|e| DataConnectorError::InvalidConfiguration {
            dataconnector: CONNECTOR_NAME.to_string(),
            message: format!("{e}"),
            connector_component: ConnectorComponent::from(dataset),
            source: Box::new(e),
        })?;
    let max_put_bytes =
        parse_max_put_bytes(params).map_err(|e| DataConnectorError::InvalidConfiguration {
            dataconnector: CONNECTOR_NAME.to_string(),
            message: format!("{e}"),
            connector_component: ConnectorComponent::from(dataset),
            source: Box::new(e),
        })?;
    Ok((
        store_url,
        kind,
        SharepointObjectStoreConfig {
            conflict_behavior,
            max_put_bytes,
        },
    ))
}

fn build_auth_from_params(params: &Parameters) -> Result<SharepointAuth> {
    let tenant = params.get("tenant_id").expose().ok().map(String::from);
    let client_id = params.get("client_id").expose().ok().map(String::from);

    let client_secret = params.get("client_secret").expose().ok();
    let bearer_token = params.get("bearer_token").expose().ok();
    let auth_code = params.get("auth_code").expose().ok();
    let refresh_token = params.get("refresh_token").expose().ok();
    let device_code = params.get("device_code").expose().ok();
    let saml_assertion = params.get("saml_assertion").expose().ok();

    // Exactly one top-level flow must be selected. `client_secret` alone
    // implies client credentials, but `client_secret` combined with
    // `auth_code` or `refresh_token` represents a single auth-code /
    // refresh-token flow (secret acts as the app credential, not a
    // separate flow). Any other combination is ambiguous.
    let client_secret_as_flow =
        client_secret.is_some() && auth_code.is_none() && refresh_token.is_none();
    let flow_count = [
        client_secret_as_flow,
        bearer_token.is_some(),
        auth_code.is_some(),
        refresh_token.is_some(),
        device_code.is_some(),
        saml_assertion.is_some(),
    ]
    .iter()
    .filter(|b| **b)
    .count();

    if flow_count == 0 {
        return Err(Error::InvalidAuthentication);
    }
    if flow_count > 1 {
        return Err(Error::DuplicateAuthentication);
    }

    let scope = params.get("scope").expose().ok().map(String::from);

    if let Some(token) = bearer_token {
        return Ok(SharepointAuth::BearerToken(SecretString::new(
            token.to_string().into(),
        )));
    }
    if let Some(assertion) = saml_assertion {
        let tenant = tenant.ok_or_else(|| Error::MissingParameter {
            parameter: "tenant_id".into(),
        })?;
        let client_id = client_id.ok_or_else(|| Error::MissingParameter {
            parameter: "client_id".into(),
        })?;
        return Ok(SharepointAuth::SamlBearer(SamlBearerConfig {
            tenant_id: tenant,
            client_id,
            assertion: SecretString::new(assertion.to_string().into()),
            scope,
            authority_host_override: None,
        }));
    }

    let tenant = tenant.ok_or_else(|| Error::MissingParameter {
        parameter: "tenant_id".into(),
    })?;
    let client_id = client_id.ok_or_else(|| Error::MissingParameter {
        parameter: "client_id".into(),
    })?;

    if let Some(code) = auth_code {
        let secret = client_secret.ok_or_else(|| Error::MissingParameter {
            parameter: "client_secret (required with auth_code)".into(),
        })?;
        let redirect = params
            .get("redirect_uri")
            .expose()
            .ok()
            .ok_or_else(|| Error::MissingParameter {
                parameter: "redirect_uri (required with auth_code)".into(),
            })?
            .to_string();
        return Ok(SharepointAuth::AuthCode {
            tenant_id: tenant,
            client_id,
            client_secret: SecretString::new(secret.to_string().into()),
            auth_code: SecretString::new(code.to_string().into()),
            redirect_uri: redirect,
            scope,
        });
    }
    if let Some(token) = refresh_token {
        let secret = client_secret.ok_or_else(|| Error::MissingParameter {
            parameter: "client_secret (required with refresh_token)".into(),
        })?;
        return Ok(SharepointAuth::RefreshToken {
            tenant_id: tenant,
            client_id,
            client_secret: SecretString::new(secret.to_string().into()),
            refresh_token: SecretString::new(token.to_string().into()),
            scope,
        });
    }
    if let Some(dc) = device_code {
        return Ok(SharepointAuth::DeviceCode {
            tenant_id: tenant,
            client_id,
            device_code: SecretString::new(dc.to_string().into()),
            scope,
        });
    }
    // Must be client_credentials (only remaining authenticated form).
    let secret = client_secret.ok_or(Error::InvalidAuthentication)?;
    Ok(SharepointAuth::ClientCredentials {
        tenant_id: tenant,
        client_id,
        client_secret: SecretString::new(secret.to_string().into()),
        scope,
    })
}

fn parse_conflict_behavior(params: &Parameters) -> Result<ConflictBehavior> {
    match params.get("conflict_behavior").expose().ok() {
        None => Ok(ConflictBehavior::default()),
        Some(v) => ConflictBehavior::from_str(v).map_err(|_| Error::InvalidConflictBehavior {
            value: v.to_string(),
        }),
    }
}

/// Parse `sharepoint_max_put_bytes` from connector params, falling back to
/// `SharepointObjectStoreConfig::default().max_put_bytes` when unset.
fn parse_max_put_bytes(params: &Parameters) -> Result<usize> {
    match params.get("max_put_bytes").expose().ok() {
        None => Ok(SharepointObjectStoreConfig::default().max_put_bytes),
        Some(v) => v.parse::<usize>().map_err(|_| Error::InvalidMaxPutBytes {
            value: v.to_string(),
        }),
    }
}

#[derive(Default, Copy, Clone)]
pub struct SharepointFactory {}

impl SharepointFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    // Identity / tenant
    ParameterSpec::component("client_id").secret().required(),
    ParameterSpec::component("tenant_id").secret().required(),
    // Auth flows (exactly one of these should be set)
    ParameterSpec::component("client_secret").secret(),
    ParameterSpec::component("bearer_token").secret(),
    ParameterSpec::component("auth_code").secret(),
    ParameterSpec::component("refresh_token").secret(),
    ParameterSpec::component("device_code").secret(),
    ParameterSpec::component("saml_assertion").secret(),
    ParameterSpec::component("redirect_uri"),
    ParameterSpec::component("scope"),
    // Write behavior
    ParameterSpec::component("conflict_behavior")
        .description("How to handle writes to an existing path: 'replace' (default; creates a new SharePoint version), 'fail' (reject), or 'rename' (write under a unique name)."),
    ParameterSpec::component("max_put_bytes")
        .description("Hard cap (in bytes) on the size of a single put/multipart upload. Writes above this limit are rejected rather than silently buffered. Default: 1 GiB."),
    // Legacy / shared
    ParameterSpec::runtime("file_format"),
];

impl DataConnectorFactory for SharepointFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let io_runtime = params.io_runtime.clone();
            let runtime = params.runtime.clone().map(Arc::unwrap_or_clone);
            let connector = Sharepoint::new(params.parameters, io_runtime, runtime).await?;
            Ok(Arc::new(connector) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        CONNECTOR_NAME
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Sharepoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        if Self::uses_object_store(dataset) {
            return self
                .listing_connector(dataset)?
                .read_provider(dataset)
                .await;
        }
        // Legacy path — metadata-listing table provider.
        let client = SharepointClient::new(Arc::clone(&self.client), &dataset.from)
            .await
            .boxed()
            .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            })?;
        Ok(Arc::new(SharepointTableProvider::new(
            client,
            true,
            self.get_formatter(dataset).await,
        )))
    }

    async fn metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        if !dataset.has_metadata_table {
            return None;
        }
        if Self::uses_object_store(dataset) {
            return match self.listing_connector(dataset) {
                Ok(c) => c.metadata_provider(dataset).await,
                Err(e) => Some(Err(e)),
            };
        }
        let result = SharepointClient::new(Arc::clone(&self.client), &dataset.from)
            .await
            .boxed()
            .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            });
        Some(match result {
            Ok(client) => Ok(Arc::new(SharepointTableProvider::new(
                client,
                false,
                self.get_formatter(dataset).await,
            ))),
            Err(e) => Err(e),
        })
    }

    async fn register_object_stores(
        &self,
        dataset: &Dataset,
        runtime_env: &Arc<RuntimeEnv>,
    ) -> DataConnectorResult<()> {
        if !Self::uses_object_store(dataset) {
            return Ok(());
        }
        // Pre-register the SharepointObjectStore under the dataset's URL.
        // DataFusion's registry keys on scheme+authority, so all
        // `sharepoint://drives/…` (or `…/sites/…`, etc.) datasets share a
        // single registered store — the store reads the drive ID from the
        // first path segment on every operation, rather than binding one
        // drive per store instance. `sharepoint://me` is the one special
        // case where the drive is fixed.
        let (store_url, kind, config) = parse_object_store_components(&self.params, dataset)?;
        let store = Arc::new(SharepointObjectStore::new(
            Arc::clone(&self.client),
            kind,
            config,
        ));
        register_sharepoint_store(runtime_env, &store_url, store);
        // Then defer to the listing connector for any additional setup.
        self.listing_connector(dataset)?
            .register_object_stores(dataset, runtime_env)
            .await
    }
}

/// Internal helper that implements [`ListingTableConnector`]. Not a public
/// connector (not registered in the connector factory); instantiated on
/// demand by [`Sharepoint`] for `sharepoint://` datasets.
///
/// Carries the authenticated [`GraphClient`] and parsed drive routing so
/// that the `get_session_context` / `get_object_store` overrides can
/// register a [`SharepointObjectStore`] on the freshly built session
/// context. Without that, the blanket [`ListingTableConnector`] impl
/// falls back to `SpiceObjectStoreRegistry`, which doesn't know how to
/// build `sharepoint://` stores and would fail at schema inference.
struct SharepointListingConnector {
    client: Arc<GraphClient>,
    store_url: Url,
    kind: Option<DriveKind>,
    config: SharepointObjectStoreConfig,
    params: Parameters,
    tokio_io_runtime: tokio::runtime::Handle,
    runtime: Option<Runtime>,
}

impl fmt::Debug for SharepointListingConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharepointListingConnector")
            .finish_non_exhaustive()
    }
}

impl Display for SharepointListingConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(CONNECTOR_NAME)
    }
}

impl SharepointListingConnector {
    fn build_object_store(&self) -> Arc<SharepointObjectStore> {
        Arc::new(SharepointObjectStore::new(
            Arc::clone(&self.client),
            self.kind,
            self.config,
        ))
    }
}

impl ListingTableConnector for SharepointListingConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_tokio_io_runtime(&self) -> tokio::runtime::Handle {
        self.tokio_io_runtime.clone()
    }

    fn get_runtime(&self) -> Option<Runtime> {
        self.runtime.clone()
    }

    fn object_versioning_type(&self) -> Option<ObjectVersionType> {
        Some(ObjectVersionType::Version)
    }

    fn get_object_store_url(
        &self,
        dataset: &Dataset,
        url: Option<&str>,
    ) -> DataConnectorResult<Url> {
        let url_str = url.unwrap_or(dataset.from.as_str());
        Url::parse(url_str).map_err(|e| DataConnectorError::InvalidConfiguration {
            dataconnector: CONNECTOR_NAME.to_string(),
            message: format!(
                "'{url_str}' is not a valid sharepoint:// URL. See https://spiceai.org/docs/components/data-connectors/sharepoint#from"
            ),
            connector_component: ConnectorComponent::from(dataset),
            source: Box::new(e),
        })
    }

    /// Override the default to short-circuit the registry lookup — we
    /// already know how to build a [`SharepointObjectStore`] for this
    /// dataset, and `SpiceObjectStoreRegistry` doesn't.
    fn get_object_store(
        &self,
        _dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn datafusion::object_store::ObjectStore>> {
        Ok(self.build_object_store())
    }

    /// Override the default to register the [`SharepointObjectStore`] on
    /// the freshly created [`SessionContext`]'s runtime env. Schema
    /// inference (`ListingOptions::infer_schema`) reaches into
    /// `ctx.state().runtime_env()` to resolve the store, so without this
    /// the blanket [`ListingTableConnector::read_provider`] impl would
    /// fail on first listing.
    fn get_session_context(&self) -> datafusion::execution::context::SessionContext {
        use datafusion::execution::context::SessionContext;
        use runtime_object_store::registry::default_runtime_env;

        let mut config = runtime::datafusion::builder::DEFAULT_DATAFUSION_CONFIG
            .read()
            .map_or_else(|_| datafusion::prelude::SessionConfig::new(), |c| c.clone());
        config
            .options_mut()
            .execution
            .listing_table_ignore_subdirectory = false;
        let ctx = SessionContext::new_with_config_rt(
            config,
            default_runtime_env(self.tokio_io_runtime.clone()),
        );
        register_sharepoint_store(
            &ctx.runtime_env(),
            &self.store_url,
            self.build_object_store(),
        );
        ctx
    }
}

/// Entry point used by the factory registry in `bin/spiced`.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    SharepointFactory::new_arc()
}
