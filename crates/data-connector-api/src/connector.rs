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

//! The `DataConnector` contract itself: what a connector can do, the factory
//! that builds one, and the link-time registration that makes it discoverable.

use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::cdc::{AccelerationContents, ChangesStream};
use datafusion::datasource::TableProvider;
use linkme::distributed_slice;
use runtime_component::ComponentInitialization;
use runtime_component::dataset::DatasetSpec;
use runtime_component::dataset::acceleration::RefreshMode;
use runtime_metrics::component::MetricsProvider;
use runtime_parameters::ParameterSpec;

use crate::accelerated::{AcceleratorSetup, RegisteredAcceleratedTable};
use crate::federated::FederatedTableProvider;
use crate::parameters::{ConnectorContext, ConnectorParams};
use crate::{AnyErrorResult, DataConnectorResult};

pub type NewDataConnectorResult = AnyErrorResult<Arc<dyn DataConnector>>;

/// Creates a default reqwest client with standard Spice settings.
///
/// # Errors
///
/// Returns an error if the client cannot be built.
pub fn default_spice_client(content_type: &'static str) -> reqwest::Result<reqwest::Client> {
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};

    let mut headers = HeaderMap::new();
    headers.append(CONTENT_TYPE, HeaderValue::from_static(content_type));

    reqwest::Client::builder()
        .user_agent(util::spiceai_user_agent())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .default_headers(headers)
        .build()
}

#[derive(Clone, Copy)]
pub struct DataConnectorRegistration {
    pub name: &'static str,
    pub constructor: fn() -> Arc<dyn DataConnectorFactory>,
}

impl DataConnectorRegistration {
    pub const fn new(
        name: &'static str,
        constructor: fn() -> Arc<dyn DataConnectorFactory>,
    ) -> Self {
        Self { name, constructor }
    }
}

/// Distributed slice that automatically collects all data connector registrations at link time
/// via the `linkme` crate. Entries are added using the [`register_data_connector!`] macro.
#[distributed_slice]
pub static DATA_CONNECTOR_REGISTRATIONS: [DataConnectorRegistration] = [..];

/// Registers a data connector factory by name.
///
/// This macro creates a constructor function for the specified connector factory type and
/// registers it in the global distributed slice of data connectors. This allows
/// the runtime to discover and instantiate connectors without updating a central registry.
///
/// # Example (simple form)
///
/// ```
/// register_data_connector!("file", FileFactory);
/// ```
///
/// # Example (explicit form)
///
/// ```
/// register_data_connector!(
///     register_file_connector,
///     FILE_CONNECTOR_REGISTRATION,
///     "file",
///     FileFactory
/// );
/// ```
///
/// Using this macro automatically adds the connector to the distributed slice,
/// making it available for discovery by the runtime.
///
/// # Linking (connectors in their own crate)
///
/// The registration this generates is a `#[linkme::distributed_slice]` static, and a static
/// is included only when its crate is actually linked — merely being a Cargo dependency is
/// **not** enough, because the linker drops the unreferenced static. So a connector defined in
/// its own crate (e.g. a `connector-*` crate) must be **force-linked** in every binary/tool that
/// should see it, via `use <crate> as _;`: currently `bin/spiced` (so `register_all()` registers
/// it) and `tools/spicepodschema` (so it appears in the generated schema). Miss that line and the
/// connector silently vanishes from both. Connectors defined inside `runtime` itself need nothing
/// extra, since `runtime` is always linked.
///
/// `linkme` must be in scope where this macro is invoked — `$crate` does not
/// resolve inside an attribute-macro path, so the expansion has to name it
/// unqualified. Bring it in with `use data_connector_api::linkme;` rather than
/// taking a separate dependency, so the version can never drift from the one
/// that declared the slice.
#[macro_export]
macro_rules! register_data_connector {
    ($fn_name:ident, $static_name:ident, $name:expr, $factory:path) => {
        fn $fn_name() -> ::std::sync::Arc<dyn $crate::DataConnectorFactory> {
            <$factory>::new_arc()
        }

        #[linkme::distributed_slice($crate::DATA_CONNECTOR_REGISTRATIONS)]
        pub static $static_name: $crate::DataConnectorRegistration =
            $crate::DataConnectorRegistration::new($name, $fn_name);
    };

    ($name:expr, $factory:ident) => {
        ::paste::paste! {
            $crate::register_data_connector!(
                [<__register_data_connector_fn_ $factory:snake>],
                [<__REGISTER_DATA_CONNECTOR_ $factory:upper>],
                $name,
                $factory
            );
        }
    };
}

pub trait DataConnectorFactory: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Builds the connector.
    ///
    /// `context` is borrowed for the duration of the call, so a connector may
    /// resolve a capability from it but must not keep the context itself — see
    /// [`ConnectorContext`].
    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>>;

    fn supports_unsupported_type_action(&self) -> bool {
        false
    }

    /// The prefix to use for parameters and secrets for this `DataConnector`.
    ///
    /// This prefix is applied to any `ParameterType::Connector` parameters.
    ///
    /// ## Example
    ///
    /// If the prefix is `pg` then the following parameters are accepted:
    ///
    /// - `pg_host` -> `host`
    /// - `pg_port` -> `port`
    ///
    /// The prefix will be stripped from the parameter name before being passed to the data connector.
    fn prefix(&self) -> &'static str;

    /// Returns a list of parameters that the data connector requires to be able to connect to the data source.
    ///
    /// Any parameter provided by a user that isn't in this list will be filtered out and a warning logged.
    fn parameters(&self) -> &'static [ParameterSpec];

    /// Returns a list of keywords that are reserved by the data connector.
    /// Used to ensure that any table name isn't a reserved keyword.
    fn reserved_keywords(&self) -> &'static [&'static str] {
        &[]
    }

    /// Returns a static schema for the given dataset if this connector
    /// **intrinsically** knows the schema from configuration alone
    /// — i.e. without contacting the source and without relying on a
    /// user-declared `columns:` block. (User-declared columns are
    /// handled separately by the runtime's deferral dispatch as a
    /// fallback when this method returns `None`.)
    ///
    /// Called during dataset registration **before** the connector itself
    /// is built (no `create` call is required first). Implementations may
    /// consult `params` (e.g. a configured file format) and `dataset`
    /// (e.g. declared content type, JSON column decomposition) but must
    /// not perform any I/O.
    ///
    /// When `Some(schema)` is returned, the runtime is allowed to register
    /// the dataset using that schema and defer building the connector and
    /// calling [`DataConnector::read_provider`] until the dataset is
    /// actually referenced. The connector is still expected to return a
    /// `TableProvider` whose schema matches on the first `read_provider`
    /// call; mismatches surface at first scan as a hard error rather than
    /// being silently retried (the static schema is configuration, not
    /// source state).
    ///
    /// Default: `None`. Most connectors do not have an intrinsic
    /// configuration-only schema and instead rely on either source
    /// inference or the user-declared `columns:` fallback.
    fn static_schema(
        &self,
        _params: &ConnectorParams,
        _dataset: &DatasetSpec,
    ) -> Option<SchemaRef> {
        None
    }
}

/// A `DataConnector` knows how to retrieve and optionally write or stream data.
#[async_trait]
pub trait DataConnector: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    /// Resolves the default refresh mode for the data connector.
    ///
    /// Most data connectors should keep this as `RefreshMode::Full`.
    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        refresh_mode.unwrap_or(RefreshMode::Full)
    }

    async fn read_provider(
        &self,
        context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>;

    async fn read_write_provider(
        &self,
        _context: &dyn ConnectorContext,
        _dataset: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    fn supports_changes_stream(&self) -> bool {
        false
    }

    /// The CDC stream for `dataset`, if this connector produces one.
    ///
    /// `async` so that anything the stream needs from `context` — a checkpoint
    /// store, a session — is resolved **here**, before the stream is built. The
    /// generator then holds only the resolved capability. That is what keeps a
    /// long-lived stream from pinning the runtime: a checkpoint store holds a
    /// connection pool and no runtime, so a stream holding one cannot close the
    /// loop.
    ///
    /// `acceleration` reports what the accelerator already holds, so a source
    /// that must otherwise assume the worst about contents it cannot place — see
    /// [`AccelerationContents`] — can tell an acceleration that is starting from
    /// nothing apart from one that may be carrying rows the source has since
    /// deleted. Sources that place their position by other means may ignore it.
    ///
    /// Wrappers must forward this argument unchanged. Substituting
    /// [`AccelerationContents::Unknown`] is safe but costs the inner connector
    /// the distinction, which for `PostgreSQL` means re-reading the whole table
    /// on a first load.
    async fn changes_stream(
        &self,
        _context: &dyn ConnectorContext,
        _federated_table: Arc<dyn FederatedTableProvider>,
        _dataset: &DatasetSpec,
        _acceleration: AccelerationContents,
    ) -> Option<ChangesStream> {
        None
    }

    fn supports_append_stream(&self) -> bool {
        false
    }

    fn append_stream(
        &self,
        _federated_table: Arc<dyn FederatedTableProvider>,
    ) -> Option<ChangesStream> {
        None
    }

    /// Whether this connector can accept durable federated write-back delivery
    /// without risking the silent loss of a committed write.
    ///
    /// Delivery reconciles a committed accelerator row to the source. Emulating
    /// an upsert as a standalone `DELETE` followed by a separate `INSERT` is
    /// **not** safe for a CDC-fed accelerator: the two are distinct source
    /// commits, so the source echoes the `DELETE` back over CDC and erases the
    /// committed row from the accelerator. If the follow-up `INSERT` then fails,
    /// the next delivery pass sees the key as absent, treats the delete as
    /// complete, and clears the marker — the write is gone from both sides with
    /// no error raised.
    ///
    /// Returning `true` therefore asserts that delivery is atomic from the
    /// source's point of view: either a single transaction covering both legs,
    /// or a native conditional upsert (`INSERT … ON CONFLICT DO UPDATE`) that
    /// removes the delete leg entirely for present keys.
    ///
    /// Defaults to `false` — the conservative answer. A connector that has not
    /// been audited for this makes the dataset fail registration with an
    /// actionable error rather than silently risk losing writes.
    ///
    /// **Wrappers must forward this.** Inheriting the default would report a
    /// perfectly safe inner connector as unsafe and reject a valid dataset.
    fn supports_durable_write_back_delivery(&self) -> bool {
        false
    }

    async fn metadata_provider(
        &self,
        _dataset: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    /// Pre-register any object stores this connector needs in order to execute
    /// scans for `dataset` against the supplied `runtime_env`.
    ///
    /// Called on cluster executor startup so that physical plans decoded from
    /// the scheduler can resolve their object stores via
    /// `runtime_env().object_store(url)` even when the per-scan
    /// `parquet_file_reader_factory` (or equivalent) is dropped during proto
    /// round-trip.
    ///
    /// The default implementation is a no-op. Connectors backed by per-table
    /// object stores (object-store-style connectors, Delta on S3/Azure/GCS,
    /// Iceberg, etc.) should override this to register the appropriate stores
    /// using the dataset's already secret-expanded params.
    async fn register_object_stores(
        &self,
        _dataset: &DatasetSpec,
        _runtime_env: &Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> DataConnectorResult<()> {
        Ok(())
    }

    /// A hook called **before** the accelerated table is built, giving the
    /// connector a chance to wrap or replace the accelerator's provider.
    ///
    /// Any provider set here will be shared with the `Refresher` created when
    /// the table is built. Use this hook instead of
    /// [`on_accelerated_table_registration`](Self::on_accelerated_table_registration)
    /// when the wrapped provider must be visible to the refresh pipeline
    /// (e.g. to recreate indexes after a data refresh).
    async fn on_accelerator_setup(
        &self,
        _dataset: &DatasetSpec,
        _accelerator: &mut dyn AcceleratorSetup,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// A hook that is called when an accelerated table is registered to the
    /// `DataFusion` context for this data connector.
    ///
    /// Allows running any setup logic specific to the data connector when its
    /// accelerated table is registered, i.e. setting up a file watcher to refresh
    /// the table when the file is updated.
    async fn on_accelerated_table_registration(
        &self,
        _dataset: &DatasetSpec,
        _accelerated_table: &mut dyn RegisteredAcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// Returns a `MetricsProvider` for the data connector.
    ///
    /// If the data connector does not support metrics, return `None`.
    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        None
    }

    /// Returns whether the data connector should be initialized on startup or on trigger.
    fn initialization(&self) -> ComponentInitialization {
        ComponentInitialization::default()
    }

    /// Returns whether the data connector should be initialized on startup or on trigger,
    /// with dataset-specific logic.
    ///
    /// This method allows connectors to make initialization decisions based on the specific
    /// dataset configuration. The default implementation delegates to `initialization()`.
    fn initialization_for_dataset(&self, _dataset: &DatasetSpec) -> ComponentInitialization {
        self.initialization()
    }
}

pub trait MetricsProviderComponent: Debug + Send + Sync + 'static {
    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>>;
}

impl<T: DataConnector + Debug + 'static> MetricsProviderComponent for T {
    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        self.metrics_provider()
    }
}
