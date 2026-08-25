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

use crate::component::catalog::Catalog;
use crate::component::dataset::Dataset;
// A second alias for the `runtime-parameters` types, kept crate-visible for the
// same reason as the `parameters` alias itself: it would otherwise be a way for
// a connector to name them without depending on the crate that owns them.
pub(crate) use crate::parameters::ParameterSpec;
pub(crate) use crate::parameters::Parameters;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;

pub mod client_identity;
// Re-exports `data-http-rate-control`; crate-visible so a connector outside the
// runtime depends on that crate directly instead of routing through here.
pub(crate) mod http_rate_control;

// abfs: moved to crates/data-connectors/connector-abfs
// #[deprecated] pub mod abfs;
// adbc: moved to crates/data-connectors/connector-adbc
// #[cfg(feature = "adbc")] pub mod adbc;
// cosmosdb: moved to crates/data-connectors/connector-cosmosdb
// #[cfg(feature = "cosmosdb")] pub mod cosmosdb;
#[cfg(feature = "debezium")]
pub mod cdc_ingest;
#[cfg(feature = "debezium")]
pub mod debezium;
pub mod file;

// git: moved to crates/data-connectors/connector-git
// github: moved to crates/data-connectors/connector-github
pub mod https;
// kafka connector moved to crates/data-connectors/connector-kafka; module kept for debezium sidecar types
#[cfg(feature = "debezium")]
pub mod kafka;
pub mod localpod;
pub mod memory;

pub const ODBC_DATACONNECTOR: &str = "odbc"; // const needs to be accessible when ODBC isn't built
pub const SCYLLADB_DATACONNECTOR: &str = "scylladb"; // const needs to be accessible when ScyllaDB isn't built
/// The cargo feature that builds the ScyllaDB data connector into `spiced`.
pub const SCYLLADB_FEATURE: &str = "scylladb";
pub mod deferred;
// ducklake: moved to crates/data-connectors/connector-ducklake
// gcs: moved to crates/data-connectors/connector-gcs
// glue: registration moved to crates/data-connectors/connector-glue; module kept for catalog connector
pub mod glue;
pub mod iceberg;
pub mod iceberg_cluster;
pub mod parameters;
pub mod refresh_source;
pub mod s3;
// Re-exports `data-connector-api`'s projection parser; crate-visible so a
// connector outside the runtime depends on that crate directly.
pub(crate) mod schema_projection;
pub mod sink;
// spiceai: registration moved to crates/data-connectors/connector-spiceai; module kept for catalog connector
pub mod spiceai;

// The connector contract lives in `data-connector-api`, below `runtime`.
// Crate-visible, not public: a public re-export would be a second path to every
// contract item, and anything reached through `runtime` re-acquires the
// dependency on the orchestrator that the inversion just removed — invisibly to
// the layering guard, which only sees the `runtime` edge. Everything outside
// this crate names `data-connector-api` directly.
pub(crate) use data_connector_api::*;

static DATA_CONNECTOR_FACTORY_REGISTRY: LazyLock<
    Mutex<HashMap<String, Arc<dyn DataConnectorFactory>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub async fn register_connector_factory(
    name: &str,
    connector_factory: Arc<dyn DataConnectorFactory>,
) {
    let mut registry = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;

    registry.insert(name.to_string(), connector_factory);
}

/// Look up a registered connector factory by name.
pub async fn get_connector_factory(name: &str) -> Option<Arc<dyn DataConnectorFactory>> {
    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
    guard.get(name).map(Arc::clone)
}

/// Create a new `DataConnector` by name.
///
/// # Returns
///
/// `None` if the connector for `name` is not registered, otherwise a `Result` containing the result of calling the constructor to create a `DataConnector`.
pub async fn create_new_connector(
    name: &str,
    params: ConnectorParams,
    context: &dyn ConnectorContext,
) -> Option<AnyErrorResult<Arc<dyn DataConnector>>> {
    let factory = {
        let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
        Arc::clone(guard.get(name)?)
    };

    let ConnectorComponent::Dataset(ds) = &params.component else {
        unreachable!("Component is always a dataset at this point")
    };

    if factory
        .reserved_keywords()
        .contains(&ds.name.table().to_ascii_lowercase().as_str())
    {
        return Some(Err(DataConnectorError::UseOfProtectedKeyword {
            dataconnector: name.to_string(),
            keyword: ds.name.table().to_string(),
        }
        .into()));
    }

    if params.unsupported_type_action.is_some() && !factory.supports_unsupported_type_action() {
        return Some(Err(DataConnectorError::UnsupportedTypeAction {
            dataconnector: name.to_string(),
            connector_component: params.component.clone(),
        }
        .into()));
    }

    let result = factory.create(params, context).await;
    Some(result)
}

// [`DataConnectorFactory`] added here should not hold live resources (e.g. cached connection pools).
// If a factory is ever added that owns a live resource, must reimplement an `unregister_all`.
pub async fn register_all() {
    for registration in DATA_CONNECTOR_REGISTRATIONS {
        register_connector_factory(registration.name, (registration.constructor)()).await;
    }
}

/// Names of every registered data connector. Useful for generating helpful
/// "did you mean?" suggestions when a user references an unknown connector.
pub async fn registered_connector_names() -> Vec<String> {
    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
    let mut names: Vec<String> = guard.keys().cloned().collect();
    names.sort();
    names
}

/// Returns the registered connector name whose Levenshtein distance to `name`
/// is lowest (bounded so short typos only match very close names). Routes
/// through [`util::levenshtein::closest_match`] so the "did you mean" UX is
/// consistent with runtime tunables and component-level parameters.
pub async fn suggest_connector(name: &str) -> Option<String> {
    util::levenshtein::closest_match(name, &registered_connector_names().await)
}

impl From<&Dataset> for ConnectorComponent {
    fn from(dataset: &Dataset) -> Self {
        ConnectorComponent::Dataset(Arc::new(dataset.spec.clone()))
    }
}

impl From<&Catalog> for ConnectorComponent {
    fn from(catalog: &Catalog) -> Self {
        ConnectorComponent::Catalog(Arc::new(catalog.spec.clone()))
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use datafusion::datasource::TableProvider;
    use datafusion_table_providers::UnsupportedTypeAction;
    use runtime_component::dataset::DatasetSpec;
    use std::any::Any;
    use std::future::Future;
    use std::pin::Pin;
    use tokio::runtime::Handle;
    use tokio::sync::{Barrier, RwLock};
    use tokio::time::{Duration, timeout};

    use super::*;

    // The closest-match algorithm is exercised in
    // crates/util/src/levenshtein.rs (`test_closest_match_*`). `suggest_connector`
    // / `suggest_catalog_connector` just plumb the registry's name list through
    // it, so no per-call wrapper test is needed here.

    use crate::component::dataset::UnsupportedTypeAction as DatasetUnsupportedTypeAction;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataconnector::parameters::ConnectorParamsBuilder;
    use crate::secrets::Secrets;

    async fn build_test_connector_params(
        connector_name: &str,
        dataset_name: &str,
        app: Arc<app::App>,
        runtime: Arc<crate::Runtime>,
        secrets: Arc<RwLock<Secrets>>,
    ) -> ConnectorParams {
        let dataset =
            DatasetBuilder::try_new(format!("{connector_name}:{dataset_name}"), dataset_name)
                .expect("Failed to create builder")
                .with_app(app)
                .with_runtime(runtime)
                .build()
                .expect("Failed to build dataset");

        ConnectorParamsBuilder::for_dataset(connector_name.into(), &dataset)
            .build(secrets, Handle::current())
            .await
            .expect("failed to build connector params")
    }

    #[tokio::test]
    async fn test_static_schema_default_returns_none() {
        // Any factory that doesn't override `static_schema` should return
        // None. This is the contract relied on by the deferred-dataset
        // path: a None return falls back to the eager source-contact
        // registration flow.
        struct DefaultFactory;
        impl DataConnectorFactory for DefaultFactory {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn create<'a>(
                &'a self,
                _params: ConnectorParams,
                _context: &'a dyn ConnectorContext,
            ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
                unimplemented!("static_schema must not require create()")
            }
            fn prefix(&self) -> &'static str {
                "default_factory"
            }
            fn parameters(&self) -> &'static [ParameterSpec] {
                &[]
            }
        }

        register_connector_factory("default_factory", Arc::new(DefaultFactory)).await;

        let app = Arc::new(app::AppBuilder::new("test_app").build());
        let rt = Arc::new(crate::Runtime::builder().build().await);
        let secrets = Arc::new(RwLock::new(Secrets::default()));
        let dataset = DatasetBuilder::try_new("default_factory:tbl".to_string(), "tbl")
            .expect("Failed to create builder")
            .with_app(Arc::clone(&app))
            .with_runtime(Arc::clone(&rt))
            .build()
            .expect("Failed to build dataset");

        let params = ConnectorParamsBuilder::for_dataset("default_factory".into(), &dataset)
            .build(secrets, Handle::current())
            .await
            .expect("failed to build connector params");

        let factory = DefaultFactory;
        assert!(factory.static_schema(&params, &dataset).is_none());
    }

    #[tokio::test]
    async fn test_connector_params_builder_unsupported_type_action() {
        // Register a test connector factory
        struct TestConnectorFactory;
        impl DataConnectorFactory for TestConnectorFactory {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn create<'a>(
                &'a self,
                _params: ConnectorParams,
                _context: &'a dyn ConnectorContext,
            ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
                Box::pin(async {
                    let connector: Arc<dyn DataConnector> = Arc::new(TestConnector);
                    Ok(connector)
                })
            }

            fn prefix(&self) -> &'static str {
                "test"
            }

            fn parameters(&self) -> &'static [ParameterSpec] {
                &[]
            }

            fn supports_unsupported_type_action(&self) -> bool {
                true
            }
        }

        #[derive(Debug)]
        struct TestConnector;

        #[async_trait]
        impl DataConnector for TestConnector {
            fn as_any(&self) -> &dyn Any {
                self
            }

            async fn read_provider(
                &self,
                _context: &dyn ConnectorContext,
                _dataset: &DatasetSpec,
            ) -> DataConnectorResult<Arc<dyn TableProvider>> {
                unimplemented!()
            }
        }

        register_connector_factory("test", Arc::new(TestConnectorFactory)).await;

        // Create a test dataset with unsupported_type_action
        let app = app::AppBuilder::new("test_app").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new("test:test_dataset".to_string(), "test_dataset")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");
        dataset.unsupported_type_action = Some(DatasetUnsupportedTypeAction::Ignore);

        let secrets = Arc::new(RwLock::new(Secrets::default()));
        let builder = ConnectorParamsBuilder::for_dataset("test".into(), &dataset);

        let result = builder.build(secrets, Handle::current()).await;
        assert!(result.is_ok());

        let params = result.expect("failed to build connector params");
        assert_eq!(
            params.unsupported_type_action,
            Some(UnsupportedTypeAction::Ignore),
            "Unsupported type action should be properly set in connector params"
        );
    }

    #[tokio::test]
    async fn test_create_new_connector_allows_concurrent_factory_initialization() {
        struct ConcurrentConnectorFactory {
            barrier: Arc<Barrier>,
        }

        impl DataConnectorFactory for ConcurrentConnectorFactory {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn create<'a>(
                &'a self,
                _params: ConnectorParams,
                _context: &'a dyn ConnectorContext,
            ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send + 'a>> {
                let barrier = Arc::clone(&self.barrier);

                Box::pin(async move {
                    barrier.wait().await;

                    let connector: Arc<dyn DataConnector> = Arc::new(TestConnector);
                    Ok(connector)
                })
            }

            fn prefix(&self) -> &'static str {
                "test_concurrent"
            }

            fn parameters(&self) -> &'static [ParameterSpec] {
                &[]
            }
        }

        #[derive(Debug)]
        struct TestConnector;

        #[async_trait]
        impl DataConnector for TestConnector {
            fn as_any(&self) -> &dyn Any {
                self
            }

            async fn read_provider(
                &self,
                _context: &dyn ConnectorContext,
                _dataset: &DatasetSpec,
            ) -> DataConnectorResult<Arc<dyn TableProvider>> {
                unimplemented!()
            }
        }

        register_connector_factory(
            "test_concurrent",
            Arc::new(ConcurrentConnectorFactory {
                barrier: Arc::new(Barrier::new(2)),
            }),
        )
        .await;

        let app = Arc::new(app::AppBuilder::new("test_app").build());
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let secrets = Arc::new(RwLock::new(Secrets::default()));

        let params_one = build_test_connector_params(
            "test_concurrent",
            "first",
            Arc::clone(&app),
            Arc::clone(&runtime),
            Arc::clone(&secrets),
        )
        .await;
        let params_two = build_test_connector_params(
            "test_concurrent",
            "second",
            Arc::clone(&app),
            Arc::clone(&runtime),
            secrets,
        )
        .await;
        let context = parameters::RuntimeConnectorContext::new(app, runtime);

        let (result_one, result_two) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                create_new_connector("test_concurrent", params_one, &context),
                create_new_connector("test_concurrent", params_two, &context),
            )
        })
        .await
        .expect("create_new_connector should not serialize concurrent factory initialization");

        assert!(result_one.is_some(), "first factory should be registered");
        assert!(result_two.is_some(), "second factory should be registered");
        assert!(
            result_one.expect("first factory should exist").is_ok(),
            "first connector should initialize successfully"
        );
        assert!(
            result_two.expect("second factory should exist").is_ok(),
            "second connector should initialize successfully"
        );
    }

    /// A source that advertises safe durable write-back delivery, so a wrapper
    /// that silently inherits the trait default is visible as `false`.
    #[derive(Debug)]
    struct SafeDeliveryConnector;

    #[async_trait]
    impl DataConnector for SafeDeliveryConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn read_provider(
            &self,
            _context: &dyn ConnectorContext,
            _dataset: &DatasetSpec,
        ) -> DataConnectorResult<Arc<dyn TableProvider>> {
            unimplemented!("capability-forwarding test never reads")
        }

        fn supports_durable_write_back_delivery(&self) -> bool {
            true
        }
    }

    /// `supports_durable_write_back_delivery` has a `false` default, so any
    /// wrapper that forgets to forward it silently reports a safe source as
    /// unsafe and the registration gate rejects a valid dataset. That is the
    /// defaulted-no-op wrapper bug (#10460) applied to this capability, and it
    /// compiles cleanly — only a test catches it.
    ///
    /// `ElasticsearchFullTextConnector` forwards the same one-liner but is
    /// behind the `elasticsearch` feature and needs a params-bearing dataset to
    /// construct, so it is not exercised here.
    #[test]
    fn every_wrapper_forwards_durable_write_back_delivery_support() {
        let inner: Arc<dyn DataConnector> = Arc::new(SafeDeliveryConnector);
        assert!(
            inner.supports_durable_write_back_delivery(),
            "precondition: the wrapped source advertises safe delivery"
        );

        let deferred = crate::dataconnector::deferred::DeferredConnector::new(Arc::clone(&inner));
        assert!(
            deferred.supports_durable_write_back_delivery(),
            "DeferredConnector must forward the source's delivery capability"
        );

        let full_text =
            crate::search::full_text::connector::FullTextConnector::new(Arc::clone(&inner));
        assert!(
            full_text.supports_durable_write_back_delivery(),
            "FullTextConnector must forward the source's delivery capability"
        );

        let embedding = crate::embeddings::connector::EmbeddingConnector::new(
            Arc::clone(&inner),
            Arc::new(RwLock::new(std::collections::HashMap::new())),
            Arc::new(RwLock::new(Secrets::default())),
        );
        assert!(
            embedding.supports_durable_write_back_delivery(),
            "EmbeddingConnector must forward the source's delivery capability"
        );

        let drasi = crate::drasi::connector::DrasiConnector::new(
            Arc::clone(&inner),
            crate::drasi::DeliveryMode::Acknowledged(Arc::new(
                runtime_drasi::DrasiSink::try_new(runtime_drasi::DrasiSinkConfig {
                    dataset: "test".to_string(),
                    source_id: "test".to_string(),
                    mapping: runtime_drasi::ElementMapping::new(
                        "test".to_string(),
                        vec!["test".to_string()],
                    ),
                    // Never connected to: building the sink only builds a client.
                    transport: runtime_drasi::TransportConfig::Http {
                        endpoint: url::Url::parse("http://127.0.0.1:1").expect("valid url"),
                        request_timeout: Duration::from_secs(1),
                    },
                    on_delivery_error: runtime_drasi::OnDeliveryError::Block,
                })
                .expect("builds a sink"),
            )),
        );
        assert!(
            drasi.supports_durable_write_back_delivery(),
            "DrasiConnector must forward the source's delivery capability"
        );
    }

    #[tokio::test]
    async fn shutting_down_one_runtime_does_not_deregister_connectors_for_another() {
        // Regression test for the `s3` connector race that flaked
        // `search::test_megascience_permutations` in CI: shutting one
        // `Runtime` down must not deregister connectors a sibling `Runtime`
        // still relies on.
        let rt_a = crate::Runtime::builder().build().await;
        let rt_b = crate::Runtime::builder().build().await;

        assert!(
            get_connector_factory("s3").await.is_some(),
            "s3 connector should be registered before any shutdown"
        );

        rt_a.shutdown().await;

        assert!(
            get_connector_factory("s3").await.is_some(),
            "shutting down one Runtime must not deregister connectors a sibling Runtime still needs"
        );

        rt_b.shutdown().await;
    }
}
