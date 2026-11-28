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

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{
    Runtime,
    component::{access::AccessMode, catalog::Catalog},
};

use super::CatalogConnector;

#[derive(Clone)]
pub struct DeferredCatalogProvider {
    rt: Arc<Runtime>,
    connector: Arc<dyn CatalogConnector>,
    catalog: Catalog,
    /// Held while the catalog loads, so one load runs at a time.
    loading: Arc<tokio::sync::Mutex<()>>,
    /// Loads that ended, so a request that waited through one does not repeat it.
    ended_loads: Arc<AtomicUsize>,
}

impl Debug for DeferredCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredCatalogProvider")
            .field("catalog", &self.catalog)
            .field("connector", &"<connector>")
            .finish_non_exhaustive()
    }
}

impl DeferredCatalogProvider {
    pub fn new(rt: Arc<Runtime>, connector: Arc<dyn CatalogConnector>, catalog: Catalog) -> Self {
        Self {
            rt,
            connector,
            catalog,
            loading: Arc::default(),
            ended_loads: Arc::default(),
        }
    }

    /// Waits until no other load of this catalog runs, and holds off later loads until
    /// the guard drops.
    pub async fn lock_load(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.loading.lock().await
    }

    #[must_use]
    pub fn access(&self) -> &AccessMode {
        &self.catalog.access
    }

    #[must_use]
    pub fn user_token_client_id(&self) -> Option<&str> {
        self.connector.user_token_client_id()
    }

    #[must_use]
    pub fn ended_loads(&self) -> usize {
        self.ended_loads.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn source(&self) -> Arc<dyn CatalogConnector> {
        Arc::clone(&self.connector)
    }

    pub async fn get_catalog_provider(&self) -> super::Result<Arc<dyn CatalogProvider>> {
        let provider = Arc::clone(&self.connector)
            .refreshable_catalog_provider(Arc::clone(&self.rt), &self.catalog)
            .await;
        self.ended_loads.fetch_add(1, Ordering::SeqCst);
        Ok(provider?)
    }
}

impl CatalogProvider for DeferredCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn schema(&self, _: &str) -> Option<Arc<dyn SchemaProvider>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use data_components::RefreshableCatalogProvider;
    use datafusion::catalog::MemoryCatalogProvider;
    use tokio::sync::{Notify, Semaphore};

    use super::*;
    use crate::component::catalog::CatalogBuilder;
    use crate::dataconnector::ConnectorComponent;
    use crate::datafusion::{DataFusion, Error};

    /// A catalog source whose loads fail a set number of times first, as on an expired token.
    #[derive(Default)]
    struct TestCatalog {
        client_id: Option<String>,
        failures: AtomicUsize,
        misconfigured: bool,
        /// When set, a load waits for a permit after it starts.
        gate: Option<Semaphore>,
        started: Notify,
        loads: AtomicUsize,
    }

    #[derive(Debug)]
    struct LoadedCatalog(MemoryCatalogProvider);

    impl CatalogProvider for LoadedCatalog {
        fn schema_names(&self) -> Vec<String> {
            self.0.schema_names()
        }

        fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            self.0.schema(name)
        }
    }

    #[async_trait]
    impl RefreshableCatalogProvider for LoadedCatalog {
        async fn refresh(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
    }

    #[async_trait]
    impl CatalogConnector for TestCatalog {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn user_token_client_id(&self) -> Option<&str> {
            self.client_id.as_deref()
        }

        async fn refreshable_catalog_provider(
            self: Arc<Self>,
            _runtime: Arc<Runtime>,
            catalog: &Catalog,
        ) -> super::super::Result<Arc<dyn RefreshableCatalogProvider>> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            if let Some(gate) = &self.gate {
                gate.acquire().await.expect("the gate stays open").forget();
            }
            if self.misconfigured {
                return Err(super::super::Error::InvalidConfigurationNoSource {
                    connector: "test".to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    message: "an invalid parameter".to_string(),
                });
            }
            if self
                .failures
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(super::super::Error::FailedToGetAppFromRuntime {});
            }
            Ok(Arc::new(LoadedCatalog(MemoryCatalogProvider::new())))
        }
    }

    async fn defer(rt: &Arc<Runtime>, source: &Arc<TestCatalog>) -> Arc<DataFusion> {
        let mut catalog =
            CatalogBuilder::try_new("databricks:main".to_string(), "u2m").expect("valid catalog");
        catalog.access = AccessMode::ReadWrite;
        let catalog = catalog
            .with_app(Arc::new(app::AppBuilder::new("deferred").build()))
            .with_runtime(Arc::clone(rt))
            .build()
            .expect("catalog builds");
        let df = rt.datafusion();
        df.register_catalog(
            "u2m",
            &AccessMode::Read,
            Arc::new(DeferredCatalogProvider::new(
                Arc::clone(rt),
                Arc::clone(source) as Arc<dyn CatalogConnector>,
                catalog,
            )),
        )
        .await
        .expect("registers the deferred catalog");
        df
    }

    #[tokio::test]
    async fn a_catalog_whose_load_failed_loads_on_a_later_request() {
        let rt = Arc::new(Runtime::builder().build().await);
        let source = Arc::new(TestCatalog {
            failures: AtomicUsize::new(1),
            ..TestCatalog::default()
        });
        let df = defer(&rt, &source).await;

        let err = df
            .load_deferred_catalog("u2m")
            .await
            .expect_err("the first load fails");
        assert!(
            matches!(err, Error::UnableToLoadDeferredCatalog { .. }),
            "{err}"
        );
        assert!(df.ctx.catalog("u2m").is_none());

        df.load_deferred_catalog("u2m")
            .await
            .expect("a later request loads the catalog");
        assert!(df.ctx.catalog("u2m").is_some());
        assert!(df.is_catalog_writable("u2m"));

        df.load_deferred_catalog("u2m")
            .await
            .expect("a loaded catalog is not loaded again");
        assert_eq!(source.loads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn requests_load_a_catalog_by_the_client_id_it_resolved() {
        let rt = Arc::new(Runtime::builder().build().await);
        let source = Arc::new(TestCatalog {
            client_id: Some("resolved-client".to_string()),
            ..TestCatalog::default()
        });
        let df = defer(&rt, &source).await;
        let request = |client_id: &str| {
            crate::request::DatabricksAuthExtension::new(
                Some(Arc::clone(&df)),
                Arc::new(std::collections::HashMap::from([(
                    client_id.to_string(),
                    secrecy::SecretString::from("token"),
                )])),
            )
        };

        request("other-client").load_u2m_components().await;
        assert!(df.ctx.catalog("u2m").is_none());

        request("resolved-client").load_u2m_components().await;
        assert!(df.ctx.catalog("u2m").is_some());
    }

    #[tokio::test]
    async fn a_catalog_registered_at_startup_replaces_a_deferred_one() {
        let rt = Arc::new(Runtime::builder().build().await);
        let source = Arc::new(TestCatalog::default());
        let df = defer(&rt, &source).await;

        df.register_catalog(
            "u2m",
            &AccessMode::Read,
            Arc::new(MemoryCatalogProvider::new()),
        )
        .await
        .expect("registers the catalog");
        df.load_deferred_catalog("u2m")
            .await
            .expect("nothing is deferred");
        assert_eq!(source.loads.load(Ordering::SeqCst), 0);
        assert!(df.ctx.catalog("u2m").is_some());
    }

    #[tokio::test]
    async fn a_misconfigured_catalog_is_not_loaded_again() {
        let rt = Arc::new(Runtime::builder().build().await);
        let source = Arc::new(TestCatalog {
            misconfigured: true,
            ..TestCatalog::default()
        });
        let df = defer(&rt, &source).await;

        let err = df
            .load_deferred_catalog("u2m")
            .await
            .expect_err("the load fails");
        assert!(matches!(err, Error::InvalidDeferredCatalog { .. }), "{err}");
        df.load_deferred_catalog("u2m")
            .await
            .expect("no later request loads it");
        assert_eq!(source.loads.load(Ordering::SeqCst), 1);
        assert!(df.ctx.catalog("u2m").is_none());
        assert!(matches!(
            rt.status.get_catalog_statuses().get("u2m"),
            Some(crate::status::ComponentStatus::Error(_))
        ));
    }

    #[tokio::test]
    async fn a_request_that_waited_through_a_failed_load_does_not_repeat_it() {
        let rt = Arc::new(Runtime::builder().build().await);
        let source = Arc::new(TestCatalog {
            failures: AtomicUsize::new(1),
            gate: Some(Semaphore::new(0)),
            ..TestCatalog::default()
        });
        let df = defer(&rt, &source).await;

        let first = tokio::spawn({
            let df = Arc::clone(&df);
            async move { df.load_deferred_catalog("u2m").await }
        });
        let second = tokio::spawn({
            let df = Arc::clone(&df);
            async move { df.load_deferred_catalog("u2m").await }
        });
        source.started.notified().await;
        // Lets the second request reach the catalog while the first load waits at the gate.
        tokio::time::sleep(Duration::from_millis(50)).await;
        source.gate.as_ref().expect("gated").add_permits(3);

        first
            .await
            .expect("joins")
            .expect_err("the first load fails");
        second
            .await
            .expect("joins")
            .expect("the second request ends without a load");
        assert_eq!(source.loads.load(Ordering::SeqCst), 1);

        df.load_deferred_catalog("u2m")
            .await
            .expect("a later request loads the catalog");
        assert!(df.ctx.catalog("u2m").is_some());
    }

    #[tokio::test]
    async fn concurrent_requests_load_a_catalog_once() {
        let rt = Arc::new(Runtime::builder().build().await);
        let source = Arc::new(TestCatalog {
            gate: Some(Semaphore::new(0)),
            ..TestCatalog::default()
        });
        let df = defer(&rt, &source).await;

        let first = tokio::spawn({
            let df = Arc::clone(&df);
            async move { df.load_deferred_catalog("u2m").await }
        });
        let second = tokio::spawn({
            let df = Arc::clone(&df);
            async move { df.load_deferred_catalog("u2m").await }
        });
        source.started.notified().await;
        // Lets the second request reach the catalog while the first load waits at the gate.
        tokio::time::sleep(Duration::from_millis(50)).await;
        source.gate.as_ref().expect("gated").add_permits(2);

        first.await.expect("joins").expect("loads");
        second.await.expect("joins").expect("loads");
        assert_eq!(source.loads.load(Ordering::SeqCst), 1);
        assert!(df.ctx.catalog("u2m").is_some());
    }

    #[tokio::test]
    async fn a_load_whose_catalog_a_reload_replaced_registers_nothing() {
        let rt = Arc::new(Runtime::builder().build().await);
        let replaced = Arc::new(TestCatalog {
            gate: Some(Semaphore::new(0)),
            ..TestCatalog::default()
        });
        let df = defer(&rt, &replaced).await;

        let load = tokio::spawn({
            let df = Arc::clone(&df);
            async move { df.load_deferred_catalog("u2m").await }
        });
        replaced.started.notified().await;
        let current = Arc::new(TestCatalog::default());
        defer(&rt, &current).await;
        replaced.gate.as_ref().expect("gated").add_permits(1);
        load.await.expect("joins").expect("ends");
        assert!(df.ctx.catalog("u2m").is_none());

        df.load_deferred_catalog("u2m")
            .await
            .expect("a later request loads the current catalog");
        assert!(df.ctx.catalog("u2m").is_some());
        assert_eq!(current.loads.load(Ordering::SeqCst), 1);
    }
}
