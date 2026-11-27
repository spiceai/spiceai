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

//! The process disk cache is opened by the first object-native catalog, not by
//! runtime startup, and concurrent catalogs share that one opening attempt.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use accelerator_cayenne::CayenneAccelerator;
use app::AppBuilder;
use arrow_schema::{DataType, Field, Schema};
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry, DataAccelerator};
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use object_store_cache::{NodeCache, NodeCacheConfig};
use runtime_acceleration::Engine;
use runtime_acceleration::acceleration::{Acceleration, Mode, RefreshMode};
use runtime_acceleration::testing::TestAccelerationSource;
use spicepod::component::runtime::RuntimeState;

fn require_process_per_test() {
    assert!(
        std::env::var("NEXTEST_EXECUTION_MODE").as_deref() == Ok("process-per-test"),
        "this test changes the process disk cache; run it with `cargo nextest run -p accelerator-cayenne`"
    );
}

async fn create_object_native_table(root: &std::path::Path, table: &str) -> Arc<dyn TableProvider> {
    let state_dir = root.join("state");
    std::fs::create_dir_all(&state_dir).expect("create state directory");
    let location = url::Url::from_directory_path(&state_dir)
        .expect("state location URL")
        .to_string();
    let mut app = AppBuilder::new(format!("disk-cache-{table}"))
        .with_runtime_params(
            [(
                cayenne::CAYENNE_VNEXT_PARAM.to_string(),
                "enabled".to_string(),
            )]
            .into_iter()
            .collect(),
        )
        .build();
    app.runtime.state = Some(RuntimeState {
        location,
        params: None,
    });
    let source = TestAccelerationSource::new(table)
        .with_app(Arc::new(app))
        .with_acceleration(Acceleration {
            enabled: true,
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(RefreshMode::Append),
            ..Default::default()
        });
    let registry = AcceleratorEngineRegistry::new();
    registry
        .register_accelerator_engine(
            Engine::Cayenne,
            Arc::new(CayenneAccelerator::new()) as Arc<dyn DataAccelerator>,
        )
        .await;
    let acceleration = source.acceleration().expect("acceleration").clone();
    registry
        .create_accelerator_table(
            TableReference::bare(table),
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            None,
            &acceleration,
            source.secrets(),
            Some(&source),
            Arc::new(SessionContext::new()),
        )
        .await
        .expect("create object-native table")
}

fn register_test_opener(cache_dir: &std::path::Path, calls: Arc<AtomicUsize>) {
    let cache_dir = cache_dir.to_path_buf();
    assert!(cayenne::disk_cache::register_opener(move || async move {
        calls.fetch_add(1, Ordering::SeqCst);
        let cache = NodeCache::open(NodeCacheConfig::new(&cache_dir, 32 << 20))
            .await
            .expect("open cache");
        assert!(cayenne::disk_cache::install(cache, cache_dir));
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_created_after_startup_opens_the_registered_cache() {
    require_process_per_test();
    let directory = tempfile::tempdir().expect("test directory");
    let calls = Arc::new(AtomicUsize::new(0));
    register_test_opener(&directory.path().join("cache"), Arc::clone(&calls));

    assert!(cayenne::disk_cache::process_disk_cache().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let _table = create_object_native_table(directory.path(), "orders").await;

    assert!(cayenne::disk_cache::process_disk_cache().is_some());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    cayenne::disk_cache::close_process_disk_cache(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_catalog_creation_runs_the_registered_opener_once() {
    require_process_per_test();
    let directory = tempfile::tempdir().expect("test directory");
    let calls = Arc::new(AtomicUsize::new(0));
    register_test_opener(&directory.path().join("cache"), Arc::clone(&calls));

    let orders_root = directory.path().join("orders");
    let customers_root = directory.path().join("customers");
    let (orders, customers) = tokio::join!(
        create_object_native_table(&orders_root, "orders"),
        create_object_native_table(&customers_root, "customers"),
    );

    assert_eq!(orders.schema().fields().len(), 1);
    assert_eq!(customers.schema().fields().len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(cayenne::disk_cache::process_disk_cache().is_some());
    cayenne::disk_cache::close_process_disk_cache(Duration::from_secs(5)).await;
}
