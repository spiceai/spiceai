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

//! The harness runs Cayenne only on the metastore and data-store pairs it
//! supports: the SQLite metastore with local data files, and the object-native
//! metastore with data files placed in its location. It refuses the SQLite
//! metastore with data files in an object store wherever a test could build it,
//! and fails an object-native fixture's table that is not placed.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use cayenne::MetadataCatalog;
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;
use object_store::memory::InMemory;

use crate::common::{self, BackendType, TestFixture, UNSUPPORTED_PAIRING};

fn options(name: &str, base_path: String) -> CreateTableOptions {
    CreateTableOptions {
        table_name: name.to_string(),
        schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path,
        partition_column: None,
        vortex_config: VortexConfig::default(),
    }
}

#[tokio::test]
async fn a_sqlite_fixture_refuses_data_files_in_an_object_store() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let Err(error) = TestFixture::with_data_store(BackendType::Sqlite, Some(store)).await else {
        panic!("a SQLite fixture over an object store must fail");
    };
    assert_eq!(error.to_string(), UNSUPPORTED_PAIRING);
}

#[tokio::test]
#[should_panic(
    expected = "Cayenne pairs the SQLite metastore only with local data files (`runtime.params.cayenne_vnext` off)"
)]
async fn a_sqlite_catalog_refuses_a_table_builder_on_an_object_store() {
    let fixture = TestFixture::new(BackendType::Sqlite)
        .await
        .expect("SQLite fixture");
    let ctx = SessionContext::new();
    let _builder = common::table_builder_on_store(
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
        ctx.runtime_env(),
        ObjectStoreConfig {
            url: url::Url::parse("s3://cayenne-test/refused/data").expect("url"),
            store: Arc::new(InMemory::new()),
        },
    );
}

/// An object-native fixture places each table in a dataset folder of its
/// location, as the runtime places a new dataset, and a table given any other
/// object-store path is refused.
#[tokio::test]
async fn an_object_native_fixture_places_its_tables_in_the_location() {
    let fixture = TestFixture::new(BackendType::ObjectNative)
        .await
        .expect("object-native fixture");
    let catalog = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let base_path = fixture.data_store.base_path("orders");
    let table = fixture
        .data_store
        .create(
            Arc::clone(&catalog),
            ctx.runtime_env(),
            options("orders", base_path.clone()),
        )
        .await
        .expect("a placed table");
    assert!(
        cayenne::metastore_layout::is_object_native_data_path(&table.metadata().path),
        "the table records a placed data path: {}",
        table.metadata().path
    );
    let datasets = base_path
        .rsplit_once("/orders-")
        .map(|(datasets, _)| datasets)
        .expect("a dataset folder named for the table");
    assert!(
        datasets.ends_with("/state/cayenne/v1/datasets"),
        "the dataset folders are the location's: {base_path}"
    );

    let unplaced = datasets.replace("/state/cayenne/v1/datasets", "/elsewhere/data");
    let Err(error) = fixture
        .data_store
        .create(
            catalog,
            ctx.runtime_env(),
            options("stray", unplaced.clone()),
        )
        .await
    else {
        panic!("a table outside the location's dataset folders must fail");
    };
    // The object-native catalog refuses the unplaced path before the fixture's
    // own placement check is reached.
    assert_eq!(
        error.to_string(),
        format!(
            "Invalid operation: {}",
            cayenne::metastore_layout::object_native_data_path_refusal("stray")
        ),
        "data path: {unplaced}"
    );
}

/// Every test that gives a table an object store builds it with
/// `common::table_builder_on_store`, which refuses the SQLite metastore, so no
/// test can pair the two without that refusal.
#[test]
fn every_object_store_table_builder_goes_through_the_pairing_check() {
    let tests = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut pending = vec![tests.clone()];
    let mut direct = Vec::new();
    let mut scanned = 0usize;
    // Assembled, so this file does not match itself.
    let direct_call = [".with_", "object_store("].concat();
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).expect("read the tests directory") {
            let path = entry.expect("directory entry").path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().is_none_or(|extension| extension != "rs")
                || path.ends_with("common/mod.rs")
            {
                continue;
            }
            scanned += 1;
            let source = std::fs::read_to_string(&path).expect("read a test source");
            for (number, line) in source.lines().enumerate() {
                if line.contains(&direct_call) {
                    direct.push(format!(
                        "{}:{}",
                        path.strip_prefix(&tests).unwrap_or(&path).display(),
                        number + 1
                    ));
                }
            }
        }
    }
    assert!(scanned > 100, "scanned only {scanned} test sources");
    assert!(
        direct.is_empty(),
        "build these tables with `common::table_builder_on_store`, which refuses the SQLite \
         metastore with data files in an object store: {direct:?}"
    );
}
