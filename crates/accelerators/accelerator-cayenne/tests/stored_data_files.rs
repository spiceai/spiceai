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

//! A file-mode Cayenne acceleration whose data files no longer match its
//! metastore is not opened as if nothing happened. Opening one used to serve
//! the rows of whatever files were left, or none, with nothing logged.

#![expect(
    clippy::expect_used,
    reason = "set-up and helpers outside `#[test]` functions fail fast with `expect`"
)]

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use accelerator_cayenne::CayenneAccelerator;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
};
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry};
use datafusion::{
    common::TableReference,
    datasource::TableProvider,
    execution::context::{SessionConfig, SessionContext},
};
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, Mode, RefreshMode},
    testing::TestAccelerationSource,
};

const ROWS: i64 = 1000;

/// The Cayenne metastores every check runs against: the SQLite metastore, and
/// the object-native one `runtime.params.cayenne_vnext: enabled` selects.
#[derive(Clone, Copy, Debug)]
enum Metastore {
    Sqlite,
    ObjectNative,
}

const METASTORES: [Metastore; 2] = [Metastore::Sqlite, Metastore::ObjectNative];

/// The app the acceleration belongs to, with the flag set for `metastore`.
fn app(metastore: Metastore) -> app::App {
    let builder = app::AppBuilder::new("test");
    match metastore {
        Metastore::Sqlite => builder.build(),
        Metastore::ObjectNative => builder
            .with_runtime_params(
                [("cayenne_vnext".to_string(), "enabled".to_string())]
                    .into_iter()
                    .collect(),
            )
            .build(),
    }
}

/// The file-mode Cayenne acceleration of `t (id)` stored under `dir`, as the
/// runtime opens it. Under `refresh_mode: full` a reload brings back every row,
/// so damage is reported for the runtime to rebuild it; under `append` it is not.
async fn table(
    ctx: &SessionContext,
    dir: &Path,
    refresh_mode: RefreshMode,
    metastore: Metastore,
) -> Result<Arc<dyn TableProvider>, String> {
    let source = TestAccelerationSource::new("t")
        .with_app(app(metastore))
        .with_acceleration(Acceleration {
            enabled: true,
            engine: Engine::Cayenne,
            mode: Mode::File,
            refresh_mode: Some(refresh_mode),
            params: [
                (
                    "cayenne_file_path".to_string(),
                    dir.join("data").to_string_lossy().into_owned(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    dir.join("metadata").to_string_lossy().into_owned(),
                ),
                // Write the rows to data files rather than inlining them in the
                // metastore, as a refresh larger than the inline caps does.
                ("cayenne_inline_max_rows".to_string(), "0".to_string()),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        });
    let registry = AcceleratorEngineRegistry::new();
    registry
        .register_accelerator_engine(Engine::Cayenne, Arc::new(CayenneAccelerator::new()))
        .await;
    let acceleration = source.acceleration().expect("acceleration").clone();
    registry
        .create_accelerator_table(
            TableReference::bare("t"),
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            None,
            &acceleration,
            source.secrets(),
            Some(&source),
            Arc::new(ctx.clone()),
        )
        .await
        .map_err(|error| error.to_string())
}

/// A session configured like `spiced`'s for the check that matters here. Cayenne tags
/// its table schema with `spice.accelerator`, which `DataFusion`'s physical aggregate
/// schema check rejects (apache/datafusion#12733), so `spiced` skips that check.
fn session() -> SessionContext {
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .skip_physical_aggregate_schema_check = true;
    SessionContext::new_with_config(config)
}

/// Loads [`ROWS`] rows into `t` the way a full refresh does, then closes it.
async fn store(dir: &Path, metastore: Metastore) {
    let ctx = session();
    let table = table(&ctx, dir, RefreshMode::Full, metastore)
        .await
        .expect("create the acceleration");
    ctx.register_table("t", table).expect("register");
    ctx.sql(&format!(
        "INSERT OVERWRITE t SELECT value AS id FROM generate_series(1, {ROWS})"
    ))
    .await
    .expect("plan")
    .collect()
    .await
    .expect("load");
    assert_eq!(count(&ctx).await, ROWS);
    // The flag selected the metastore under test: the object-native catalog keeps
    // its head log in `{metadata_dir}/object_native`, and SQLite creates none.
    assert_eq!(
        dir.join("metadata").join("object_native").is_dir(),
        matches!(metastore, Metastore::ObjectNative),
        "{metastore:?}: wrong metastore under {}",
        dir.display()
    );
}

async fn count(ctx: &SessionContext) -> i64 {
    let batches = ctx
        .sql("SELECT COUNT(*) FROM t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("rows");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 count")
        .value(0)
}

/// Every Vortex data file stored under `dir`.
fn data_files(dir: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read a data directory") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                walk(&path, files);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "vortex")
            {
                files.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(&dir.join("data"), &mut files);
    files.sort();
    assert!(!files.is_empty(), "the load wrote no data files");
    files
}

/// A restart that finds every file in place serves every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_healthy_acceleration_reopens_with_every_row() {
    for metastore in METASTORES {
        for refresh_mode in [RefreshMode::Full, RefreshMode::Append] {
            let dir = tempfile::tempdir().expect("dir");
            store(dir.path(), metastore).await;
            let ctx = session();
            let table = table(&ctx, dir.path(), refresh_mode, metastore)
                .await
                .expect("reopen");
            ctx.register_table("t", table).expect("register");
            assert_eq!(count(&ctx).await, ROWS, "{metastore:?} {refresh_mode:?}");
        }
    }
}

/// Regression test: deleting a data file between restarts used to serve the
/// other files' rows as the whole table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_data_file_rebuilds_a_full_refresh_acceleration() {
    for metastore in METASTORES {
        let dir = tempfile::tempdir().expect("dir");
        store(dir.path(), metastore).await;
        let lost = data_files(dir.path()).remove(0);
        std::fs::remove_file(&lost).expect("delete a data file");

        let error = table(&session(), dir.path(), RefreshMode::Full, metastore)
            .await
            .expect_err("an acceleration missing a data file must not open as it is");
        let lost_name = lost
            .file_name()
            .expect("a file name")
            .to_string_lossy()
            .into_owned();
        assert!(
            error.starts_with(
                "the existing acceleration of dataset 't' does not match its Cayenne metastore ("
            ) && error.contains("data files the metastore records in '")
                && error.contains(&format!("are missing: {lost_name}")),
            "{metastore:?}: {error}"
        );
    }
}

/// Regression test: deleting the data directory between restarts used to
/// serve an empty table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_data_directory_is_not_served() {
    for metastore in METASTORES {
        let dir = tempfile::tempdir().expect("dir");
        store(dir.path(), metastore).await;
        std::fs::remove_dir_all(dir.path().join("data")).expect("delete the data directory");

        let error = table(&session(), dir.path(), RefreshMode::Full, metastore)
            .await
            .expect_err("an acceleration without its data must not open as it is");
        assert!(
        error.starts_with("the existing acceleration of dataset 't' does not match its Cayenne metastore (its snapshot directory '"),
        "{metastore:?}: {error}"
    );

        // An acceleration that is not rebuilt from the source fails to load instead.
        let error = table(&session(), dir.path(), RefreshMode::Append, metastore)
            .await
            .expect_err("an acceleration without its data must not open as it is");
        assert!(
        error.contains("Failed to load table 't': its acceleration does not match its Cayenne metastore (its snapshot directory '"),
        "{metastore:?}: {error}"
    );
    }
}

/// Regression test: a truncated data file used to open, and fail only the
/// queries that read it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_truncated_data_file_is_not_served() {
    for metastore in METASTORES {
        let dir = tempfile::tempdir().expect("dir");
        store(dir.path(), metastore).await;
        let file = data_files(dir.path()).remove(0);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&file)
            .expect("open a data file")
            .set_len(100)
            .expect("truncate it");

        let error = table(&session(), dir.path(), RefreshMode::Full, metastore)
            .await
            .expect_err("an acceleration with a truncated file must not open as it is");
        assert!(
            error.contains("differ in size from the metastore's record")
                && error.contains("(100 bytes, recorded "),
            "{metastore:?}: {error}"
        );

        let error = table(&session(), dir.path(), RefreshMode::Append, metastore)
            .await
            .expect_err("an acceleration with a truncated file must not open as it is");
        assert!(
            error.contains("Failed to load table 't'"),
            "{metastore:?}: {error}"
        );
    }
}
