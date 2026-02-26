/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Common test utilities for Cayenne with multiple metastore backends

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_common::Result as DFResult;
use datafusion_expr::dml::InsertOp;
use tempfile::TempDir;

/// Backend type for parameterized tests
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(dead_code)]
pub enum BackendType {
    Sqlite,
    #[cfg(feature = "turso")]
    Turso,
}

impl BackendType {
    #[expect(dead_code, clippy::allow_attributes)]
    #[allow(unfulfilled_lint_expectations)]
    pub fn name(self) -> &'static str {
        match self {
            BackendType::Sqlite => "SQLite",
            #[cfg(feature = "turso")]
            BackendType::Turso => "Turso",
        }
    }
}

/// Test fixture that sets up a temporary directory and catalog
#[expect(dead_code)]
pub struct TestFixture {
    // this is only used in 1 of the tests, but is imported in all test files
    // hence, it is dead everywhere else
    #[expect(dead_code, clippy::allow_attributes)]
    #[allow(unfulfilled_lint_expectations)]
    pub temp_dir: TempDir,
    pub catalog: Arc<CayenneCatalog>,
    pub data_path: std::path::PathBuf,
    #[expect(dead_code, clippy::allow_attributes)]
    #[allow(unfulfilled_lint_expectations)]
    pub backend_type: BackendType,
}

impl TestFixture {
    /// Create a new test fixture with the specified backend
    pub async fn new(backend: BackendType) -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let data_path = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_path)?;

        let connection_string = match backend {
            BackendType::Sqlite => {
                let db_path = temp_dir.path().join("test.db");
                format!("sqlite://{}", db_path.to_string_lossy())
            }
            #[cfg(feature = "turso")]
            BackendType::Turso => {
                let db_path = temp_dir.path().join("test.db");
                format!("libsql://{}", db_path.to_string_lossy())
            }
        };

        let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
        catalog.init().await?;

        Ok(Self {
            temp_dir,
            catalog,
            data_path,
            backend_type: backend,
        })
    }

    /// Get the database path for SQLite-specific verification
    #[expect(dead_code, clippy::allow_attributes)]
    #[allow(unfulfilled_lint_expectations)]
    pub fn db_path(&self) -> std::path::PathBuf {
        self.temp_dir.path().join("test.db")
    }
}

/// Run a test with all available backends
#[macro_export]
macro_rules! test_with_backends {
    ($test_fn:ident) => {
        paste::paste! {
            #[tokio::test]
            async fn [<$test_fn _sqlite>]() -> Result<(), Box<dyn std::error::Error>> {
                tracing::debug!("\n🔧 Running {} with SQLite backend", stringify!($test_fn));
                common::run_with_backend(common::BackendType::Sqlite, $test_fn).await
            }

            #[cfg(feature = "turso")]
            #[tokio::test]
            async fn [<$test_fn _turso>]() -> Result<(), Box<dyn std::error::Error>> {
                tracing::debug!("\n🔧 Running {} with Turso backend", stringify!($test_fn));
                common::run_with_backend(common::BackendType::Turso, $test_fn).await
            }
        }
    };
}

/// Helper to run a test function with a specific backend
#[expect(dead_code)]
pub async fn run_with_backend<F, Fut>(
    backend: BackendType,
    test_fn: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(TestFixture) -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let fixture = TestFixture::new(backend).await?;
    test_fn(fixture).await
}

// ============================================================================
// Insert Helper Functions
// ============================================================================
//
// These helpers wrap the `CayenneTableProvider::insert_into()` API for tests.
// They ensure write logic goes through `CayenneDataSink::write_all()`.

/// Insert a single batch using `insert_into()` (append mode).
///
/// Creates a temporary `SessionContext` internally.
#[expect(dead_code)]
pub async fn insert_batch(provider: &CayenneTableProvider, batch: RecordBatch) -> DFResult<u64> {
    insert_batches(provider, vec![batch]).await
}

/// Insert record batches using `insert_into()` API (append mode).
///
/// Creates a temporary `SessionContext` internally.
#[expect(dead_code)]
pub async fn insert_batches(
    provider: &CayenneTableProvider,
    batches: Vec<RecordBatch>,
) -> DFResult<u64> {
    use datafusion::physical_plan::collect;

    if batches.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "Cannot insert empty batches".to_string(),
        ));
    }

    let ctx = SessionContext::new();
    let schema = Arc::clone(batches[0].schema_ref());
    let input_exec = MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
    let insert_plan = provider
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await?;
    let results = collect(insert_plan, ctx.task_ctx()).await?;

    Ok(extract_row_count(&results))
}

/// Extract the row count from insert result batches.
fn extract_row_count(results: &[RecordBatch]) -> u64 {
    use arrow::datatypes::DataType;

    if results.is_empty() {
        return 0;
    }
    let batch = &results[0];
    if batch.num_columns() == 0 || batch.num_rows() == 0 {
        return 0;
    }
    let col = batch.column(0);
    match col.data_type() {
        DataType::UInt64 => col
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .map_or(0, |a| a.value(0)),
        _ => 0,
    }
}
