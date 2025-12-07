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

use cayenne::{CayenneCatalog, MetadataCatalog};
use std::sync::Arc;
use tempfile::TempDir;

/// Backend type for parameterized tests
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
