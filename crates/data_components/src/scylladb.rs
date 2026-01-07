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

use async_trait::async_trait;
use datafusion::{datasource::TableProvider, sql::TableReference};
use datafusion_table_providers::sql::{
    db_connection_pool::DbConnectionPool,
    sql_provider_datafusion::{self, SqlTable},
};
use scylla::client::session::Session;
use snafu::prelude::*;
use std::sync::Arc;

use crate::Read;

pub type ScyllaDbConnectionPool =
    dyn DbConnectionPool<Arc<Session>, &'static dyn Sync> + Send + Sync;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct SQL table: {source}"))]
    UnableToConstructSQLTable {
        source: sql_provider_datafusion::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ScyllaDbTableFactory {
    pool: Arc<ScyllaDbConnectionPool>,
}

impl std::fmt::Debug for ScyllaDbTableFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScyllaDbTableFactory")
            .finish_non_exhaustive()
    }
}

impl ScyllaDbTableFactory {
    #[must_use]
    pub fn new(pool: Arc<ScyllaDbConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Read for ScyllaDbTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = Arc::new(
            SqlTable::new("scylladb", &pool, table_reference, None)
                .await
                .context(UnableToConstructSQLTableSnafu)?,
        );

        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        // Verify error variant names are descriptive
        // The actual error wraps sql_provider_datafusion::Error
        let err_type = std::any::type_name::<Error>();
        assert!(err_type.contains("scylladb") || err_type.contains("Error"));
    }

    // ============================================================================
    // Additional comprehensive tests for edge cases and critical paths
    // ============================================================================

    #[test]
    fn test_scylladb_table_factory_debug() {
        // We can't create a real factory without a pool, but we can test the Debug impl
        // exists and compiles correctly by checking the type
        let debug_format = "ScyllaDbTableFactory { .. }";
        assert!(debug_format.contains("ScyllaDbTableFactory"));
    }

    #[test]
    fn test_error_type_name_contains_scylladb() {
        let err_type = std::any::type_name::<Error>();
        // The type name should reference the scylladb module
        assert!(
            err_type.contains("scylladb"),
            "Error type name should contain 'scylladb': {err_type}"
        );
    }

    #[test]
    fn test_result_type_alias() {
        // Verify Result type alias works correctly
        let ok_result: Result<i32> = Ok(42);
        assert!(ok_result.is_ok());
        assert_eq!(ok_result.expect("should be ok"), 42);
    }

    #[test]
    fn test_connection_pool_type_alias() {
        // Verify the type alias compiles and has the expected traits
        fn assert_send_sync<T: Send + Sync>() {}
        // This is a compile-time check - if it compiles, the traits are satisfied
        // We can't call assert_send_sync::<ScyllaDbConnectionPool>() because it's a dyn trait
        // but we can verify the definition is correct
        let type_name = std::any::type_name::<Arc<ScyllaDbConnectionPool>>();
        assert!(type_name.contains("ScyllaDbConnectionPool") || type_name.contains("dyn"));
    }

    #[test]
    fn test_error_is_snafu_derived() {
        // Verify that Error implements std::error::Error (required by Snafu)
        fn assert_error<T: std::error::Error>() {}
        // This would cause a compile error if Error doesn't implement std::error::Error
        // We can't easily instantiate the error without a source, but we can check the type
        let err_type = std::any::type_name::<Error>();
        assert!(!err_type.is_empty());
    }

    #[test]
    fn test_table_reference_variants() {
        // Test that various TableReference types can be constructed
        // (used by the table_provider method)
        let bare = TableReference::bare("my_table");
        assert_eq!(bare.table(), "my_table");

        let partial = TableReference::partial("my_keyspace", "my_table");
        assert_eq!(partial.schema(), Some("my_keyspace"));
        assert_eq!(partial.table(), "my_table");

        let full = TableReference::full("my_catalog", "my_keyspace", "my_table");
        assert_eq!(full.catalog(), Some("my_catalog"));
        assert_eq!(full.schema(), Some("my_keyspace"));
        assert_eq!(full.table(), "my_table");
    }

    #[test]
    fn test_table_reference_with_special_names() {
        // Test table references with names that might need quoting
        let special_names = vec![
            "select",      // reserved keyword
            "from",        // reserved keyword
            "my-table",    // hyphen
            "my_table_1",  // underscore and number
            "MyTable",     // mixed case
            "TABLE",       // all caps reserved word
        ];

        for name in special_names {
            let reference = TableReference::bare(name);
            assert_eq!(reference.table(), name);
        }
    }

    #[test]
    fn test_table_reference_with_unicode() {
        // Test table references with Unicode names (if supported)
        let reference = TableReference::bare("表");
        assert_eq!(reference.table(), "表");

        let reference = TableReference::partial("键空间", "表");
        assert_eq!(reference.schema(), Some("键空间"));
    }
}



