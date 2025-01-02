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

use std::sync::Arc;

use datafusion::sql::unparser::dialect::{Dialect, DuckDBDialect, ScalarFnToSqlHandler};

mod duckdb;

/// Creates a new instance of the `DuckDB` dialect with support for Spice internal UDFs
pub fn new_duckdb_dialect() -> Arc<dyn Dialect> {
    let dialect = DuckDBDialect::new().with_custom_scalar_overrides(vec![(
        "cosine_distance",
        Box::new(duckdb::cosine_distance_to_sql) as ScalarFnToSqlHandler,
    )]);

    Arc::new(dialect) as Arc<dyn Dialect>
}
