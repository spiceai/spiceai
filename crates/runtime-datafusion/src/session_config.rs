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

//! The `DataFusion` [`SessionConfig`] every Spice session starts from.

use std::sync::{LazyLock, RwLock};

use datafusion::prelude::SessionConfig;

pub static DEFAULT_DATAFUSION_CONFIG: LazyLock<RwLock<SessionConfig>> = LazyLock::new(|| {
    let mut df_config = SessionConfig::new();

    // Prevents DataFusion from lowercasing identifiers, i.e. "SELECT MyColumn FROM my_table" would be "SELECT mycolumn FROM mytable" without this.
    // This improves the UX for data sources where column names are case-sensitive, since they no longer need to be quoted.
    df_config
        .options_mut()
        .sql_parser
        .enable_ident_normalization = false;

    df_config.options_mut().optimizer.expand_views_at_output = true;
    df_config.options_mut().sql_parser.dialect = datafusion::common::config::Dialect::PostgreSQL;
    df_config
        .options_mut()
        .execution
        .listing_table_ignore_subdirectory = false;

    // There are some unidentified bugs in DataFusion that cause schema checks to fail for aggregate functions.
    // Spice is affected by this - skip the check until all bugs are fixed.
    // Tracking issue: https://github.com/apache/datafusion/issues/12733
    df_config
        .options_mut()
        .execution
        .skip_physical_aggregate_schema_check = true;

    // Enabling parquet filter pushdown can improve query performance by applying filters while decoding
    // https://docs.rs/datafusion/latest/datafusion/config/struct.ParquetOptions.html#structfield.pushdown_filters
    df_config.options_mut().execution.parquet.pushdown_filters = true;

    RwLock::new(df_config)
});

/// A clone of the process-wide default [`SessionConfig`].
///
/// # Panics
///
/// Panics if the lock guarding the default config is poisoned, which indicates a
/// bug — the config is only ever read, or written once during test setup.
#[must_use]
pub fn get_df_default_config() -> SessionConfig {
    match DEFAULT_DATAFUSION_CONFIG.read() {
        Ok(config) => config.clone(),
        _ => panic!("Failed to read default DataFusion config. This is a bug."),
    }
}
