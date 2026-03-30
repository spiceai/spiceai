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

use crate::args::SchemaTestArgs;
use std::sync::Arc;
use test_framework::{
    anyhow,
    arrow::util::pretty::{pretty_format_batches, print_batches},
    flight::query_to_batches,
};

/// Derive a snapshot-friendly name from the spicepod path.
///
/// For example, `federated/postgres[catalog].yaml` becomes `postgres[catalog]`.
fn snapshot_name_from_spicepod(spicepod_path: &std::path::Path) -> String {
    spicepod_path.file_stem().map_or_else(
        || "unknown".to_string(),
        |s| s.to_string_lossy().to_string(),
    )
}

/// Run a schema test that queries the `information_schema` for all tables and their columns
/// in the catalogs defined by the spicepod.
///
/// This validates that catalog connectors correctly discover and register tables and schemas.
/// Results are captured as insta snapshots for regression testing.
pub(crate) async fn run(args: &SchemaTestArgs) -> anyhow::Result<()> {
    let (_, instance) = super::run_or_connect_spiced(&args.common).await?;

    let spice_client = Arc::new(instance.spice_client(None, false).await?);
    let name = snapshot_name_from_spicepod(&args.common.spicepod_path);

    // Query information_schema.tables to discover all tables from catalogs
    println!("Querying information_schema.tables...");
    let tables_sql = "SELECT table_catalog, table_schema, table_name, table_type \
                      FROM information_schema.tables \
                      ORDER BY table_catalog, table_schema, table_name";

    let table_batches = query_to_batches(Arc::clone(&spice_client), tables_sql, None).await?;
    let total_table_rows: usize = table_batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    println!("\n=== Tables ({total_table_rows} found) ===");
    print_batches(&table_batches)?;

    if total_table_rows == 0 {
        return Err(anyhow::anyhow!(
            "Schema test failed: no tables found in information_schema.tables. Catalog connector may not have registered any tables."
        ));
    }

    // Snapshot the tables result
    let tables_pretty = pretty_format_batches(&table_batches)?;
    {
        let snapshot_name = format!("{name}_schema_tables");
        insta::with_settings!({
            description => format!("Schema tables for {name}"),
            omit_expression => true,
            snapshot_path => "snapshots/schema",
        }, {
            insta::assert_snapshot!(snapshot_name, tables_pretty.to_string());
        });
    }

    // Query information_schema.columns to get all column schemas
    println!("\nQuerying information_schema.columns...");
    let columns_sql = "SELECT table_catalog, table_schema, table_name, column_name, \
                       ordinal_position, data_type, is_nullable \
                       FROM information_schema.columns \
                       ORDER BY table_catalog, table_schema, table_name, ordinal_position";

    let column_batches = query_to_batches(Arc::clone(&spice_client), columns_sql, None).await?;
    let total_column_rows: usize = column_batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    println!("\n=== Columns ({total_column_rows} found) ===");
    print_batches(&column_batches)?;

    if total_column_rows == 0 {
        return Err(anyhow::anyhow!(
            "Schema test failed: no columns found in information_schema.columns. Tables were discovered but their schemas were not registered."
        ));
    }

    // Snapshot the columns result
    let columns_pretty = pretty_format_batches(&column_batches)?;
    {
        let snapshot_name = format!("{name}_schema_columns");
        insta::with_settings!({
            description => format!("Schema columns for {name}"),
            omit_expression => true,
            snapshot_path => "snapshots/schema",
        }, {
            insta::assert_snapshot!(snapshot_name, columns_pretty.to_string());
        });
    }

    // If a minimum table count is specified, validate against it
    if let Some(min_tables) = args.min_tables {
        if total_table_rows < min_tables {
            return Err(anyhow::anyhow!(
                "Schema test failed: expected at least {min_tables} tables but found {total_table_rows}"
            ));
        }
        println!("\nValidation passed: found {total_table_rows} tables (minimum: {min_tables})");
    }

    println!(
        "\nSchema test passed: {total_table_rows} tables with {total_column_rows} columns discovered"
    );
    Ok(())
}
