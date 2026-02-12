/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    accelerated_table::AcceleratedTable,
    cluster::partition::{
        Error, PartitionDiscoverySnafu, RegisterTableSnafu, Result, metadata::PartitionValue,
    },
    datafusion::DataFusion,
};
use datafusion::{execution::SessionStateBuilder, prelude::SessionContext, sql::TableReference};
use snafu::prelude::*;
use spicepod::partitioning::PartitionedBy;

/// Query the source table provider for partition values for a given table.
///
/// This builds a SQL query to get distinct values from the partition columns.
pub async fn table_partition_values(
    table: &TableReference,
    partitioning: &[PartitionedBy],
    df: &Arc<DataFusion>,
) -> Result<Vec<PartitionValue>> {
    let table_name = table.to_string();

    // Build SQL query to get distinct partition values
    // For single partition column: SELECT DISTINCT partition_col as  FROM table
    // For multiple columns: SELECT DISTINCT partition_col1, partition_col2, ... FROM table
    let partition_exprs: Vec<String> = partitioning
        .iter()
        .map(|p| {
            let PartitionedBy { name, expression } = p;
            format!("{expression} AS {name}")
        })
        .collect();

    if partition_exprs.is_empty() {
        return Ok(Vec::new());
    }

    let cols_str = partition_exprs.join(", ");
    let sql = format!("SELECT DISTINCT {cols_str} FROM {table_name}");

    tracing::debug!(
        table = %table_name,
        sql = %sql,
        "Querying for partition values"
    );

    let batches = execute_partition_discovery_query(df, table, &sql).await?;

    // Convert record batches to partition value strings
    let mut partition_values = Vec::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            // Build partition value string from column values
            let mut value_parts = HashMap::new();

            for col_idx in 0..num_cols {
                let column = batch.column(col_idx);
                let value_str = arrow::util::display::array_value_to_string(column, row_idx)
                    .boxed()
                    .context(PartitionDiscoverySnafu {
                        table: table_name.clone(),
                    })?;
                if let Some(pname) = partitioning.get(col_idx).map(|p| p.expression.clone()) {
                    value_parts.insert(pname, value_str);
                }
            }

            partition_values.push(value_parts);
        }
    }

    tracing::debug!(
        table = %table_name,
        partition_count = partition_values.len(),
        "Discovered partition values"
    );

    Ok(partition_values)
}

/// Executes a SQL query against the underlying table source of an accelerated dataset to discover partition values.
///
/// This function creates a temporary, isolated `SessionContext` to execute the query. It is critical
/// to query the *federated* table (the source) rather than the accelerated table itself, as the
/// acceleration will be empty (for schedulers).
async fn execute_partition_discovery_query(
    df: &Arc<DataFusion>,
    table: &TableReference,
    sql: &str,
) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    let table_name = table.to_string();

    // Wait for table to be registered.
    // TODO: we should call `initialize_partition_metadata` after all datasets registered.
    if !wait_for_table(table, df).await {
        return Err(Error::TableRegistrationTimeout { table: table_name });
    }

    // Must get table source of `AcceleratedTable` to get true value of partition.
    let Some(acc) = df.get_table(table).await.and_then(|t| {
        t.as_any()
            .downcast_ref::<AcceleratedTable>()
            .map(AcceleratedTable::get_federated_table)
    }) else {
        return Err(Error::NotAcceleratedTable {
            table: table.to_string(),
        });
    };

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(df.ctx.state()).build(),
    );

    // Must deregister table in this context before registering source table.
    let _ = ctx.deregister_table(table.clone());
    ctx.register_table(table.clone(), acc.table_provider().await)
        .context(RegisterTableSnafu {
            table: table_name.clone(),
        })?;

    // Execute query
    let batches = ctx
        .sql(sql)
        .await
        .boxed()
        .context(PartitionDiscoverySnafu {
            table: table_name.clone(),
        })?
        .collect()
        .await
        .boxed()
        .context(PartitionDiscoverySnafu { table: table_name })?;

    Ok(batches)
}

/// Wait for the [`TableReference`] to be registered in Runtime.
async fn wait_for_table(table: &TableReference, df: &Arc<DataFusion>) -> bool {
    for _ in 0..5 {
        if df.table_exists(table.clone()) {
            return true;
        }
        let () = tokio::time::sleep(Duration::from_secs(1)).await;
    }
    false
}
