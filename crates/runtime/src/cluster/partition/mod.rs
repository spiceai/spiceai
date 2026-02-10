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

mod manager;
mod metadata;
mod startup;

use std::collections::HashMap;

use datafusion::sql::{TableReference, unparser::expr_to_sql};
use datafusion_expr::Expr;
pub use manager::PartitionManager;
pub use metadata::{PartitionMetadata, TablePartitionMetadata, partition_value_to_bytes};
pub use startup::{
    accelerated_tables, build_partition_metadata_store, executor_request_initial_partitions,
    initialize_partition_metadata,
};

#[expect(clippy::implicit_hasher)]
pub fn update_partitioning_filter_in_refresh_sql(
    current_sql: Option<&str>,
    tbl: &TableReference,
    assignments: &HashMap<TableReference, Vec<Expr>>,
) -> Result<Option<String>, datafusion::error::DataFusionError> {
    let partitions = assignments.get(tbl).cloned().unwrap_or_default();
    if partitions.is_empty() {
        return Ok(current_sql.map(ToString::to_string));
    }
    let filter_expr = partitions
        .iter()
        .cloned()
        .reduce(Expr::or)
        .unwrap_or_else(|| unreachable!("partitions is not empty"));

    let filter_sql = expr_to_sql(&filter_expr).map(|ast| ast.to_string())?;

    let sql = if let Some(sql) = current_sql {
        format!("SELECT * FROM ({sql}) AS _partitioned_source WHERE {filter_sql}")
    } else {
        format!("SELECT * FROM {tbl} WHERE {filter_sql}")
    };
    Ok(Some(sql))
}
