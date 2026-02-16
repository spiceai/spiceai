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

mod discovery;
mod manager;
mod metadata;
pub mod scheduler_task;
mod startup;

use std::collections::HashMap;

use datafusion::sql::{TableReference, unparser::expr_to_sql};
use datafusion_expr::Expr;
pub use manager::PartitionManager;
pub use metadata::{
    PartitionMetadata, PartitionValue, TablePartitionMetadata, partition_value_to_bytes,
};
use snafu::Snafu;
pub use startup::{
    accelerated_tables, build_partition_metadata_store, executor_request_initial_partitions,
    initialize_partition_metadata,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build object store for partition metadata: {source}"))]
    ObjectStoreBuild {
        source: crate::cluster::scheduler_registry::Error,
    },

    #[snafu(display("Failed to initialize partition metadata for table {table}: {source}"))]
    PartitionMetadataInit {
        table: String,
        source: manager::Error,
    },

    #[snafu(display("Failed to discover partitions for table {table}: {source}"))]
    PartitionDiscovery {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Scheduler configuration is missing state_location"))]
    MissingStateLocation,

    #[snafu(display("No schedulers available to request partition allocation"))]
    NoSchedulersAvailable,

    #[snafu(display("Failed to connect to scheduler at {url}: {source}"))]
    SchedulerConnection {
        url: String,
        source: tonic::transport::Error,
    },

    #[snafu(display("Failed to request partition allocation: {source}"))]
    PartitionAllocationRequest { source: tonic::Status },

    #[snafu(display("Failed to deserialize partition expression: {source}"))]
    PartitionExpressionDeserialization {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to register table {table}: {source}"))]
    RegisterTable {
        table: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Timed out waiting for table {table} to be registered"))]
    TableRegistrationTimeout { table: String },

    #[snafu(display("Table {table} is not an accelerated table"))]
    NotAcceleratedTable { table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
