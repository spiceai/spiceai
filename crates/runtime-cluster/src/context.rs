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

//! Traits that abstract the runtime-side `DataFusion` god-struct away from this
//! crate. The runtime side implements these; this crate only holds trait objects.

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use datafusion_expr::Expr;
use spicepod::partitioning::PartitionedBy;

use crate::metadata::PartitionValue;

/// Parses a SQL partition expression against a specific table's schema.
///
/// Implemented in the runtime crate by `DataFusion::try_parse_expr`, which
/// looks up the table provider and uses its schema as the DF schema for
/// `SessionContext::parse_sql_expr`.
#[async_trait]
pub trait PartitionExprResolver: Send + Sync {
    async fn try_parse_expr(
        &self,
        tbl: &TableReference,
        expr: &str,
    ) -> Result<Expr, DataFusionError>;
}

/// Discovers the values a `table` can have for a given `partition_by` expression.
///
/// For >1 `partition_by` value, the cartesian product of individual options is returned.
#[async_trait]
pub trait PartitionDiscoverer: Send + Sync {
    async fn table_partition_values(
        &self,
        table: &TableReference,
        partition_by: &[PartitionedBy],
    ) -> Result<Vec<PartitionValue>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Combined bound for partition-management operations that need both expression
/// resolution (for serializing partition values) and source discovery (for
/// finding new partition values).
pub trait PartitionOperations: PartitionExprResolver + PartitionDiscoverer {}

impl<T: PartitionExprResolver + PartitionDiscoverer + ?Sized> PartitionOperations for T {}
