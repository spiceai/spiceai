/*
Copyright 2026, Spice AI, Inc.

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

//! Distributed DML logical plan nodes for Cayenne.
//!
//! DDL nodes (`CayenneCreateTableNode`, `CayenneDropTableNode`, `CayenneCreateSchemaNode`)
//! have moved to `cayenne::ddl::logical_nodes` and are re-exported from `super`.
//!
//! This file contains only the distributed DML nodes that depend on cluster-level
//! runtime types.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore, dml::InsertOp};
use datafusion::sql::TableReference;

// ── DistributedCayenneDeleteNode ──────────────────────────────────────────────

/// Logical plan node to forward `DELETE` DML to Cayenne executors in distributed mode.
#[derive(Debug)]
pub struct DistributedCayenneDeleteNode {
    pub table_name: TableReference,
    pub input: Arc<LogicalPlan>,
    pub output_schema: DFSchemaRef,
    pub filters: Vec<Expr>,
}

impl DistributedCayenneDeleteNode {
    #[must_use]
    pub fn new(
        table_name: TableReference,
        input: Arc<LogicalPlan>,
        output_schema: DFSchemaRef,
        filters: Vec<Expr>,
    ) -> Self {
        Self {
            table_name,
            input,
            output_schema,
            filters,
        }
    }
}

impl Hash for DistributedCayenneDeleteNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.input.hash(state);
        self.output_schema.hash(state);
        self.filters.hash(state);
    }
}
impl PartialEq for DistributedCayenneDeleteNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.input == other.input
            && self.output_schema == other.output_schema
            && self.filters == other.filters
    }
}
impl Eq for DistributedCayenneDeleteNode {}
impl PartialOrd for DistributedCayenneDeleteNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.table_name
            .to_string()
            .partial_cmp(&other.table_name.to_string())
    }
}

impl UserDefinedLogicalNodeCore for DistributedCayenneDeleteNode {
    fn name(&self) -> &'static str {
        "CayenneDelete"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CayenneDelete: {}", self.table_name)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        let input = inputs.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "CayenneDeleteNode requires exactly one input".to_string(),
            )
        })?;
        Ok(Self {
            table_name: self.table_name.clone(),
            input: Arc::new(input),
            output_schema: DFSchemaRef::clone(&self.output_schema),
            filters: self.filters.clone(),
        })
    }
}

// ── DistributedCayenneUpdateNode ──────────────────────────────────────────────

/// Logical plan node to forward `UPDATE` DML to Cayenne executors in distributed mode.
#[derive(Debug)]
pub struct DistributedCayenneUpdateNode {
    pub table_name: TableReference,
    pub input: Arc<LogicalPlan>,
    pub output_schema: DFSchemaRef,
    pub filters: Vec<Expr>,
    pub assignments: Vec<(String, Expr)>,
}

impl DistributedCayenneUpdateNode {
    #[must_use]
    pub fn new(
        table_name: TableReference,
        input: Arc<LogicalPlan>,
        output_schema: DFSchemaRef,
        filters: Vec<Expr>,
        assignments: Vec<(String, Expr)>,
    ) -> Self {
        Self {
            table_name,
            input,
            output_schema,
            filters,
            assignments,
        }
    }
}

impl Hash for DistributedCayenneUpdateNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.input.hash(state);
        self.output_schema.hash(state);
        self.filters.hash(state);
        self.assignments.hash(state);
    }
}
impl PartialEq for DistributedCayenneUpdateNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.input == other.input
            && self.output_schema == other.output_schema
            && self.filters == other.filters
            && self.assignments == other.assignments
    }
}
impl Eq for DistributedCayenneUpdateNode {}
impl PartialOrd for DistributedCayenneUpdateNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.table_name
            .to_string()
            .partial_cmp(&other.table_name.to_string())
    }
}

impl UserDefinedLogicalNodeCore for DistributedCayenneUpdateNode {
    fn name(&self) -> &'static str {
        "CayenneUpdate"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CayenneUpdate: {}", self.table_name)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        let input = inputs.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "CayenneUpdateNode requires exactly one input".to_string(),
            )
        })?;
        Ok(Self {
            table_name: self.table_name.clone(),
            input: Arc::new(input),
            output_schema: DFSchemaRef::clone(&self.output_schema),
            filters: self.filters.clone(),
            assignments: self.assignments.clone(),
        })
    }
}

// ── DistributedCayenneInsertNode ──────────────────────────────────────────────

/// Logical plan node to forward `INSERT` DML to Cayenne executors in distributed mode.
#[derive(Debug)]
pub struct DistributedCayenneInsertNode {
    pub table_name: TableReference,
    pub input: Arc<LogicalPlan>,
    pub output_schema: DFSchemaRef,
    pub insert_op: InsertOp,
}

impl DistributedCayenneInsertNode {
    #[must_use]
    pub fn new(
        table_name: TableReference,
        input: Arc<LogicalPlan>,
        output_schema: DFSchemaRef,
        insert_op: InsertOp,
    ) -> Self {
        Self {
            table_name,
            input,
            output_schema,
            insert_op,
        }
    }
}

impl Hash for DistributedCayenneInsertNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.input.hash(state);
        self.output_schema.hash(state);
        self.insert_op.hash(state);
    }
}
impl PartialEq for DistributedCayenneInsertNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.input == other.input
            && self.output_schema == other.output_schema
            && self.insert_op == other.insert_op
    }
}
impl Eq for DistributedCayenneInsertNode {}
impl PartialOrd for DistributedCayenneInsertNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.table_name
            .to_string()
            .partial_cmp(&other.table_name.to_string())
    }
}

impl UserDefinedLogicalNodeCore for DistributedCayenneInsertNode {
    fn name(&self) -> &'static str {
        "CayenneInsert"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CayenneInsert: {}", self.table_name)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        let input = inputs.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "CayenneInsertNode requires exactly one input".to_string(),
            )
        })?;
        Ok(Self {
            table_name: self.table_name.clone(),
            input: Arc::new(input),
            output_schema: DFSchemaRef::clone(&self.output_schema),
            insert_op: self.insert_op,
        })
    }
}

// ── DistributedCayenneMergeNode ───────────────────────────────────────────────

/// Logical plan node to forward `MERGE` DML to Cayenne executors in distributed mode.
#[derive(Debug, Clone)]
pub struct DistributedCayenneMergeNode {
    pub target_table: TableReference,
    pub source_table: TableReference,
    pub target_qualifier: String,
    pub source_qualifier: String,
    pub on_keys: Vec<(String, String)>,
    pub assignments: Vec<(String, String)>,
    pub original_sql: String,
    pub output_schema: DFSchemaRef,
}

impl DistributedCayenneMergeNode {
    /// # Errors
    ///
    /// Returns an error if the output schema cannot be constructed.
    pub fn try_new(
        target_table: TableReference,
        source_table: TableReference,
        target_qualifier: String,
        source_qualifier: String,
        on_keys: Vec<(String, String)>,
        assignments: Vec<(String, String)>,
        original_sql: String,
    ) -> datafusion::error::Result<Self> {
        let output_schema = Arc::new(datafusion::common::DFSchema::try_from(Schema::new(vec![
            Field::new("count", DataType::UInt64, false),
        ]))?);
        Ok(Self {
            target_table,
            source_table,
            target_qualifier,
            source_qualifier,
            on_keys,
            assignments,
            original_sql,
            output_schema,
        })
    }
}

impl Hash for DistributedCayenneMergeNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.target_table.hash(state);
        self.source_table.hash(state);
        self.target_qualifier.hash(state);
        self.source_qualifier.hash(state);
        self.on_keys.hash(state);
        self.assignments.hash(state);
        self.original_sql.hash(state);
    }
}
impl PartialEq for DistributedCayenneMergeNode {
    fn eq(&self, other: &Self) -> bool {
        self.target_table == other.target_table
            && self.source_table == other.source_table
            && self.target_qualifier == other.target_qualifier
            && self.source_qualifier == other.source_qualifier
            && self.on_keys == other.on_keys
            && self.assignments == other.assignments
            && self.original_sql == other.original_sql
    }
}
impl Eq for DistributedCayenneMergeNode {}
impl PartialOrd for DistributedCayenneMergeNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.target_table
            .to_string()
            .partial_cmp(&other.target_table.to_string())
    }
}

impl UserDefinedLogicalNodeCore for DistributedCayenneMergeNode {
    fn name(&self) -> &'static str {
        "DistributedCayenneMerge"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedCayenneMerge: target={}, source={}, keys={:?}",
            self.target_table, self.source_table, self.on_keys
        )
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(self.clone())
    }
}
