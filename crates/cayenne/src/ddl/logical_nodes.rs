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

//! `CayenneMergeNode` — logical plan node for local MERGE execution.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::sql::TableReference;

/// Logical plan node for a local `MERGE INTO ... USING ... ON ... WHEN MATCHED
/// THEN UPDATE SET ...` operation on a Cayenne table.
///
/// Produced by the statement planner when a MERGE targets a Cayenne table in
/// local (non-distributed) mode. The extension planner converts this into a
/// `CayenneMergeExec` that builds a `DataFusion` join plan and executes
/// delete + insert.
#[derive(Debug, Clone)]
pub struct CayenneMergeNode {
    /// Fully qualified target table reference.
    pub target_table: TableReference,
    /// Fully qualified source table reference.
    pub source_table: TableReference,
    /// Scan qualifier for the target side — the alias if provided, otherwise
    /// the table name. Assignment value SQL (e.g. `t.qty + 1`) references
    /// this qualifier, so the extension planner must use it for schema
    /// resolution.
    pub target_qualifier: String,
    /// Scan qualifier for the source side.
    pub source_qualifier: String,
    /// Equi-join key pairs as `(target_col, source_col)` bare column names,
    /// normalized from the ON clause at plan time.
    pub on_keys: Vec<(String, String)>,
    /// SET assignments as `(target_col, value_sql)` pairs.
    /// The `value_sql` is the raw SQL text of the assignment expression,
    /// resolved against the joined schema at physical planning time.
    pub assignments: Vec<(String, String)>,
    /// Output schema: single `count: UInt64` column.
    pub output_schema: DFSchemaRef,
}

impl CayenneMergeNode {
    /// Create a new `CayenneMergeNode`.
    ///
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
    ) -> datafusion::error::Result<Self> {
        let output_schema = Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))?);

        Ok(Self {
            target_table,
            source_table,
            target_qualifier,
            source_qualifier,
            on_keys,
            assignments,
            output_schema,
        })
    }
}

impl Hash for CayenneMergeNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.target_table.hash(state);
        self.source_table.hash(state);
        self.target_qualifier.hash(state);
        self.source_qualifier.hash(state);
        self.on_keys.hash(state);
        self.assignments.hash(state);
    }
}

impl PartialEq for CayenneMergeNode {
    fn eq(&self, other: &Self) -> bool {
        self.target_table == other.target_table
            && self.source_table == other.source_table
            && self.target_qualifier == other.target_qualifier
            && self.source_qualifier == other.source_qualifier
            && self.on_keys == other.on_keys
            && self.assignments == other.assignments
    }
}

impl Eq for CayenneMergeNode {}

impl PartialOrd for CayenneMergeNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.target_table
            .to_string()
            .partial_cmp(&other.target_table.to_string())
    }
}

impl UserDefinedLogicalNodeCore for CayenneMergeNode {
    fn name(&self) -> &'static str {
        "CayenneMerge"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CayenneMerge: target={}, source={}, keys={:?}, assignments={:?}",
            self.target_table, self.source_table, self.on_keys, self.assignments
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(self.clone())
    }
}
