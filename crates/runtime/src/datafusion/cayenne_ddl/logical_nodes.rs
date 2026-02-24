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

//! Custom logical plan nodes for Cayenne DDL operations.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

/// Creates the shared output schema for DDL result nodes (single `result` column).
fn ddl_output_schema() -> DFSchemaRef {
    DFSchemaRef::new(
        datafusion::common::DFSchema::try_from(Schema::new(vec![Field::new(
            "result",
            DataType::Utf8,
            false,
        )]))
        .unwrap_or_else(|e| unreachable!("fixed DDL output schema must be valid: {e}")),
    )
}

/// Logical plan node for `CREATE TABLE` on a Cayenne catalog.
#[derive(Debug)]
pub struct CayenneCreateTableNode {
    /// The table name to create.
    pub table_name: String,
    /// The Arrow schema for the new table.
    pub arrow_schema: Arc<Schema>,
    /// If true, do not error if the table already exists.
    pub if_not_exists: bool,
    /// If true, replace the table if it already exists.
    pub or_replace: bool,
    /// The `DataFusion` catalog name (for registering the table provider).
    pub df_catalog_name: String,
    /// The `DataFusion` schema name (for registering the table provider).
    pub df_schema_name: String,
    /// Output schema (single "result" column).
    output_schema: DFSchemaRef,
}

impl CayenneCreateTableNode {
    #[must_use]
    pub fn new(
        table_name: String,
        arrow_schema: Arc<Schema>,
        if_not_exists: bool,
        or_replace: bool,
        df_catalog_name: String,
        df_schema_name: String,
    ) -> Self {
        Self {
            table_name,
            arrow_schema,
            if_not_exists,
            or_replace,
            df_catalog_name,
            df_schema_name,
            output_schema: ddl_output_schema(),
        }
    }
}

impl Hash for CayenneCreateTableNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.df_catalog_name.hash(state);
        self.df_schema_name.hash(state);
    }
}

impl PartialEq for CayenneCreateTableNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.df_catalog_name == other.df_catalog_name
            && self.df_schema_name == other.df_schema_name
    }
}

impl Eq for CayenneCreateTableNode {}

impl PartialOrd for CayenneCreateTableNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.table_name.partial_cmp(&other.table_name)
    }
}

impl UserDefinedLogicalNodeCore for CayenneCreateTableNode {
    fn name(&self) -> &'static str {
        "CayenneCreateTable"
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
            "CayenneCreateTable: {}.{}.{}",
            self.df_catalog_name, self.df_schema_name, self.table_name
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            table_name: self.table_name.clone(),
            arrow_schema: Arc::clone(&self.arrow_schema),
            if_not_exists: self.if_not_exists,
            or_replace: self.or_replace,
            df_catalog_name: self.df_catalog_name.clone(),
            df_schema_name: self.df_schema_name.clone(),
            output_schema: DFSchemaRef::clone(&self.output_schema),
        })
    }
}

/// Logical plan node for `CREATE SCHEMA` on a Cayenne catalog.
#[derive(Debug)]
pub struct CayenneCreateSchemaNode {
    /// The schema name to create.
    pub schema_name: String,
    /// If true, do not error if the schema already exists.
    pub if_not_exists: bool,
    /// The `DataFusion` catalog name.
    pub df_catalog_name: String,
    /// Output schema (single "result" column).
    output_schema: DFSchemaRef,
}

impl CayenneCreateSchemaNode {
    #[must_use]
    pub fn new(schema_name: String, if_not_exists: bool, df_catalog_name: String) -> Self {
        Self {
            schema_name,
            if_not_exists,
            df_catalog_name,
            output_schema: ddl_output_schema(),
        }
    }
}

impl Hash for CayenneCreateSchemaNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema_name.hash(state);
        self.df_catalog_name.hash(state);
    }
}

impl PartialEq for CayenneCreateSchemaNode {
    fn eq(&self, other: &Self) -> bool {
        self.schema_name == other.schema_name && self.df_catalog_name == other.df_catalog_name
    }
}

impl Eq for CayenneCreateSchemaNode {}

impl PartialOrd for CayenneCreateSchemaNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.schema_name.partial_cmp(&other.schema_name)
    }
}

impl UserDefinedLogicalNodeCore for CayenneCreateSchemaNode {
    fn name(&self) -> &'static str {
        "CayenneCreateSchema"
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
            "CayenneCreateSchema: {}.{}",
            self.df_catalog_name, self.schema_name
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            schema_name: self.schema_name.clone(),
            if_not_exists: self.if_not_exists,
            df_catalog_name: self.df_catalog_name.clone(),
            output_schema: DFSchemaRef::clone(&self.output_schema),
        })
    }
}

/// Logical plan node for `DROP TABLE` on a Cayenne catalog.
#[derive(Debug)]
pub struct CayenneDropTableNode {
    /// The table name to drop.
    pub table_name: String,
    /// If true, do not error if the table does not exist.
    pub if_exists: bool,
    /// The `DataFusion` catalog name.
    pub df_catalog_name: String,
    /// The `DataFusion` schema name.
    pub df_schema_name: String,
    /// Output schema (single "result" column).
    output_schema: DFSchemaRef,
}

impl CayenneDropTableNode {
    #[must_use]
    pub fn new(
        table_name: String,
        if_exists: bool,
        df_catalog_name: String,
        df_schema_name: String,
    ) -> Self {
        Self {
            table_name,
            if_exists,
            df_catalog_name,
            df_schema_name,
            output_schema: ddl_output_schema(),
        }
    }
}

impl Hash for CayenneDropTableNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.df_catalog_name.hash(state);
    }
}

impl PartialEq for CayenneDropTableNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.df_catalog_name == other.df_catalog_name
            && self.df_schema_name == other.df_schema_name
    }
}

impl Eq for CayenneDropTableNode {}

impl PartialOrd for CayenneDropTableNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.table_name.partial_cmp(&other.table_name)
    }
}

impl UserDefinedLogicalNodeCore for CayenneDropTableNode {
    fn name(&self) -> &'static str {
        "CayenneDropTable"
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
            "CayenneDropTable: {}.{}.{}",
            self.df_catalog_name, self.df_schema_name, self.table_name
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            table_name: self.table_name.clone(),
            if_exists: self.if_exists,
            df_catalog_name: self.df_catalog_name.clone(),
            df_schema_name: self.df_schema_name.clone(),
            output_schema: DFSchemaRef::clone(&self.output_schema),
        })
    }
}
