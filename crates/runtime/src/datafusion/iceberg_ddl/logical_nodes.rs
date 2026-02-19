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

//! Custom logical plan nodes for Iceberg DDL operations.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use iceberg::{Catalog, NamespaceIdent};
use spicepod::acceleration::Acceleration;

use super::acceleration_options::DatasetOptions;

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

/// Logical plan node for `CREATE TABLE` on an Iceberg catalog.
#[derive(Debug)]
pub struct IcebergCreateTableNode {
    /// The Iceberg catalog to create the table in.
    pub catalog: Arc<dyn Catalog>,
    /// The namespace (schema) to create the table in.
    pub namespace: NamespaceIdent,
    /// The table name.
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
    /// Acceleration options from `WITH (acceleration.*)`, if any.
    pub acceleration: Option<Acceleration>,
    /// Dataset options from `WITH (dataset.*)`, if any.
    pub dataset_options: DatasetOptions,
    /// Output schema (single "result" column).
    output_schema: DFSchemaRef,
}

impl IcebergCreateTableNode {
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        table_name: String,
        arrow_schema: Arc<Schema>,
        if_not_exists: bool,
        or_replace: bool,
        df_catalog_name: String,
        df_schema_name: String,
        acceleration: Option<Acceleration>,
        dataset_options: DatasetOptions,
    ) -> Self {
        Self {
            catalog,
            namespace,
            table_name,
            arrow_schema,
            if_not_exists,
            or_replace,
            df_catalog_name,
            df_schema_name,
            acceleration,
            dataset_options,
            output_schema: ddl_output_schema(),
        }
    }
}

impl Hash for IcebergCreateTableNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.namespace.to_string().hash(state);
        self.df_catalog_name.hash(state);
        self.df_schema_name.hash(state);
    }
}

impl PartialEq for IcebergCreateTableNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.namespace == other.namespace
            && self.df_catalog_name == other.df_catalog_name
            && self.df_schema_name == other.df_schema_name
    }
}

impl Eq for IcebergCreateTableNode {}

impl PartialOrd for IcebergCreateTableNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.table_name.partial_cmp(&other.table_name)
    }
}

impl UserDefinedLogicalNodeCore for IcebergCreateTableNode {
    fn name(&self) -> &'static str {
        "IcebergCreateTable"
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
            "IcebergCreateTable: {}.{}.{}",
            self.df_catalog_name, self.df_schema_name, self.table_name
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            catalog: Arc::clone(&self.catalog),
            namespace: self.namespace.clone(),
            table_name: self.table_name.clone(),
            arrow_schema: Arc::clone(&self.arrow_schema),
            if_not_exists: self.if_not_exists,
            or_replace: self.or_replace,
            df_catalog_name: self.df_catalog_name.clone(),
            df_schema_name: self.df_schema_name.clone(),
            acceleration: self.acceleration.clone(),
            dataset_options: self.dataset_options.clone(),
            output_schema: DFSchemaRef::clone(&self.output_schema),
        })
    }
}

/// Logical plan node for `DROP TABLE` on an Iceberg catalog.
#[derive(Debug)]
pub struct IcebergDropTableNode {
    /// The Iceberg catalog to drop the table from.
    pub catalog: Arc<dyn Catalog>,
    /// The namespace (schema) the table is in.
    pub namespace: NamespaceIdent,
    /// The table name.
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

impl IcebergDropTableNode {
    #[must_use]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        table_name: String,
        if_exists: bool,
        df_catalog_name: String,
        df_schema_name: String,
    ) -> Self {
        Self {
            catalog,
            namespace,
            table_name,
            if_exists,
            df_catalog_name,
            df_schema_name,
            output_schema: ddl_output_schema(),
        }
    }
}

impl Hash for IcebergDropTableNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.namespace.to_string().hash(state);
        self.df_catalog_name.hash(state);
    }
}

impl PartialEq for IcebergDropTableNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.namespace == other.namespace
            && self.df_catalog_name == other.df_catalog_name
            && self.df_schema_name == other.df_schema_name
    }
}

impl Eq for IcebergDropTableNode {}

impl PartialOrd for IcebergDropTableNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.table_name.partial_cmp(&other.table_name)
    }
}

impl UserDefinedLogicalNodeCore for IcebergDropTableNode {
    fn name(&self) -> &'static str {
        "IcebergDropTable"
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
            "IcebergDropTable: {}.{}.{}",
            self.df_catalog_name, self.df_schema_name, self.table_name
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            catalog: Arc::clone(&self.catalog),
            namespace: self.namespace.clone(),
            table_name: self.table_name.clone(),
            if_exists: self.if_exists,
            df_catalog_name: self.df_catalog_name.clone(),
            df_schema_name: self.df_schema_name.clone(),
            output_schema: DFSchemaRef::clone(&self.output_schema),
        })
    }
}
