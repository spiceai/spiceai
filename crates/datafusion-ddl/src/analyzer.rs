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

//! Generic DDL analyzer rule and extension planner for Spice catalog integrations.
//!
//! [`DdlAnalyzerRule`] intercepts `CREATE TABLE` / `DROP TABLE` / `CREATE SCHEMA`
//! logical plans, extracts parameters (Arrow schema, primary keys, DDL extensions),
//! and produces [`DdlExtensionNode`]s that carry both the parameters and the
//! catalog-specific [`CatalogDdlHandler`].
//!
//! [`DdlExtensionPlanner`] is stateless — it matches any [`DdlExtensionNode`]
//! and dispatches to the handler stored on the node.  Because the handler is
//! embedded in the node, a single planner instance handles every registered
//! catalog type.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock, Weak};

use async_trait::async_trait;
use datafusion::catalog::CatalogProviderList;
use datafusion::common::DFSchemaRef;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{DdlStatement, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::logical_expr::{Expr, UserDefinedLogicalNode};
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::sql::TableReference;

use crate::handler::{CatalogDdlHandler, CreateSchemaParams, CreateTableParams, DropTableParams};
use crate::{
    SharedDdlExtensionStore, ddl_output_schema, extract_primary_key_columns, is_ddl_enabled,
    parse_qualified_schema_name,
};

// ── DdlExtensionNode ──────────────────────────────────────────────────────────

/// The single logical plan node type for all Spice DDL operations.
///
/// Produced by [`DdlAnalyzerRule`] and consumed by [`DdlExtensionPlanner`].
/// The embedded [`CatalogDdlHandler`] carries catalog-specific logic; the
/// planner has no catalog knowledge of its own.
pub struct DdlExtensionNode {
    /// The DDL operation and its full parameters.
    pub op: DdlNodeOp,
    /// The handler that will convert this node to a physical plan.
    pub handler: Arc<dyn CatalogDdlHandler>,
    output_schema: DFSchemaRef,
}

impl DdlExtensionNode {
    /// Construct a new node. The output schema is fixed: a single `result: Utf8` column.
    #[must_use]
    pub fn new(op: DdlNodeOp, handler: Arc<dyn CatalogDdlHandler>) -> Self {
        Self {
            op,
            handler,
            output_schema: ddl_output_schema(),
        }
    }
}

impl fmt::Debug for DdlExtensionNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DdlExtensionNode")
            .field("op", &self.op)
            .field("handler", &self.handler.name())
            .finish_non_exhaustive()
    }
}

// Hash / PartialEq / Eq / PartialOrd — required by UserDefinedLogicalNodeCore.
// We hash only the catalog-identifying names; `handler` and `arrow_schema` are excluded.
impl Hash for DdlExtensionNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.op.kind_label().hash(state);
        self.op.catalog_name().hash(state);
        self.op.schema_name().hash(state);
        self.op.table_name().hash(state);
    }
}
impl PartialEq for DdlExtensionNode {
    fn eq(&self, other: &Self) -> bool {
        self.op.kind_label() == other.op.kind_label()
            && self.op.catalog_name() == other.op.catalog_name()
            && self.op.schema_name() == other.op.schema_name()
            && self.op.table_name() == other.op.table_name()
    }
}
impl Eq for DdlExtensionNode {}
impl PartialOrd for DdlExtensionNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.op.catalog_name(), self.op.table_name())
            .partial_cmp(&(other.op.catalog_name(), other.op.table_name()))
    }
}

impl UserDefinedLogicalNodeCore for DdlExtensionNode {
    fn name(&self) -> &'static str {
        "DdlExtension"
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
            "DdlExtension({}): {}.{}.{}",
            self.op.kind_label(),
            self.op.catalog_name(),
            self.op.schema_name(),
            self.op.table_name().unwrap_or(""),
        )
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Self {
            op: self.op.clone(),
            handler: Arc::clone(&self.handler),
            output_schema: DFSchemaRef::clone(&self.output_schema),
        })
    }
}

// ── DdlNodeOp ─────────────────────────────────────────────────────────────────

/// The DDL operation kind and its full parameters, stored inside [`DdlExtensionNode`].
#[derive(Debug, Clone)]
pub enum DdlNodeOp {
    CreateTable(Box<CreateTableParams>),
    DropTable(DropTableParams),
    CreateSchema(CreateSchemaParams),
}

impl DdlNodeOp {
    fn kind_label(&self) -> &'static str {
        match self {
            Self::CreateTable(_) => "CreateTable",
            Self::DropTable(_) => "DropTable",
            Self::CreateSchema(_) => "CreateSchema",
        }
    }
    fn catalog_name(&self) -> &str {
        match self {
            Self::CreateTable(p) => &p.catalog_name,
            Self::DropTable(p) => &p.catalog_name,
            Self::CreateSchema(p) => &p.catalog_name,
        }
    }
    fn schema_name(&self) -> &str {
        match self {
            Self::CreateTable(p) => &p.schema_name,
            Self::DropTable(p) => &p.schema_name,
            Self::CreateSchema(p) => &p.schema_name,
        }
    }
    fn table_name(&self) -> Option<&str> {
        match self {
            Self::CreateTable(p) => Some(&p.table_name),
            Self::DropTable(p) => Some(&p.table_name),
            Self::CreateSchema(_) => None,
        }
    }
}

// ── DdlAnalyzerRule ───────────────────────────────────────────────────────────

/// Generic `DataFusion` analyzer rule for Spice catalog DDL.
///
/// Intercepts `CREATE TABLE` / `DROP TABLE` / `CREATE SCHEMA` plans,
/// performs the DDL-enabled check, extracts Arrow schema + primary keys,
/// pops the [`SharedDdlExtensionStore`], and produces a [`DdlExtensionNode`]
/// carrying the params and the catalog-specific [`CatalogDdlHandler`].
///
/// Install one instance per catalog type (Cayenne, Iceberg, …).
pub struct DdlAnalyzerRule {
    catalog_list: Weak<dyn CatalogProviderList>,
    ddl_enabled_catalogs: Weak<RwLock<HashSet<String>>>,
    ddl_options: SharedDdlExtensionStore,
    handler: Arc<dyn CatalogDdlHandler>,
    default_schema: String,
    default_catalog: String,
}

impl fmt::Debug for DdlAnalyzerRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DdlAnalyzerRule")
            .field("handler", &self.handler.name())
            .finish_non_exhaustive()
    }
}

impl DdlAnalyzerRule {
    #[must_use]
    pub fn new(
        catalog_list: &Arc<dyn CatalogProviderList>,
        ddl_enabled_catalogs: &Arc<RwLock<HashSet<String>>>,
        ddl_options: SharedDdlExtensionStore,
        handler: Arc<dyn CatalogDdlHandler>,
        default_schema: impl Into<String>,
        default_catalog: impl Into<String>,
    ) -> Self {
        Self {
            catalog_list: Arc::downgrade(catalog_list),
            ddl_enabled_catalogs: Arc::downgrade(ddl_enabled_catalogs),
            ddl_options,
            handler,
            default_schema: default_schema.into(),
            default_catalog: default_catalog.into(),
        }
    }
}

impl AnalyzerRule for DdlAnalyzerRule {
    fn name(&self) -> &'static str {
        "spice_ddl_rewrite"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        let Some(catalog_list) = self.catalog_list.upgrade() else {
            return Ok(plan);
        };

        match &plan {
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(create)) => {
                let catalog_name = create
                    .name
                    .catalog()
                    .unwrap_or(&self.default_catalog)
                    .to_string();

                if !is_ddl_enabled(&self.ddl_enabled_catalogs, &catalog_name) {
                    return Ok(plan);
                }
                if !self.handler.is_target_catalog(&catalog_name, &catalog_list) {
                    return Ok(plan);
                }

                let schema_name = create
                    .name
                    .schema()
                    .unwrap_or(&self.default_schema)
                    .to_string();
                let table_name = create.name.table().to_string();
                let arrow_schema = Arc::new(create.input.schema().inner().as_ref().clone());
                let primary_key = extract_primary_key_columns(&create.constraints, &arrow_schema);

                // Pop the extension store (stored by the SQL pre-processor).
                let extension_key = create.name.to_string();
                let extension = self
                    .ddl_options
                    .write()
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to acquire DDL extension store lock: {e}"
                        ))
                    })?
                    .remove(&TableReference::parse_str(&extension_key))
                    .unwrap_or_default();

                let params = CreateTableParams {
                    catalog_name,
                    schema_name,
                    table_name,
                    arrow_schema,
                    primary_key,
                    extension,
                    if_not_exists: create.if_not_exists,
                    or_replace: create.or_replace,
                    like_source_table: None,
                };
                let node = DdlExtensionNode::new(
                    DdlNodeOp::CreateTable(Box::new(params)),
                    Arc::clone(&self.handler),
                );
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }))
            }

            LogicalPlan::Ddl(DdlStatement::DropTable(drop)) => {
                let catalog_name = drop
                    .name
                    .catalog()
                    .unwrap_or(&self.default_catalog)
                    .to_string();

                if !is_ddl_enabled(&self.ddl_enabled_catalogs, &catalog_name) {
                    return Ok(plan);
                }
                if !self.handler.is_target_catalog(&catalog_name, &catalog_list) {
                    return Ok(plan);
                }

                let params = DropTableParams {
                    catalog_name,
                    schema_name: drop
                        .name
                        .schema()
                        .unwrap_or(&self.default_schema)
                        .to_string(),
                    table_name: drop.name.table().to_string(),
                    if_exists: drop.if_exists,
                };
                let node =
                    DdlExtensionNode::new(DdlNodeOp::DropTable(params), Arc::clone(&self.handler));
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }))
            }

            LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(create)) => {
                let (catalog_name, schema_name) =
                    parse_qualified_schema_name(&create.schema_name, &self.default_catalog);

                if !is_ddl_enabled(&self.ddl_enabled_catalogs, &catalog_name) {
                    return Ok(plan);
                }
                if !self.handler.is_target_catalog(&catalog_name, &catalog_list) {
                    return Ok(plan);
                }

                let params = CreateSchemaParams {
                    catalog_name,
                    schema_name,
                    if_not_exists: create.if_not_exists,
                };
                let node = DdlExtensionNode::new(
                    DdlNodeOp::CreateSchema(params),
                    Arc::clone(&self.handler),
                );
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }))
            }

            _ => Ok(plan),
        }
    }
}

// ── DdlExtensionPlanner ───────────────────────────────────────────────────────

/// Stateless extension planner for all Spice DDL operations.
///
/// Matches any [`DdlExtensionNode`] and delegates to the handler embedded in
/// that node. Because the handler is on the node itself, a single instance of
/// this planner handles every registered catalog type (Cayenne, Iceberg, …).
///
/// Install this alongside a [`datafusion_dml::DmlExtensionPlanner`] (or
/// equivalent) for the full set of extension nodes the runtime produces.
#[derive(Debug, Default)]
pub struct DdlExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for DdlExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(ddl_node) = node.as_any().downcast_ref::<DdlExtensionNode>() else {
            return Ok(None);
        };

        let catalog_list = Arc::<dyn CatalogProviderList>::clone(session_state.catalog_list());

        let exec = match &ddl_node.op {
            DdlNodeOp::CreateTable(params) => {
                ddl_node
                    .handler
                    .create_table_exec(*params.clone(), catalog_list, session_state)?
            }
            DdlNodeOp::DropTable(params) => ddl_node
                .handler
                .drop_table_exec(params.clone(), catalog_list)?,
            DdlNodeOp::CreateSchema(params) => {
                ddl_node
                    .handler
                    .create_schema_exec(params.clone(), catalog_list, session_state)?
            }
        };

        Ok(Some(exec))
    }
}
