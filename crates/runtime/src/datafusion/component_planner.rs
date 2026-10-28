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

//! Plans the statements that define, change, and remove Spicepod datasets and
//! catalogs.
//!
//! Datasets are the tables of the `spice` catalog, so `CREATE TABLE`, `ALTER
//! TABLE`, `DROP TABLE`, and `SHOW CREATE TABLE` naming one define or show a
//! dataset. A table of any other catalog is left to that catalog's DDL. The
//! catalog statements, and `ALTER TABLE ... RESET (...)`, are Spice syntax the
//! SQL parser does not read; [`parse_spice_statement`] reads them first.

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::TableReference;
use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableType, provider_as_source, source_as_provider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::ast::{
    AlterTable, AlterTableOperation, CreateTable, Ident, ObjectName, ObjectNamePart, ObjectType,
    SqlOption, Statement as SQLStatement,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use datafusion_ddl::component_ddl::{
    COMPONENT_DDL_NODE_NAME, ComponentDdlHandler, ComponentDdlNode, ComponentDdlOp, OptionsChange,
};
use datafusion_ddl::component_options::{
    ClauseFields, ComponentKind, ComponentOption, ConnectorParameters, OptionValue,
    component_from_options, component_name_from_sql, create_table_with_options,
    dataset_clause_fields, options_from_sql,
};
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;

use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

/// A table name as `DataFusion` reads one: unquoted parts are lowercased.
fn table_reference(name: &ObjectName) -> TableReference {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => normalized(ident),
            other @ ObjectNamePart::Function(_) => other.to_string(),
        })
        .collect();
    match parts.as_slice() {
        [table] => TableReference::bare(table.as_str()),
        [schema, table] => TableReference::partial(schema.as_str(), table.as_str()),
        [catalog, schema, table] => {
            TableReference::full(catalog.as_str(), schema.as_str(), table.as_str())
        }
        _ => TableReference::bare(name.to_string()),
    }
}

fn normalized(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// The name a dataset has in a Spicepod for the table `name` of the `spice`
/// catalog: its schema and table, as SQL wrote them. A Spicepod dataset's name
/// never carries the catalog.
pub(crate) fn dataset_name(name: &ObjectName) -> String {
    match name.0.as_slice() {
        [_catalog, schema, table] => ObjectName(vec![schema.clone(), table.clone()]).to_string(),
        _ => component_name_from_sql(name),
    }
}

/// Whether `name` is a table of the `spice` catalog: a dataset.
pub(crate) fn names_a_dataset(name: &ObjectName) -> bool {
    table_reference(name)
        .catalog()
        .is_none_or(|catalog| catalog == SPICE_DEFAULT_CATALOG)
}

fn refuse_reserved_schema(name: &ObjectName) -> DFResult<()> {
    let reference = table_reference(name);
    if runtime_datafusion::is_spice_internal_dataset(&reference) {
        return Err(DataFusionError::Plan(format!(
            "Table '{reference}' is in a schema Spice reserves for its own tables. Choose another schema."
        )));
    }
    Ok(())
}

/// The source a statement's `from` option names.
fn source_option(options: &[ComponentOption]) -> Option<&str> {
    options
        .iter()
        .find_map(|option| match (&*option.key, &option.value) {
            ("from", OptionValue::String(from)) => Some(from.as_str()),
            _ => None,
        })
}

async fn plan_op(
    op: ComponentDdlOp,
    handler: Arc<dyn ComponentDdlHandler>,
) -> DFResult<LogicalPlan> {
    handler.check_access(&op).await?;
    Ok(ComponentDdlNode::new(op, handler).into_plan())
}

/// Plan `CREATE TABLE` naming a dataset.
async fn plan_create_dataset(
    create: &CreateTable,
    handler: Arc<dyn ComponentDdlHandler>,
) -> DFResult<LogicalPlan> {
    refuse_reserved_schema(&create.name)?;
    let name = dataset_name(&create.name);
    if create.query.is_some() {
        return Err(DataFusionError::NotImplemented(format!(
            "CREATE TABLE {name} ... AS SELECT is not supported for a dataset. Define the dataset with WITH (from = '...', ...), then load it with INSERT INTO when its source is writable."
        )));
    }
    let options = options_from_sql(create_table_with_options(create))?;
    let Some(from) = source_option(&options) else {
        return Err(DataFusionError::Plan(format!(
            "Dataset '{name}' has no source. Name it with WITH (from = '<connector>:<path>', ...), for example WITH (from = 'postgres:public.orders'). See https://spiceai.org/docs/components/data-connectors"
        )));
    };
    let clauses = dataset_clause_fields(create)?;
    let parameters =
        crate::component_ddl::SpecParameters::for_component(ComponentKind::Dataset, from).await;
    let dataset: Dataset = component_from_options(
        &name,
        &clauses,
        &options,
        parameters.as_ref().map(|p| p as &dyn ConnectorParameters),
    )?;
    plan_op(
        ComponentDdlOp::CreateDataset {
            dataset: Box::new(dataset),
            if_not_exists: create.if_not_exists,
            or_replace: create.or_replace,
        },
        handler,
    )
    .await
}

/// Plan `ALTER TABLE ... SET (...)` naming a dataset.
async fn plan_alter_dataset(
    alter: &AlterTable,
    handler: Arc<dyn ComponentDdlHandler>,
) -> DFResult<LogicalPlan> {
    refuse_reserved_schema(&alter.name)?;
    let name = dataset_name(&alter.name);
    let [AlterTableOperation::SetOptionsParens { options }] = alter.operations.as_slice() else {
        return Err(DataFusionError::NotImplemented(format!(
            "ALTER TABLE {name} supports SET (key = value, ...) and RESET (key, ...) on a dataset, one per statement."
        )));
    };
    let options = options_from_sql(options)?;
    plan_op(
        ComponentDdlOp::AlterDataset {
            name,
            change: OptionsChange::Set(options),
        },
        handler,
    )
    .await
}

/// Plan `DROP TABLE` naming datasets.
async fn plan_drop_dataset(
    names: &[ObjectName],
    if_exists: bool,
    handler: Arc<dyn ComponentDdlHandler>,
) -> DFResult<LogicalPlan> {
    let [name] = names else {
        return Err(DataFusionError::NotImplemented(
            "DROP TABLE drops one dataset per statement.".to_string(),
        ));
    };
    refuse_reserved_schema(name)?;
    plan_op(
        ComponentDdlOp::DropDataset {
            name: dataset_name(name),
            if_exists,
        },
        handler,
    )
    .await
}

/// Plan `SHOW CREATE TABLE` or `SHOW CREATE CATALOG` naming a component: one
/// row, in the columns `SHOW CREATE TABLE` returns. `None` when no component
/// has the name, so the statement is planned as `DataFusion` plans it.
async fn plan_show_create(
    kind: ComponentKind,
    name: &str,
    handler: &Arc<dyn ComponentDdlHandler>,
) -> DFResult<Option<LogicalPlan>> {
    let Some(statement) = handler.create_statement(kind, name).await? else {
        return Ok(None);
    };
    let (catalog, schema, table) = match kind {
        ComponentKind::Dataset => {
            let reference = TableReference::parse_str(name)
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
            (
                reference.catalog.to_string(),
                Some(reference.schema.to_string()),
                reference.table.to_string(),
            )
        }
        ComponentKind::Catalog => (name.to_string(), None, name.to_string()),
    };
    let schema_ref = Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, true),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("definition", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema_ref),
        vec![
            Arc::new(StringArray::from(vec![catalog])),
            Arc::new(StringArray::from(vec![schema])),
            Arc::new(StringArray::from(vec![table])),
            Arc::new(StringArray::from(vec![statement])),
        ],
    )?;
    let plan = LogicalPlanBuilder::scan(
        "definition",
        provider_as_source(Arc::new(DefinitionRow { batch })),
        None,
    )?
    .build()?;
    Ok(Some(plan))
}

/// The row `SHOW CREATE` returns. Its table is `Temporary`, so its result is
/// never cached: a definition can change between two runs of the statement.
#[derive(Debug)]
struct DefinitionRow {
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for DefinitionRow {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        MemorySourceConfig::try_new_exec(
            &[vec![self.batch.clone()]],
            self.schema(),
            projection.cloned(),
        )
        .map(|exec| exec as Arc<dyn ExecutionPlan>)
    }
}

/// Whether `plan` defines, changes, removes, or shows a component. Such a plan
/// is not reused: it embeds the definitions of the moment, and its access was
/// checked for the caller who planned it.
#[must_use]
pub(crate) fn embeds_definitions(plan: &LogicalPlan) -> bool {
    plan.exists(|node| {
        Ok(match node {
            LogicalPlan::Extension(extension) => extension.node.name() == COMPONENT_DDL_NODE_NAME,
            LogicalPlan::TableScan(scan) => source_as_provider(&scan.source)
                .is_ok_and(|provider| provider.is::<DefinitionRow>()),
            _ => false,
        })
    })
    // A plan that cannot be inspected is not reused either.
    .unwrap_or(true)
}

// ── Spice statements ──────────────────────────────────────────────────────────

/// A statement in Spice syntax the SQL parser does not read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SpiceStatement {
    CreateCatalog {
        name: String,
        options: Vec<SqlOption>,
        if_not_exists: bool,
        or_replace: bool,
    },
    AlterCatalog {
        name: String,
        change: AlterChange,
    },
    DropCatalog {
        name: String,
        if_exists: bool,
    },
    ShowCreateCatalog {
        name: String,
    },
    /// `ALTER TABLE ... RESET (...)`.
    ResetTable {
        name: ObjectName,
        keys: Vec<String>,
    },
}

/// The change an `ALTER CATALOG` makes.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AlterChange {
    Set(Vec<SqlOption>),
    Reset(Vec<String>),
}

/// Read `sql` as a Spice statement, or `None` when it is not one: the
/// statement is then parsed as any other.
///
/// # Errors
///
/// Returns an error for a statement that starts as a Spice statement and does
/// not parse as one.
pub(crate) fn parse_spice_statement(sql: &str) -> DFResult<Option<SpiceStatement>> {
    // A cheap check first: every Spice statement starts with one of these words.
    let first_word = datafusion_ddl::component_options::first_word(sql);
    if !["create", "alter", "drop", "show"]
        .iter()
        .any(|word| first_word.eq_ignore_ascii_case(word))
    {
        return Ok(None);
    }

    let dialect = PostgreSqlDialect {};
    let Ok(mut parser) = Parser::new(&dialect).try_with_sql(sql) else {
        return Ok(None);
    };
    let statement = if parser.parse_keyword(Keyword::CREATE) {
        let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
        if !parser.parse_keyword(Keyword::CATALOG) {
            return Ok(None);
        }
        let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = catalog_name(&mut parser)?;
        let options = parser.parse_options(Keyword::WITH).map_err(parse_error)?;
        SpiceStatement::CreateCatalog {
            name,
            options,
            if_not_exists,
            or_replace,
        }
    } else if parser.parse_keyword(Keyword::ALTER) {
        if parser.parse_keyword(Keyword::CATALOG) {
            let name = catalog_name(&mut parser)?;
            let change = if parser.peek_keyword(Keyword::SET) {
                AlterChange::Set(parser.parse_options(Keyword::SET).map_err(parse_error)?)
            } else if parser.parse_keyword(Keyword::RESET) {
                AlterChange::Reset(reset_keys(&mut parser)?)
            } else {
                return Err(DataFusionError::Plan(format!(
                    "ALTER CATALOG {name} supports SET (key = value, ...) and RESET (key, ...)."
                )));
            };
            SpiceStatement::AlterCatalog { name, change }
        } else if parser.parse_keyword(Keyword::TABLE) {
            // Only `ALTER TABLE ... RESET (...)` is Spice syntax; every other
            // ALTER TABLE is the parser's.
            let _ = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let Ok(name) = parser.parse_object_name(false) else {
                return Ok(None);
            };
            if !parser.parse_keyword(Keyword::RESET) {
                return Ok(None);
            }
            let keys = reset_keys(&mut parser)?;
            SpiceStatement::ResetTable { name, keys }
        } else {
            return Ok(None);
        }
    } else if parser.parse_keyword(Keyword::DROP) {
        if !parser.parse_keyword(Keyword::CATALOG) {
            return Ok(None);
        }
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = catalog_name(&mut parser)?;
        SpiceStatement::DropCatalog { name, if_exists }
    } else if parser.parse_keywords(&[Keyword::SHOW, Keyword::CREATE, Keyword::CATALOG]) {
        let name = catalog_name(&mut parser)?;
        SpiceStatement::ShowCreateCatalog { name }
    } else {
        return Ok(None);
    };

    // One statement, optionally ended by `;`.
    let _ = parser.consume_token(&Token::SemiColon);
    if parser.peek_token().token != Token::EOF {
        return Err(DataFusionError::Plan(format!(
            "Unexpected {} after the statement. Run one statement at a time.",
            parser.peek_token().token
        )));
    }
    Ok(Some(statement))
}

fn parse_error(e: datafusion::sql::sqlparser::parser::ParserError) -> DataFusionError {
    DataFusionError::SQL(Box::new(e), None)
}

fn catalog_name(parser: &mut Parser) -> DFResult<String> {
    let ident = parser.parse_identifier().map_err(parse_error)?;
    Ok(normalized(&ident))
}

fn reset_keys(parser: &mut Parser) -> DFResult<Vec<String>> {
    parser.expect_token(&Token::LParen).map_err(parse_error)?;
    let keys = parser
        .parse_comma_separated(Parser::parse_identifier)
        .map_err(parse_error)?;
    parser.expect_token(&Token::RParen).map_err(parse_error)?;
    Ok(keys.into_iter().map(|ident| ident.value).collect())
}

/// Plan a Spice statement.
pub(crate) async fn plan_spice_statement(
    statement: SpiceStatement,
    handler: Arc<dyn ComponentDdlHandler>,
) -> DFResult<LogicalPlan> {
    match statement {
        SpiceStatement::CreateCatalog {
            name,
            options,
            if_not_exists,
            or_replace,
        } => {
            let options = options_from_sql(&options)?;
            let Some(from) = source_option(&options) else {
                return Err(DataFusionError::Plan(format!(
                    "Catalog '{name}' has no source. Name it with WITH (from = '<connector>'), for example WITH (from = 'iceberg:https://catalog.example.com/v1/namespaces'). See https://spiceai.org/docs/components/catalogs"
                )));
            };
            let parameters =
                crate::component_ddl::SpecParameters::for_component(ComponentKind::Catalog, from)
                    .await;
            let catalog: Catalog = component_from_options(
                &name,
                &ClauseFields::default(),
                &options,
                parameters.as_ref().map(|p| p as &dyn ConnectorParameters),
            )?;
            plan_op(
                ComponentDdlOp::CreateCatalog {
                    catalog: Box::new(catalog),
                    if_not_exists,
                    or_replace,
                },
                handler,
            )
            .await
        }
        SpiceStatement::AlterCatalog { name, change } => {
            let change = match change {
                AlterChange::Set(options) => OptionsChange::Set(options_from_sql(&options)?),
                AlterChange::Reset(keys) => OptionsChange::Reset(keys),
            };
            plan_op(ComponentDdlOp::AlterCatalog { name, change }, handler).await
        }
        SpiceStatement::DropCatalog { name, if_exists } => {
            plan_op(ComponentDdlOp::DropCatalog { name, if_exists }, handler).await
        }
        SpiceStatement::ShowCreateCatalog { name } => {
            plan_show_create(ComponentKind::Catalog, &name, &handler)
                .await?
                .ok_or_else(|| DataFusionError::Plan(format!("Catalog '{name}' does not exist.")))
        }
        SpiceStatement::ResetTable { name, keys } => {
            if !names_a_dataset(&name) {
                return Err(DataFusionError::NotImplemented(format!(
                    "ALTER TABLE {name} RESET (...) is supported on datasets, the tables of the '{SPICE_DEFAULT_CATALOG}' catalog."
                )));
            }
            plan_op(
                ComponentDdlOp::AlterDataset {
                    name: dataset_name(&name),
                    change: OptionsChange::Reset(keys),
                },
                handler,
            )
            .await
        }
    }
}

/// Plan `statement` when it names a dataset; `None` leaves it to the default
/// planning.
pub(crate) async fn plan_dataset_statement(
    statement: &SQLStatement,
    handler: &Arc<dyn ComponentDdlHandler>,
) -> DFResult<Option<LogicalPlan>> {
    match statement {
        SQLStatement::CreateTable(create) if names_a_dataset(&create.name) => {
            plan_create_dataset(create, Arc::clone(handler))
                .await
                .map(Some)
        }
        SQLStatement::AlterTable(alter) if names_a_dataset(&alter.name) => {
            plan_alter_dataset(alter, Arc::clone(handler))
                .await
                .map(Some)
        }
        SQLStatement::Drop {
            object_type: ObjectType::Table,
            if_exists,
            names,
            ..
        } if names.iter().all(names_a_dataset) => {
            plan_drop_dataset(names, *if_exists, Arc::clone(handler))
                .await
                .map(Some)
        }
        SQLStatement::ShowCreate {
            obj_type: datafusion::sql::sqlparser::ast::ShowCreateObject::Table,
            obj_name,
        } if names_a_dataset(obj_name) => {
            plan_show_create(ComponentKind::Dataset, &dataset_name(obj_name), handler).await
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(sql: &str) -> Option<SpiceStatement> {
        parse_spice_statement(sql).expect("parses")
    }

    #[test]
    fn spice_statements_are_read_and_others_are_left_to_the_parser() {
        assert_eq!(
            parsed(
                "CREATE CATALOG IF NOT EXISTS Lake WITH (from = 'iceberg', include = ARRAY['a.*']);"
            ),
            Some(SpiceStatement::CreateCatalog {
                name: "lake".to_string(),
                options: Parser::new(&PostgreSqlDialect {})
                    .try_with_sql("WITH (from = 'iceberg', include = ARRAY['a.*'])")
                    .expect("tokens")
                    .parse_options(Keyword::WITH)
                    .expect("options"),
                if_not_exists: true,
                or_replace: false,
            })
        );
        assert_eq!(
            parsed("drop catalog if exists \"Lake\""),
            Some(SpiceStatement::DropCatalog {
                name: "Lake".to_string(),
                if_exists: true,
            })
        );
        assert_eq!(
            parsed("ALTER CATALOG lake RESET (include, \"dataset_params.file_format\")"),
            Some(SpiceStatement::AlterCatalog {
                name: "lake".to_string(),
                change: AlterChange::Reset(vec![
                    "include".to_string(),
                    "dataset_params.file_format".to_string()
                ]),
            })
        );
        assert_eq!(
            parsed("SHOW CREATE CATALOG lake"),
            Some(SpiceStatement::ShowCreateCatalog {
                name: "lake".to_string()
            })
        );
        let Some(SpiceStatement::ResetTable { name, keys }) = parsed(
            "ALTER TABLE sales.orders RESET (\"acceleration.refresh_check_interval\", pg_host)",
        ) else {
            panic!("ALTER TABLE ... RESET is a Spice statement");
        };
        assert_eq!(name.to_string(), "sales.orders");
        assert_eq!(keys, vec!["acceleration.refresh_check_interval", "pg_host"]);

        // Written after a comment, as tools that annotate statements send them.
        assert_eq!(
            parsed("-- drop it\n  /* the lake */ DROP CATALOG lake"),
            Some(SpiceStatement::DropCatalog {
                name: "lake".to_string(),
                if_exists: false,
            })
        );

        for other in [
            "SELECT 1",
            "CREATE TABLE t (id INT)",
            "ALTER TABLE t SET (a = 1)",
            "ALTER TABLE t RENAME TO u",
            "DROP TABLE t",
            "SHOW CREATE TABLE t",
            "  -- a comment\nSELECT 1",
            "WITH x AS (SELECT 1) SELECT * FROM x",
        ] {
            assert_eq!(parsed(other), None, "{other}");
        }
    }

    /// An option key may be written dotted and unquoted in a Spice statement,
    /// as in `CREATE TABLE ... WITH`: the statement is read after the keys are
    /// quoted.
    #[test]
    fn spice_statements_read_unquoted_dotted_keys() {
        let quoted = |sql: &str| {
            datafusion_ddl::component_options::quote_dotted_option_keys(sql, &PostgreSqlDialect {})
                .into_owned()
        };
        let Some(SpiceStatement::CreateCatalog { options, .. }) = parsed(&quoted(
            "CREATE CATALOG lake WITH (from = 'iceberg', dataset_params.file_format = 'parquet')",
        )) else {
            panic!("CREATE CATALOG is a Spice statement");
        };
        let keys: Vec<String> = options_from_sql(&options)
            .expect("options")
            .into_iter()
            .map(|option| option.key)
            .collect();
        assert_eq!(keys, vec!["from", "dataset_params.file_format"]);

        assert_eq!(
            parsed(&quoted(
                "ALTER CATALOG lake SET (acceleration.enabled = true)"
            )),
            parsed("ALTER CATALOG lake SET (\"acceleration.enabled\" = true)")
        );
        let Some(SpiceStatement::ResetTable { keys, .. }) = parsed(&quoted(
            "ALTER TABLE orders RESET (acceleration.refresh_check_interval, pg_host)",
        )) else {
            panic!("ALTER TABLE ... RESET is a Spice statement");
        };
        assert_eq!(keys, vec!["acceleration.refresh_check_interval", "pg_host"]);
    }

    #[test]
    fn a_malformed_spice_statement_is_an_error() {
        let err =
            parse_spice_statement("CREATE CATALOG lake WITH (from)").expect_err("missing value");
        assert!(err.to_string().contains("Expected"), "{err}");
        let err = parse_spice_statement("DROP CATALOG lake; SELECT 1").expect_err("two statements");
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: Unexpected SELECT after the statement. Run one statement at a time."
        );
    }

    #[test]
    fn datasets_are_the_tables_of_the_spice_catalog() {
        let name = |sql: &str| {
            let mut parser = Parser::new(&PostgreSqlDialect {})
                .try_with_sql(sql)
                .expect("tokens");
            parser.parse_object_name(false).expect("object name")
        };
        assert!(names_a_dataset(&name("orders")));
        assert!(names_a_dataset(&name("sales.orders")));
        assert!(names_a_dataset(&name("spice.sales.orders")));
        assert!(names_a_dataset(&name("SPICE.sales.orders")));
        assert!(!names_a_dataset(&name("\"SPICE\".sales.orders")));
        assert!(!names_a_dataset(&name("lake.sales.orders")));

        // A Spicepod dataset's name never carries the catalog.
        assert_eq!(dataset_name(&name("orders")), "orders");
        assert_eq!(dataset_name(&name("sales.orders")), "sales.orders");
        assert_eq!(
            dataset_name(&name("spice.Sales.\"Orders\"")),
            "Sales.\"Orders\""
        );
    }

    /// Shows one definition, and applies nothing.
    #[derive(Debug)]
    struct OneDefinition;

    #[async_trait]
    impl ComponentDdlHandler for OneDefinition {
        async fn check_access(&self, _op: &ComponentDdlOp) -> DFResult<()> {
            Ok(())
        }

        async fn apply(&self, _op: ComponentDdlOp) -> DFResult<String> {
            Ok(String::new())
        }

        async fn create_statement(
            &self,
            _kind: ComponentKind,
            name: &str,
        ) -> DFResult<Option<String>> {
            Ok(Some(format!(
                "CREATE TABLE {name} WITH (from = 'file:t.csv')"
            )))
        }
    }

    /// A plan that defines or shows a component is never reused from the plan
    /// cache; any other plan may be.
    #[tokio::test]
    async fn only_plans_of_definitions_embed_them() {
        let handler: Arc<dyn ComponentDdlHandler> = Arc::new(OneDefinition);
        let drop = ComponentDdlNode::new(
            ComponentDdlOp::DropDataset {
                name: "orders".to_string(),
                if_exists: false,
            },
            Arc::clone(&handler),
        )
        .into_plan();
        assert!(embeds_definitions(&drop));

        let show = plan_show_create(ComponentKind::Dataset, "orders", &handler)
            .await
            .expect("plans")
            .expect("a definition");
        assert!(embeds_definitions(&show));

        let query = LogicalPlanBuilder::empty(true)
            .build()
            .expect("an empty relation");
        assert!(!embeds_definitions(&query));
    }
}
