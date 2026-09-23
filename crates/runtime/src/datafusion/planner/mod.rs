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

//! Unified SQL statement planner.
//!
//! Intercepts SQL statements at the AST level, before `DataFusion`'s standard
//! planner, for two purposes:
//!
//! 1. **DDL extensions** — `CREATE TABLE` with `WITH (...)` options
//!    (`acceleration.*`, `dataset.*`), `PARTITION BY`, and `CLUSTER BY` clauses that
//!    `DataFusion`'s `SqlToRel` does not support. Extensions are extracted from
//!    the AST, stored in the [`DdlExtensionStore`], and stripped before
//!    delegating to `DataFusion`.
//!
//! 2. **DML interception (optional overlay)** — only statements that need
//!    non-default behavior are rewritten to generic `datafusion_dml`
//!    extension nodes:
//!    - DELETE/UPDATE targeting Cayenne tables in scheduler mode,
//!    - INSERT targeting distributed write-through tables,
//!    - MERGE targeting Cayenne tables.
//!
//!    All other DML uses standard `DataFusion` planning/execution unchanged.
//!
//! For everything else, the planner delegates to `DataFusion`'s standard
//! `session.statement_to_plan()` path.

mod create_table;
mod delete;
mod insert;
mod merge;
pub mod physical_execs;
mod update;

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::config::Dialect;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::common::TableReference;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::CreateTableOptions;
use datafusion::sql::sqlparser::ast::Expr as SQLExpr;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
use datafusion::sql::sqlparser::ast::WrappedCollection;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};
use datafusion_dml::CatalogDmlHandler;
use datafusion_expr::WriteOp;
use datafusion_expr::dml::InsertOp;
use tokio::runtime::Handle;

use crate::accelerated::AcceleratedTable;
use crate::config::ClusterRole;
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use datafusion_ddl::{SharedDdlExtensionStore, has_ddl_extensions};

/// The type of catalog backing the planner's DML interception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogMode {
    /// At least one DDL-enabled catalog is Cayenne-backed.
    /// DML targeting Cayenne tables is intercepted at the statement level.
    Cayenne,
    /// No Cayenne catalogs are registered. All statements are delegated
    /// to `DataFusion`'s standard planner.
    Standard,
}

/// Context for the statement planner, carrying catalog and cluster information.
pub struct PlannerContext {
    /// The catalog mode determines whether statement-level interception is active.
    pub catalog_mode: CatalogMode,

    /// The cluster role, if any. When `Some(ClusterRole::Scheduler)`, Cayenne
    /// DML is rewritten into generic extension nodes backed by the distributed
    /// Cayenne DML handler.
    pub cluster_role: Option<ClusterRole>,

    /// Shared store for DDL extensions extracted from `CREATE TABLE` statements.
    /// Populated by the planner, consumed by the analyzer rules.
    pub ddl_extension_store: SharedDdlExtensionStore,

    /// Executor registry, if running in a distributed cluster.
    /// Used by `CREATE TABLE ... LIKE` to resolve auto-generated partition
    /// labels (e.g. `expr0`) back to the original SQL expression.
    pub executor_registry: Option<Arc<crate::cluster::ExecutorRegistry>>,

    /// IO runtime handle used by the distributed Cayenne DML handler.
    pub io_runtime: Handle,

    /// DDL handler for `CREATE TABLE ... LIKE`.
    /// Used to produce a [`datafusion_ddl::DdlExtensionNode`] for the LIKE path,
    /// bypassing the standard analyzer rule.
    pub ddl_handler: Option<Arc<dyn datafusion_ddl::CatalogDdlHandler>>,
}

impl PlannerContext {
    fn cayenne_dml_handler(&self) -> DFResult<Arc<dyn CatalogDmlHandler>> {
        match self.cluster_role {
            Some(ClusterRole::Scheduler) => {
                let Some(executor_registry) = &self.executor_registry else {
                    return Err(DataFusionError::Internal(
                        "Scheduler-mode Cayenne DML planning requires an executor registry"
                            .to_string(),
                    ));
                };

                Ok(Arc::new(
                    crate::datafusion::cayenne_ddl::DistributedCayenneDmlHandler::new(
                        Arc::clone(executor_registry),
                        Some(self.io_runtime.clone()),
                    ),
                ) as Arc<dyn CatalogDmlHandler>)
            }
            _ => Ok(Arc::new(cayenne::ddl::CayenneDmlHandler::new(
                SPICE_DEFAULT_CATALOG,
                SPICE_DEFAULT_SCHEMA,
            )) as Arc<dyn CatalogDmlHandler>),
        }
    }
}

/// Create a [`LogicalPlan`] from SQL, intercepting DDL extensions and
/// distributed DML at the statement level.
pub async fn create_logical_plan(
    sql: &str,
    session: &SessionState,
    ctx: &PlannerContext,
) -> DFResult<LogicalPlan> {
    let statement = parse_sql_statement(sql, session)?;
    create_logical_plan_from_statement(sql, statement, session, ctx).await
}

/// Parse SQL using the configured dialect, taking only Cayenne's
/// `CREATE TABLE ... CLUSTER BY` clause from the generic dialect.
///
/// `sqlparser` recognizes the create-table `CLUSTER BY` clause under its generic
/// and `BigQuery` dialects but not under `PostgreSqlDialect`, the runtime
/// default. When the configured dialect rejects a statement, the generic dialect
/// locates that clause, and the configured dialect must then accept the
/// statement with the clause blanked out: its reading of everything else is the
/// statement returned, with only the clause's expressions taken from the generic
/// parse. No other syntax the configured dialect rejects, or reads differently,
/// gets in. The clause is replaced by spaces rather than removed, so an error in
/// the rest of the statement points at the user's own line and column.
pub(crate) fn parse_sql_statement(sql: &str, session: &SessionState) -> DFResult<Statement> {
    let dialect = session.config().options().sql_parser.dialect;
    let configured_dialect_error = match session.sql_to_statement(sql, &dialect) {
        Ok(statement) => return Ok(statement),
        Err(error) => error,
    };
    let Some((cluster_by, without_cluster_by)) = split_create_table_cluster_by(sql, session) else {
        return Err(configured_dialect_error);
    };
    let mut statement = session.sql_to_statement(&without_cluster_by, &dialect)?;
    let Statement::Statement(sql_statement) = &mut statement else {
        return Err(configured_dialect_error);
    };
    let SQLStatement::CreateTable(table) = sql_statement.as_mut() else {
        return Err(configured_dialect_error);
    };
    if table.cluster_by.is_some() {
        return Err(configured_dialect_error);
    }
    table.cluster_by = Some(cluster_by);
    Ok(statement)
}

/// The `CLUSTER BY` clause of a `CREATE TABLE` statement as the generic dialect
/// parses it, and the statement text with that clause replaced by spaces.
fn split_create_table_cluster_by(
    sql: &str,
    session: &SessionState,
) -> Option<(WrappedCollection<Vec<SQLExpr>>, String)> {
    let Ok(Statement::Statement(generic)) = session.sql_to_statement(sql, &Dialect::Generic) else {
        return None;
    };
    let SQLStatement::CreateTable(table) = *generic else {
        return None;
    };
    let cluster_by = table.cluster_by?;
    let (start, end) = cluster_by_clause_bytes(sql)?;
    let mut blanked = String::with_capacity(sql.len());
    blanked.push_str(sql.get(..start)?);
    // Keep line breaks, so every later token keeps its line and column.
    blanked.extend(
        sql.get(start..end)?
            .chars()
            .map(|c| if c == '\n' { '\n' } else { ' ' }),
    );
    blanked.push_str(sql.get(end..)?);
    Some((cluster_by, blanked))
}

/// Byte range of the first `CLUSTER BY <expression list>` outside parentheses,
/// the create-table clause: a column default or check expression sits inside
/// the column list, and an `AS` query follows the clause.
fn cluster_by_clause_bytes(sql: &str) -> Option<(usize, usize)> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .ok()?;
    let mut depth = 0_usize;
    let significant: Vec<(usize, &TokenWithSpan)> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token.token, Token::Whitespace(_)))
        .collect();
    for pair in significant.windows(2) {
        let [(_, token), (by_index, by)] = pair else {
            continue;
        };
        match &token.token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::Word(cluster)
                if depth == 0
                    && cluster.keyword == Keyword::CLUSTER
                    && matches!(&by.token, Token::Word(word) if word.keyword == Keyword::BY) =>
            {
                let mut parser = Parser::new(&dialect)
                    .with_tokens_with_locations(tokens.get(by_index + 1..)?.to_vec());
                parser.parse_comma_separated(Parser::parse_expr).ok()?;
                let end = parser.get_current_token().span.end;
                return Some((byte_offset(sql, token.span.start)?, byte_offset(sql, end)?));
            }
            _ => {}
        }
    }
    None
}

/// Byte offset in `sql` of a tokenizer location: a one-based line, and a
/// one-based column counted in characters.
fn byte_offset(sql: &str, location: Location) -> Option<usize> {
    let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
    let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
    let line_start: usize = sql.split_inclusive('\n').take(line).map(str::len).sum();
    let rest = sql.get(line_start..)?;
    Some(
        line_start
            + rest
                .char_indices()
                .nth(column)
                .map_or(rest.len(), |(offset, _)| offset),
    )
}

pub async fn create_logical_plan_from_statement(
    sql: &str,
    statement: Statement,
    session: &SessionState,
    ctx: &PlannerContext,
) -> DFResult<LogicalPlan> {
    if let Statement::Statement(ref sql_stmt) = statement {
        match sql_stmt.as_ref() {
            SQLStatement::CreateTable(ct) if ct.like.is_some() => {
                let has_columns = !ct.columns.is_empty();
                let has_partition_by = ct.partition_by.is_some();
                let has_with = !matches!(ct.table_options, CreateTableOptions::None);
                if has_columns || has_partition_by || has_with || has_ddl_extensions(ct) {
                    return Err(DataFusionError::Plan(
                        "CREATE TABLE ... (LIKE ...) cannot be combined with PARTITION BY, CLUSTER BY, WITH \
                         options, or additional column definitions. The new table inherits all \
                         properties \
                         from the source table."
                            .to_string(),
                    ));
                }
                return create_table::plan_create_table_like(statement, session, ctx).await;
            }
            SQLStatement::CreateTable(ct) if has_ddl_extensions(ct) => {
                return create_table::plan_create_table(
                    statement,
                    session,
                    &ctx.ddl_extension_store,
                )
                .await;
            }
            SQLStatement::Delete(_) if ctx.catalog_mode == CatalogMode::Cayenne => {
                return plan_distributed_dml(statement, session, ctx, WriteOp::Delete).await;
            }
            SQLStatement::Update { .. } if ctx.catalog_mode == CatalogMode::Cayenne => {
                return plan_distributed_dml(statement, session, ctx, WriteOp::Update).await;
            }
            SQLStatement::Insert(_) => {
                return plan_distributed_dml(
                    statement,
                    session,
                    ctx,
                    WriteOp::Insert(InsertOp::Append),
                )
                .await;
            }
            SQLStatement::Merge(_) if ctx.catalog_mode == CatalogMode::Cayenne => {
                return merge::plan_distributed_merge(statement, session, ctx, sql).await;
            }
            _ => {}
        }
    }

    session.statement_to_plan(statement).await
}

async fn plan_distributed_dml(
    statement: Statement,
    session: &SessionState,
    ctx: &PlannerContext,
    expected_op: WriteOp,
) -> DFResult<LogicalPlan> {
    let df_plan = session.statement_to_plan(statement).await?;

    if !matches!(ctx.cluster_role, Some(ClusterRole::Scheduler)) {
        return Ok(df_plan);
    }

    let LogicalPlan::Dml(dml) = &df_plan else {
        return Err(DataFusionError::Internal(format!(
            "Expected LogicalPlan::Dml for {expected_op:?} statement"
        )));
    };

    if !matches_write_op(&dml.op, &expected_op) {
        return Err(DataFusionError::Internal(format!(
            "Expected WriteOp::{expected_op:?}, got {:?}",
            dml.op
        )));
    }

    let should_rewrite = match &expected_op {
        WriteOp::Insert(_) => is_distributed_insert_table(session, &dml.table_name).await,
        _ => is_cayenne_table(session, &dml.table_name),
    };

    if !should_rewrite {
        return Ok(df_plan);
    }

    let handler = ctx.cayenne_dml_handler()?;

    match expected_op {
        WriteOp::Delete => delete::plan_distributed_delete(dml, Arc::clone(&handler)),
        WriteOp::Update => update::plan_distributed_update(dml, Arc::clone(&handler)),
        WriteOp::Insert(_) => insert::plan_distributed_insert(dml, handler),
        WriteOp::Ctas => Err(DataFusionError::Internal(
            "CTAS should not reach DML planner".to_string(),
        )),
        WriteOp::Truncate => Err(DataFusionError::Internal(
            "TRUNCATE should not reach DML planner".to_string(),
        )),
    }
}

fn is_cayenne_table(session: &SessionState, table_name: &TableReference) -> bool {
    let catalog_name = table_name.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
    let catalog_list = session.catalog_list();
    if let Some(catalog) = catalog_list.catalog(catalog_name) {
        super::cayenne_ddl::is_cayenne_catalog(catalog.as_ref())
    } else {
        false
    }
}

async fn is_distributed_insert_table(session: &SessionState, table_name: &TableReference) -> bool {
    // Distributed scheduler-mode INSERT rewriting applies to both:
    // 1. Cayenne catalog tables, which must be forwarded to executors, and
    // 2. dual-write accelerated tables (Iceberg federated catalog cache), which
    //    also forward writes remotely.
    if is_cayenne_table(session, table_name) {
        return true;
    }

    let catalog_name = table_name.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
    let schema_name = table_name.schema().unwrap_or(SPICE_DEFAULT_SCHEMA);

    let Some(catalog) = session.catalog_list().catalog(catalog_name) else {
        return false;
    };

    let Some(schema) = catalog.schema(schema_name) else {
        return false;
    };

    let Ok(Some(table_provider)) = schema.table(table_name.table()).await else {
        return false;
    };

    is_dual_write_table_provider(&table_provider)
}

fn is_dual_write_table_provider(table_provider: &Arc<dyn TableProvider>) -> bool {
    spice_table::find_layer::<AcceleratedTable>(
        table_provider.as_ref(),
        spice_table::LayerWalk::Read,
    )
    .is_some_and(AcceleratedTable::is_dual_write)
}

fn matches_write_op(actual: &WriteOp, expected: &WriteOp) -> bool {
    std::mem::discriminant(actual) == std::mem::discriminant(expected)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::common::config::Dialect;
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionConfig;

    use data_components::MetadataEnrichedTableProvider;

    fn postgresql_session() -> datafusion::execution::SessionState {
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.dialect = Dialect::PostgreSQL;
        SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build()
    }

    #[test]
    fn runtime_postgresql_dialect_accepts_create_table_cluster_by() {
        let session = postgresql_session();

        let statement = super::parse_sql_statement(
            "CREATE TABLE events (id BIGINT, region TEXT) CLUSTER BY (region, id)",
            &session,
        )
        .expect("runtime dialect should accept CLUSTER BY");

        assert!(matches!(
            statement,
            datafusion::sql::parser::Statement::Statement(sql_statement)
                if matches!(
                    sql_statement.as_ref(),
                    datafusion::sql::sqlparser::ast::Statement::CreateTable(table)
                        if table.cluster_by.is_some()
                )
        ));
    }

    /// A `CLUSTER BY` spread over lines, after a column default holding
    /// parentheses and before `WITH` options, is found and blanked exactly.
    #[test]
    fn cluster_by_clause_is_blanked_in_place() {
        let session = postgresql_session();
        let sql = "CREATE TABLE events (\n  id BIGINT DEFAULT (1 + 2),\n  region TEXT\n)\nCLUSTER BY (\n  region, id\n)";
        let (cluster_by, blanked) =
            super::split_create_table_cluster_by(sql, &session).expect("the clause is found");
        assert_eq!(cluster_by.to_string(), "(region, id)");
        assert_eq!(blanked.len(), sql.len());
        assert_eq!(blanked.lines().count(), sql.lines().count());
        assert_eq!(
            blanked.trim_end(),
            "CREATE TABLE events (\n  id BIGINT DEFAULT (1 + 2),\n  region TEXT\n)"
        );
    }

    /// Only `CLUSTER BY` may come from the generic dialect: anything else in the
    /// statement that the configured dialect rejects is still rejected.
    #[test]
    fn cluster_by_does_not_admit_other_syntax_the_configured_dialect_rejects() {
        let session = postgresql_session();
        for sql in [
            // Backtick-quoted identifiers are not PostgreSQL syntax.
            "CREATE TABLE `events` (id BIGINT, region TEXT) CLUSTER BY (region)",
            // `OPTIONS (...)` is BigQuery syntax the generic dialect also accepts.
            "CREATE TABLE events (id BIGINT, region TEXT) CLUSTER BY (region) OPTIONS (description = 'x')",
        ] {
            let error = super::parse_sql_statement(sql, &session)
                .expect_err("the configured dialect rejects everything but the clause");
            let message = error.to_string();
            assert!(
                !message.contains("CLUSTER"),
                "the error must name the unsupported syntax, not the supported clause: {message}"
            );
        }
    }

    /// The rest of the statement is the configured dialect's reading of it; only
    /// the clause's expressions come from the generic dialect.
    #[test]
    fn cluster_by_keeps_the_configured_dialect_reading_of_the_rest() {
        use datafusion::sql::parser::Statement;
        use datafusion::sql::sqlparser::ast::Statement as SQLStatement;

        let session = postgresql_session();
        let with_clause = super::parse_sql_statement(
            r#"CREATE TABLE events (id BIGINT, region TEXT) WITH ("acceleration.engine" = 'cayenne') PARTITION BY region CLUSTER BY (region, id)"#,
            &session,
        )
        .expect("CLUSTER BY after WITH options and PARTITION BY");
        let without_clause = session
            .sql_to_statement(
                r#"CREATE TABLE events (id BIGINT, region TEXT) WITH ("acceleration.engine" = 'cayenne') PARTITION BY region"#,
                &Dialect::PostgreSQL,
            )
            .expect("PostgreSQL parses the statement without the clause");

        let Statement::Statement(parsed) = with_clause else {
            panic!("expected a SQL statement");
        };
        let SQLStatement::CreateTable(mut table) = *parsed else {
            panic!("expected CREATE TABLE");
        };
        let cluster_by = table.cluster_by.take().expect("the clause is kept");
        assert_eq!(cluster_by.to_string(), "(region, id)");
        assert_eq!(
            Statement::Statement(Box::new(SQLStatement::CreateTable(table))),
            without_clause
        );
    }

    fn mem_table() -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("mem table should be created"))
    }

    fn enrich(inner: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
        let mut metadata = HashMap::new();
        metadata.insert("source_owner".to_string(), "analytics".to_string());
        spice_table::SpiceTable::over(
            Arc::new(MetadataEnrichedTableProvider::new(&inner, metadata)),
            inner,
        )
    }

    /// A dataset declaring table- or column-level metadata carries a
    /// metadata-enrichment layer, and `is_dual_write_table_provider` has to see
    /// through it to reach the accelerated table. Missing it classifies a
    /// dual-write table as not-dual-write, and its INSERTs skip the
    /// distributed-insert rewrite.
    ///
    /// A `MemTable` stands in for the accelerated layer here — what matters is
    /// that the walk crosses the enrichment, not what it finds.
    #[test]
    fn a_layer_is_reachable_through_a_metadata_layer() {
        let wrapped = enrich(mem_table());
        assert!(
            spice_table::find_layer::<MetadataEnrichedTableProvider>(
                wrapped.as_ref(),
                spice_table::LayerWalk::Read
            )
            .is_some(),
            "the walk must reach a layer behind metadata enrichment"
        );
        assert!(
            spice_table::find_concrete::<MemTable>(wrapped.as_ref(), spice_table::LayerWalk::Read)
                .is_some(),
            "and must continue past it to the provider beneath"
        );
    }

    /// The layers nest (metadata over federated over metadata), so the walk must
    /// keep going rather than stop at the first one.
    #[test]
    fn a_provider_is_reachable_through_nested_metadata_layers() {
        let wrapped = enrich(enrich(mem_table()));
        assert!(
            spice_table::find_concrete::<MemTable>(wrapped.as_ref(), spice_table::LayerWalk::Read)
                .is_some(),
            "the walk must cross every nested enrichment layer"
        );
    }

    #[test]
    fn non_accelerated_metadata_wrapped_table_is_not_dual_write() {
        // A wrapped non-accelerated table must not be misclassified as dual-write — the
        // peel only routes the downcast, it does not invent an AcceleratedTable.
        let wrapped = enrich(mem_table());
        assert!(!super::is_dual_write_table_provider(&wrapped));
    }
}
