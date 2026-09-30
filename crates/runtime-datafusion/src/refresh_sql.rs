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

use std::fmt::Write;
use std::sync::Arc;

use crate::error::format_datafusion_error;
use arrow_schema::SchemaRef;
use arrow_tools::metadata_keys::INFERRED_INDEXES_METADATA_KEY;
use arrow_tools::schema::schema_meta_get_computed_columns;
use datafusion::arrow::datatypes::Schema;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::first_last::first_value_udaf;
use datafusion::logical_expr::{ExprFunctionExt, SortExpr, ident};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::planner::NullOrdering;
use datafusion::sql::sqlparser::ast::{
    Distinct, Expr, GroupByExpr, OrderByKind, OrderByOptions, SelectItem, SetExpr,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::{TableReference, sqlparser};
use datafusion_expr::sqlparser::ast::LimitClause;
use itertools::Itertools;
use snafu::prelude::*;
use sqlparser::ast::Statement as SQLStatement;

/// Columns selected in the refresh SQL.
#[derive(Clone, Debug)]
pub enum RefreshSQLColumns {
    /// SELECT * — all columns from source.
    All,
    /// SELECT col1, col2, ... — specific columns preserving original quoting/case.
    Named(Vec<sqlparser::ast::Ident>),
}

/// One `ORDER BY` column of a `DISTINCT ON` refresh SQL, with the user's direction
/// and NULL ordering as written (unset when omitted).
#[derive(Clone, Debug)]
pub struct RefreshSQLOrderBy {
    pub column: sqlparser::ast::Ident,
    pub options: OrderByOptions,
}

impl RefreshSQLOrderBy {
    /// Whether the column sorts ascending (`ASC` is the default).
    #[must_use]
    pub fn is_ascending(&self) -> bool {
        self.options.asc.unwrap_or(true)
    }

    /// The sort for this column, resolving an omitted `NULLS` clause the way SQL
    /// planning does, with the session's `default_null_ordering`.
    fn sort_expr(&self, null_ordering: NullOrdering) -> SortExpr {
        let asc = self.is_ascending();
        let nulls_first = self
            .options
            .nulls_first
            .unwrap_or_else(|| null_ordering.nulls_first(asc));
        ident(&self.column.value).sort(asc, nulls_first)
    }
}

impl std::fmt::Display for RefreshSQLOrderBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.column, self.options)
    }
}

/// `SELECT DISTINCT ON (...) ... ORDER BY ...` in a refresh SQL: keep one row per
/// distinct `on` value, the first by `order_by`.
#[derive(Clone, Debug)]
pub struct RefreshSQLDistinctOn {
    /// The `DISTINCT ON` columns, which must match the dataset's primary key.
    pub on: Vec<sqlparser::ast::Ident>,
    /// The full `ORDER BY`; it starts with the `on` columns.
    pub order_by: Vec<RefreshSQLOrderBy>,
}

impl RefreshSQLDistinctOn {
    /// The `ORDER BY` columns after the `DISTINCT ON` columns: the ones that
    /// decide which row of each key is kept.
    pub(crate) fn selecting_columns(&self) -> &[RefreshSQLOrderBy] {
        self.order_by.get(self.on.len()..).unwrap_or_default()
    }

    /// Keep the first row per `on` value of `df`, by the selecting columns.
    ///
    /// Planned directly as a grouped aggregate with an ordered `first_value` per
    /// non-key column, the plan `DataFusion` rewrites `DISTINCT ON` into, but without
    /// the sort on the key it adds above that aggregate to order the output. A refresh
    /// writes rows in any order, and that sort would buffer every kept row a second time.
    pub(crate) fn apply(
        &self,
        df: DataFrame,
        null_ordering: NullOrdering,
    ) -> datafusion::error::Result<DataFrame> {
        let order_by: Vec<SortExpr> = self
            .selecting_columns()
            .iter()
            .map(|o| o.sort_expr(null_ordering))
            .collect();
        let keys: Vec<&str> = self.on.iter().map(|c| c.value.as_str()).collect();
        let columns: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let first_values = columns
            .iter()
            .filter(|name| !keys.contains(&name.as_str()))
            .map(|name| {
                first_value_udaf()
                    .call(vec![ident(name)])
                    .order_by(order_by.clone())
                    .build()
                    .map(|e| e.alias(name))
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        df.aggregate(keys.iter().map(|k| ident(*k)).collect(), first_values)?
            .select(columns.iter().map(ident))
    }
}

/// Structured representation of refresh SQL, decomposed into validated parts.
/// The user's `refresh_sql` config is parsed into columns + `user_filters` + limit.
/// System-generated filters (partitions) are stored separately.
#[derive(Clone, Debug)]
pub struct RefreshSQL {
    /// The source table this refresh targets.
    table: TableReference,
    /// Column projection (All or specific named columns).
    columns: RefreshSQLColumns,
    /// User-provided WHERE predicates from `refresh_sql`, split on top-level `AND`.
    /// Stored as sqlparser `AST` `Expr`s so they can be recombined with system filters.
    user_filters: Vec<sqlparser::ast::Expr>,
    /// LIMIT clause from user SQL, if any.
    limit: Option<usize>,
    /// `DISTINCT ON (...) ... ORDER BY ...` from user SQL, if any. Applied after
    /// every refresh filter, so an append refresh selects within its window.
    distinct_on: Option<RefreshSQLDistinctOn>,
    /// Cluster partition filter expressions (`DataFusion` Exprs), applied as
    /// `DataFrame` `.filter()` calls at refresh time. Three-state:
    /// - `None` — the table is not partition-scoped (non-clustered, or this
    ///   node is not an executor); apply no partition predicate and retrieve
    ///   everything.
    /// - `Some(filters)` (non-empty) — apply the assigned partitions' predicate.
    /// - `Some(empty)` — this executor owns no partition of the table; apply a
    ///   `false` predicate so no rows are loaded, rather than the whole table.
    partition_filters: Option<Vec<datafusion_expr::Expr>>,
}

impl RefreshSQL {
    /// Create a new `RefreshSQL` with the given parts.
    #[must_use]
    pub fn new(
        table: TableReference,
        columns: RefreshSQLColumns,
        user_filters: Vec<sqlparser::ast::Expr>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            table,
            columns,
            user_filters,
            limit,
            distinct_on: None,
            partition_filters: None,
        }
    }

    /// Set the `DISTINCT ON` clause.
    #[must_use]
    pub fn with_distinct_on(mut self, distinct_on: Option<RefreshSQLDistinctOn>) -> Self {
        self.distinct_on = distinct_on;
        self
    }

    /// The `DISTINCT ON` clause, if the refresh SQL has one.
    #[must_use]
    pub fn distinct_on(&self) -> Option<&RefreshSQLDistinctOn> {
        self.distinct_on.as_ref()
    }

    /// Reconstruct the user SQL from parts: `SELECT [DISTINCT ON (...)] {columns} FROM {table}
    /// WHERE {user_filters} [ORDER BY ...] LIMIT {limit}`.
    /// This does NOT include partition filters — those are applied as `DataFrame` filters.
    /// It is the identity of the refresh SQL (stored with checkpoints and compared to
    /// detect a change), not what a refresh executes: see [`Self::to_scan_sql`].
    #[must_use]
    pub fn to_sql(&self) -> String {
        self.render(true)
    }

    /// The SQL a refresh executes before its filters: [`Self::to_sql`] without
    /// `DISTINCT ON` and `ORDER BY`. The refresh applies its own filters (the append
    /// window, partitions) to this, then `DISTINCT ON`, so the selection only sees the
    /// rows the refresh reads. Planning `DISTINCT ON` in the SQL would leave those
    /// filters above it, where they cannot be pushed below the aggregate it becomes.
    #[must_use]
    pub(crate) fn to_scan_sql(&self) -> String {
        self.render(false)
    }

    fn render(&self, include_distinct_on: bool) -> String {
        let distinct_on = self.distinct_on.as_ref().filter(|_| include_distinct_on);
        let columns_str = match &self.columns {
            RefreshSQLColumns::All => "*".to_string(),
            RefreshSQLColumns::Named(idents) => idents
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        };

        let distinct_str = distinct_on
            .map(|d| format!("DISTINCT ON ({}) ", d.on.iter().join(", ")))
            .unwrap_or_default();
        let mut sql = format!("SELECT {distinct_str}{columns_str} FROM {}", self.table);

        if !self.user_filters.is_empty() {
            let where_clause = self
                .user_filters
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" AND ");
            let _ = write!(sql, " WHERE {where_clause}");
        }

        if let Some(d) = distinct_on {
            let _ = write!(sql, " ORDER BY {}", d.order_by.iter().join(", "));
        }

        if let Some(limit) = self.limit {
            let _ = write!(sql, " LIMIT {limit}");
        }

        sql
    }

    /// The partition filters as stored: `None` when the table is not
    /// partition-scoped, `Some` (possibly empty) when it is. Callers applying
    /// the predicate to a refresh should use
    /// [`Self::extend_effective_partition_filters`], which resolves the
    /// empty-`Some` case to a `false` predicate.
    #[must_use]
    pub fn partition_filters(&self) -> Option<&[datafusion_expr::Expr]> {
        self.partition_filters.as_deref()
    }

    /// Set the partition filters. Pass `None` for a non-partition-scoped table,
    /// or `Some(filters)` for the assigned partitions — where an empty `Vec`
    /// means this executor owns no partition of the table and should load no
    /// rows (see [`Self::extend_effective_partition_filters`]).
    pub fn set_partition_filters(&mut self, filters: Option<Vec<datafusion_expr::Expr>>) {
        self.partition_filters = filters;
    }

    /// Append the partition predicate(s) to AND into the refresh query onto
    /// `out`, resolving the three stored states without allocating a temporary
    /// `Vec`:
    /// - `None` → nothing appended (retrieve everything).
    /// - `Some(filters)` (non-empty) → the assigned partitions' predicate.
    /// - `Some(empty)` → a single `false` predicate, so an executor with no
    ///   assigned partition loads no rows instead of the whole table.
    pub fn extend_effective_partition_filters(&self, out: &mut Vec<datafusion_expr::Expr>) {
        match &self.partition_filters {
            None => {}
            Some(filters) if filters.is_empty() => out.push(datafusion_expr::lit(false)),
            Some(filters) => out.extend(filters.iter().cloned()),
        }
    }

    /// For logging/status display. Shows the user SQL and annotates the
    /// partition-filter state.
    #[must_use]
    pub fn display_sql(&self) -> String {
        let base = self.to_sql();
        match &self.partition_filters {
            None => base,
            Some(filters) if filters.is_empty() => {
                format!("{base} [partition filter: no partitions assigned — no rows]")
            }
            Some(filters) => format!("{base} [+{} partition filter(s)]", filters.len()),
        }
    }

    /// Returns the table reference.
    #[must_use]
    pub fn table(&self) -> &TableReference {
        &self.table
    }

    /// Returns the columns selection.
    #[must_use]
    pub fn columns(&self) -> &RefreshSQLColumns {
        &self.columns
    }

    /// Check that a `DISTINCT ON` refresh SQL keeps one row per primary key: its
    /// `DISTINCT ON` columns must be exactly `acceleration.primary_key` (in any order).
    /// Keeping one row per some other column set would still hand the accelerator
    /// several rows for one key in a single write.
    ///
    /// # Errors
    ///
    /// Returns an error if the refresh SQL uses `DISTINCT ON` and `primary_key` is
    /// empty, or its `DISTINCT ON` columns differ from it.
    pub fn validate_distinct_on_primary_key<S: AsRef<str>>(&self, primary_key: &[S]) -> Result<()> {
        let Some(distinct_on) = &self.distinct_on else {
            return Ok(());
        };
        let on: Vec<&str> = distinct_on
            .on
            .iter()
            .map(|i| i.value.as_str())
            .sorted()
            .collect();
        if primary_key.is_empty() {
            return DistinctOnRequiresPrimaryKeySnafu {
                distinct_on: on.join(", "),
            }
            .fail();
        }
        let pk: Vec<&str> = primary_key.iter().map(AsRef::as_ref).sorted().collect();
        ensure!(
            on == pk,
            DistinctOnPrimaryKeyMismatchSnafu {
                distinct_on: on.join(", "),
                primary_key: pk.join(", "),
            }
        );
        Ok(())
    }

    /// Info messages for `DISTINCT ON` ordering columns sorted descending with no
    /// `NULLS` clause, where `DataFusion` sorts NULLs first — so a row with a NULL
    /// there is the one kept. One message per such column.
    #[must_use]
    pub fn null_ordering_notices(&self, dataset: &TableReference) -> Vec<String> {
        let Some(distinct_on) = &self.distinct_on else {
            return Vec::new();
        };
        distinct_on
            .selecting_columns()
            .iter()
            .filter(|o| o.options.asc == Some(false) && o.options.nulls_first.is_none())
            .map(|o| distinct_on_nulls_first_notice(dataset, &o.column.value))
            .collect()
    }
}

/// The info message for a `DISTINCT ON` ordering column sorted `DESC` without a
/// `NULLS` clause.
#[must_use]
pub(crate) fn distinct_on_nulls_first_notice(dataset: &TableReference, column: &str) -> String {
    format!(
        "Dataset '{dataset}' 'acceleration.refresh_sql' keeps one row per key ordered by '{column}' DESC with no NULLS clause, so NULLs sort first and a row whose '{column}' is NULL is kept over rows with a value. Add NULLS LAST to keep the latest row with a value, or NULLS FIRST to keep this behavior without this message. See: https://spiceai.org/docs/reference/spicepod/datasets#accelerationrefresh_sql"
    )
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "The provided Refresh SQL could not be parsed. {} Check the SQL for syntax errors.",
        format_datafusion_error(source)
    ))]
    UnableToParseSql { source: DataFusionError },

    #[snafu(display(
        "Expected a single SQL statement for the refresh SQL, found {num_statements}. Rewrite the SQL to only contain a single SELECT statement."
    ))]
    ExpectedSingleSqlStatement { num_statements: usize },

    #[snafu(display(
        "Expected a SQL query starting with SELECT <columns> FROM {expected_table}. {issue}"
    ))]
    InvalidSqlStatement {
        expected_table: TableReference,
        issue: String,
    },

    #[snafu(display(
        "Unexpected '{expr}' in the Refresh SQL statement. Rewrite the SQL to only perform WHERE filters, i.e. SELECT col1, col2, col3 FROM {expected_table} WHERE col1 = 'foo'"
    ))]
    UnexpectedExpression {
        expr: &'static str,
        expected_table: TableReference,
    },

    #[snafu(display(
        "Only column references are allowed in the SELECT clause of the refresh SQL, custom expressions and aliases are not supported. Change the SQL to only use columns references, i.e. SELECT col1, col2, col3 FROM {expected_table}"
    ))]
    OnlyColumnReferences { expected_table: TableReference },

    #[snafu(display(
        "The column '{column}' is not present in the source table '{expected_table}', valid columns are: {valid_columns} Rewrite the SQL to only select columns that exist in the source table."
    ))]
    ColumnNotFoundInSource {
        column: Arc<str>,
        valid_columns: Arc<str>,
        expected_table: TableReference,
    },

    #[snafu(display("Missing expected SQL statement - this is a bug in Spice.ai"))]
    MissingStatement,

    #[snafu(display(
        "The refresh SQL uses {clause}, which is supported only as SELECT DISTINCT ON (<primary key columns>) ... ORDER BY <primary key columns>, <column> [ASC|DESC] [NULLS FIRST|LAST] to keep one row per key. {issue} See: https://spiceai.org/docs/reference/spicepod/datasets#accelerationrefresh_sql"
    ))]
    InvalidDistinctOn { clause: &'static str, issue: String },

    #[snafu(display(
        "The refresh SQL keeps one row per ({distinct_on}) with DISTINCT ON, but the dataset has no 'acceleration.primary_key', so rows cannot be matched to the ones already loaded. Set 'acceleration.primary_key' to the DISTINCT ON columns. See: https://spiceai.org/docs/reference/spicepod/datasets#accelerationrefresh_sql"
    ))]
    DistinctOnRequiresPrimaryKey { distinct_on: String },

    #[snafu(display(
        "The refresh SQL keeps one row per ({distinct_on}) with DISTINCT ON, but 'acceleration.primary_key' is ({primary_key}), so a refresh could still load several rows for one key. Make the DISTINCT ON columns match 'acceleration.primary_key'. See: https://spiceai.org/docs/reference/spicepod/datasets#accelerationrefresh_sql"
    ))]
    DistinctOnPrimaryKeyMismatch {
        distinct_on: String,
        primary_key: String,
    },

    #[snafu(display(
        "The refresh SQL uses DISTINCT ON, which selects among the rows a refresh reads from the source, but {reason}. Remove DISTINCT ON and ORDER BY from 'acceleration.refresh_sql', or use 'acceleration.refresh_mode: full' or 'append'. See: https://spiceai.org/docs/reference/spicepod/datasets#accelerationrefresh_sql"
    ))]
    DistinctOnNotSupported { reason: &'static str },
}

/// The error for a `DISTINCT ON` refresh SQL on a refresh that never reads the source
/// through the refresh scan, where it would be silently ignored. `reason` is the cause
/// clause of the message.
#[must_use]
pub fn distinct_on_not_supported(reason: &'static str) -> Error {
    Error::DistinctOnNotSupported { reason }
}

macro_rules! ensure_no_expr {
    ($condition:expr, $expr_name:expr, $expected_table:expr) => {
        ensure!(
            $condition,
            UnexpectedExpressionSnafu {
                expr: $expr_name,
                expected_table: $expected_table.clone(),
            }
        );
    };
}

/// Parse and validate a refresh SQL string, returning both a structured
/// [`RefreshSQL`] and the schema the projection yields.
///
/// # Errors
///
/// Returns an error if `refresh_sql` is not a single `SELECT` statement, selects
/// from a table other than `expected_table`, projects a column absent from
/// `source_schema`, or uses a construct the refresh path cannot honor (`GROUP BY`,
/// a non-column projection, or an unsupported `LIMIT` clause).
pub fn parse_refresh_sql(
    expected_table: TableReference,
    refresh_sql: &str,
    source_schema: Arc<Schema>,
) -> Result<(RefreshSQL, Arc<Schema>)> {
    let mut statements = DFParser::parse_sql_with_dialect(refresh_sql, &PostgreSqlDialect {})
        .context(UnableToParseSqlSnafu)?;
    if statements.len() != 1 {
        ExpectedSingleSqlStatementSnafu {
            num_statements: statements.len(),
        }
        .fail()?;
    }

    let statement = statements.pop_front().context(MissingStatementSnafu)?;
    match statement {
        Statement::Statement(statement) => match statement.as_ref() {
            SQLStatement::Query(query) => {
                ensure_no_expr!(query.fetch.is_none(), "FETCH", expected_table);
                ensure_no_expr!(query.with.is_none(), "WITH", expected_table);
                // ORDER BY is accepted only as part of DISTINCT ON, checked below.
                ensure_no_expr!(query.for_clause.is_none(), "FOR", expected_table);

                let limit_value = parse_limit(query.limit_clause.as_ref(), &expected_table)?;

                ensure_no_expr!(query.format_clause.is_none(), "FORMAT", expected_table);
                ensure_no_expr!(query.settings.is_none(), "SETTINGS", expected_table);

                match query.body.as_ref() {
                    SetExpr::Select(select) => {
                        let (columns, refresh_schema) = validate_and_extract_columns(
                            &select.projection,
                            source_schema,
                            &expected_table,
                        )?;
                        ensure!(
                            select.from.len() == 1,
                            InvalidSqlStatementSnafu {
                                expected_table,
                                issue: format!(
                                    "A single FROM clause with table reference is expected, found {}",
                                    select.from.len()
                                )
                            }
                        );

                        ensure_no_expr!(select.cluster_by.is_empty(), "CLUSTER BY", expected_table);
                        ensure_no_expr!(select.connect_by.is_empty(), "CONNECT BY", expected_table);
                        let distinct_on = match &select.distinct {
                            None => {
                                ensure_no_expr!(
                                    query.order_by.is_none(),
                                    "ORDER BY",
                                    expected_table
                                );
                                None
                            }
                            Some(Distinct::On(on)) => Some(parse_distinct_on(
                                on,
                                query.order_by.as_ref(),
                                limit_value,
                                &refresh_schema,
                            )?),
                            Some(_) => {
                                return UnexpectedExpressionSnafu {
                                    expr: "DISTINCT",
                                    expected_table,
                                }
                                .fail();
                            }
                        };
                        ensure_no_expr!(
                            select.distribute_by.is_empty(),
                            "DISTRIBUTE BY",
                            expected_table
                        );

                        match &select.group_by {
                            GroupByExpr::All(modifiers) => {
                                ensure_no_expr!(modifiers.is_empty(), "GROUP BY", expected_table);
                            }
                            GroupByExpr::Expressions(exprs, modifiers) => {
                                ensure_no_expr!(exprs.is_empty(), "GROUP BY", expected_table);
                                ensure_no_expr!(modifiers.is_empty(), "GROUP BY", expected_table);
                            }
                        }

                        ensure_no_expr!(select.having.is_none(), "HAVING", expected_table);
                        ensure_no_expr!(select.into.is_none(), "INTO", expected_table);
                        ensure_no_expr!(
                            select.lateral_views.is_empty(),
                            "LATERAL VIEW",
                            expected_table
                        );
                        ensure_no_expr!(select.named_window.is_empty(), "WINDOW", expected_table);
                        ensure_no_expr!(select.prewhere.is_none(), "PREWHERE", expected_table);
                        ensure_no_expr!(select.qualify.is_none(), "QUALIFY", expected_table);
                        ensure_no_expr!(select.sort_by.is_empty(), "SORT BY", expected_table);
                        ensure_no_expr!(select.top.is_none(), "TOP", expected_table);
                        ensure_no_expr!(
                            select.value_table_mode.is_none(),
                            "AS VALUE",
                            expected_table
                        );

                        match &select.from[0].relation {
                            sqlparser::ast::TableFactor::Table { name, .. } => {
                                let table_name_with_schema = name
                                    .0
                                    .iter()
                                    .map(ToString::to_string)
                                    .collect::<Vec<_>>()
                                    .join(".");
                                if TableReference::parse_str(&table_name_with_schema)
                                    != expected_table
                                {
                                    return InvalidSqlStatementSnafu {
                                        expected_table: expected_table.clone(),
                                        issue: format!(
                                            "Table name in refresh_sql should be {expected_table}, is {name}",
                                        ),
                                    }
                                    .fail();
                                }
                            }
                            _ => {
                                return InvalidSqlStatementSnafu {
                                    expected_table,
                                    issue:
                                        "No FROM clause with table reference found in SQL statement"
                                            .to_string(),
                                }
                                .fail();
                            }
                        }

                        // Extract user WHERE filters, split on top-level AND
                        let user_filters = select
                            .selection
                            .as_ref()
                            .map(|expr| split_conjunction(expr.clone()))
                            .unwrap_or_default();

                        let refresh_sql =
                            RefreshSQL::new(expected_table, columns, user_filters, limit_value)
                                .with_distinct_on(distinct_on);

                        Ok((refresh_sql, refresh_schema))
                    }
                    _ => InvalidSqlStatementSnafu {
                        expected_table,
                        issue: format!(
                            "Expected a basic Select SQL query statement, found {}",
                            query.body
                        ),
                    }
                    .fail()?,
                }
            }
            _ => InvalidSqlStatementSnafu {
                expected_table,
                issue: format!("Expected a Select SQL query statement, found {statement}"),
            }
            .fail()?,
        },
        _ => InvalidSqlStatementSnafu {
            expected_table,
            issue: format!("Expected a SQL Statement, found {statement}"),
        }
        .fail()?,
    }
}

/// Extract and validate the LIMIT clause, ensuring it is a simple non-negative integer literal if present, and that no unsupported features like OFFSET or LIMIT BY are used.
fn parse_limit(limit: Option<&LimitClause>, tbl: &TableReference) -> Result<Option<usize>> {
    let (limit, offset, limit_by) = match limit {
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => (limit, offset, limit_by),
        None => return Ok(None), // No LIMIT clause specified, treated as no limit
        _ => {
            return UnexpectedExpressionSnafu {
                expr: "unsupported LIMIT clause; expected LIMIT <non-negative integer literal>",
                expected_table: tbl.clone(),
            }
            .fail();
        }
    };

    ensure_no_expr!(limit_by.is_empty(), "LIMIT BY", tbl.clone());
    ensure_no_expr!(offset.is_none(), "OFFSET", tbl.clone());

    match &limit {
        Some(Expr::Value(v)) => v
            .to_string()
            .parse::<usize>()
            .map_err(|_| Error::UnexpectedExpression {
                expr: "non-negative integer literal LIMIT",
                expected_table: tbl.clone(),
            })
            .map(Some),
        None => Ok(None), // No LIMIT value specified, treated as no limit
        _ => UnexpectedExpressionSnafu {
            expr: "non-negative integer literal LIMIT",
            expected_table: tbl.clone(),
        }
        .fail(),
    }
}

/// Validate `DISTINCT ON (...) ... ORDER BY ...`: plain column references to columns the
/// refresh SQL selects (the selection is applied to the selected rows), an `ORDER BY` that starts with exactly the `DISTINCT ON` columns (the
/// same rule `PostgreSQL` enforces) and orders by at least one more column, and no `LIMIT`.
fn parse_distinct_on(
    on: &[Expr],
    order_by: Option<&sqlparser::ast::OrderBy>,
    limit: Option<usize>,
    selected_schema: &Schema,
) -> Result<RefreshSQLDistinctOn> {
    let invalid = |issue: String| InvalidDistinctOnSnafu {
        clause: "DISTINCT ON",
        issue,
    };
    let column = |expr: &Expr, place: &str| -> Result<sqlparser::ast::Ident> {
        let Expr::Identifier(ident) = expr else {
            return invalid(format!(
                "'{expr}' in {place} is not a column name; only column names are allowed."
            ))
            .fail();
        };
        ensure!(
            selected_schema.field_with_name(&ident.value).is_ok(),
            invalid(format!(
                "Column '{}' in {place} is not selected by the refresh SQL; add it to the SELECT list or use SELECT *.",
                ident.value
            ))
        );
        Ok(ident.clone())
    };

    ensure!(
        !on.is_empty(),
        invalid("DISTINCT ON lists no columns.".to_string())
    );
    let on = on
        .iter()
        .map(|expr| column(expr, "DISTINCT ON"))
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        limit.is_none(),
        invalid("LIMIT cannot be combined with DISTINCT ON; remove the LIMIT.".to_string())
    );

    let Some(order_by) = order_by else {
        return invalid(
            "There is no ORDER BY, so which row of each key is kept is undefined. Add an ORDER BY."
                .to_string(),
        )
        .fail();
    };
    let OrderByKind::Expressions(exprs) = &order_by.kind else {
        return invalid("ORDER BY ALL is not supported; list the columns.".to_string()).fail();
    };
    ensure!(
        order_by.interpolate.is_none(),
        invalid("INTERPOLATE is not supported.".to_string())
    );
    let order_by = exprs
        .iter()
        .map(|o| {
            ensure!(
                o.with_fill.is_none(),
                invalid("WITH FILL is not supported.".to_string())
            );
            Ok(RefreshSQLOrderBy {
                column: column(&o.expr, "ORDER BY")?,
                options: o.options,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let leading: std::collections::BTreeSet<&str> = order_by
        .iter()
        .take(on.len())
        .map(|o| o.column.value.as_str())
        .collect();
    let keys: std::collections::BTreeSet<&str> = on.iter().map(|i| i.value.as_str()).collect();
    ensure!(
        order_by.len() >= on.len() && leading == keys,
        invalid(format!(
            "ORDER BY must start with the DISTINCT ON columns ({}).",
            on.iter().join(", ")
        ))
    );
    ensure!(
        order_by.len() > on.len(),
        invalid(
            "ORDER BY lists only the DISTINCT ON columns, so which row of each key is kept is undefined. Add the column that orders a key's rows, e.g. its time column DESC NULLS LAST."
                .to_string()
        )
    );

    Ok(RefreshSQLDistinctOn { on, order_by })
}

/// Validate select columns and extract both the `RefreshSQLColumns` and the resulting schema.
fn validate_and_extract_columns(
    select: &Vec<SelectItem>,
    source_schema: Arc<Schema>,
    expected_table: &TableReference,
) -> Result<(RefreshSQLColumns, Arc<Schema>)> {
    // Wildcard will select all columns
    if select.len() == 1 && matches!(select[0], SelectItem::Wildcard(_)) {
        return Ok((RefreshSQLColumns::All, source_schema));
    }

    let mut fields = vec![];
    let mut column_idents = vec![];
    for select_item in select {
        match select_item {
            SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let column_name = ident.value.as_str();
                    let Ok(field) = source_schema.field_with_name(column_name) else {
                        return ColumnNotFoundInSourceSnafu {
                            column: Arc::from(column_name),
                            valid_columns: Arc::from(
                                source_schema.fields().iter().map(|f| f.name()).join(", "),
                            ),
                            expected_table: expected_table.clone(),
                        }
                        .fail();
                    };
                    fields.push(field.clone());
                    column_idents.push(ident.clone());
                }
                _ => {
                    return OnlyColumnReferencesSnafu {
                        expected_table: expected_table.clone(),
                    }
                    .fail();
                }
            },
            SelectItem::ExprWithAlias { .. }
            | SelectItem::ExprWithAliases { .. }
            | SelectItem::QualifiedWildcard(..)
            | SelectItem::Wildcard(..) => {
                return OnlyColumnReferencesSnafu {
                    expected_table: expected_table.clone(),
                }
                .fail();
            }
        }
    }

    // If the refresh SQL defines a subset of columns to fetch, computed columns (e.g., embeddings)
    // are not included automatically. We verify their presence in the source schema and add them manually if needed.
    fields = include_computed_columns(&fields, &source_schema);

    // Keep the source's inferred secondary indexes, which the unprojected schema above
    // carries in full: the `DuckDB` accelerator reads them to find the indexes an earlier
    // schema inference copied onto a stored table. The other inferred hints (sizing,
    // column statistics) describe every source row and do not hold once a refresh SQL
    // selects fewer.
    let metadata = source_schema
        .metadata()
        .get(INFERRED_INDEXES_METADATA_KEY)
        .map(|indexes| {
            std::collections::HashMap::from([(
                INFERRED_INDEXES_METADATA_KEY.to_string(),
                indexes.clone(),
            )])
        })
        .unwrap_or_default();

    Ok((
        RefreshSQLColumns::Named(column_idents),
        Arc::new(Schema::new_with_metadata(fields, metadata)),
    ))
}

/// Split a WHERE expression on top-level AND into individual predicates.
fn split_conjunction(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::And,
            right,
        } => {
            let mut parts = split_conjunction(*left);
            parts.extend(split_conjunction(*right));
            parts
        }
        other => vec![other],
    }
}

/// Checks the source schema for associated computed columns (e.g., embeddings)
/// and adds any missing computed fields to the target schema if they are found.
fn include_computed_columns(
    fields: &[arrow_schema::Field],
    source_schema: &SchemaRef,
) -> Vec<arrow_schema::Field> {
    let mut extended_fields = fields.to_owned();
    for field in fields {
        if let Some(computed_cols) = schema_meta_get_computed_columns(source_schema, field.name()) {
            for computed_col in computed_cols {
                // Add field only if it does not exist in target schema
                if !extended_fields
                    .iter()
                    .any(|f| f.name() == computed_col.name())
                {
                    extended_fields.push((*computed_col).clone());
                }
            }
        }
    }

    extended_fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn create_test_schema_with_enmbeddings() -> Arc<Schema> {
        let mut schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
            Field::new(
                "name_embedding",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, false)),
                        1536,
                    ),
                    false,
                ))),
                false,
            ),
            Field::new(
                "name_offset",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Int32, false)),
                        2,
                    ),
                    false,
                ))),
                false,
            ),
        ]);

        // mark `name_embedding` and `name_offset` as computed columns for `name`
        let mut computed_columns_meta = std::collections::HashMap::new();
        computed_columns_meta.insert(
            "name".to_string(),
            vec!["name_embedding".to_string(), "name_offset".to_string()],
        );
        arrow_tools::schema::set_computed_columns_meta(&mut schema, &computed_columns_meta);

        Arc::new(schema)
    }

    #[test]
    fn test_valid_select_all() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT * FROM test_table";

        let (refresh_sql, result_schema) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 3);
        assert!(matches!(refresh_sql.columns(), RefreshSQLColumns::All));
        assert_eq!(refresh_sql.to_sql(), "SELECT * FROM test_table");
        Ok(())
    }

    #[test]
    fn test_valid_select_columns() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id, name FROM test_table";

        let (refresh_sql, result_schema) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 2);
        assert_eq!(result_schema.field(0).name(), "id");
        assert_eq!(result_schema.field(1).name(), "name");
        assert!(matches!(refresh_sql.columns(), RefreshSQLColumns::Named(_)));
        assert_eq!(refresh_sql.to_sql(), "SELECT id, name FROM test_table");
        Ok(())
    }

    // Regression test for #13929: the DuckDB accelerator reads the inferred indexes from
    // the projected schema to drop the ones an earlier inference installed.
    #[test]
    fn test_select_columns_keeps_only_the_inferred_indexes_metadata() -> Result<()> {
        use arrow_tools::metadata_keys::INFERRED_ROW_COUNT_METADATA_KEY;

        let source = create_test_schema();
        let mut metadata = source.metadata().clone();
        metadata.insert(
            INFERRED_INDEXES_METADATA_KEY.to_string(),
            r#"[{"columns":["name"],"unique":false}]"#.to_string(),
        );
        metadata.insert(
            INFERRED_ROW_COUNT_METADATA_KEY.to_string(),
            "1000".to_string(),
        );
        let schema = Arc::new(source.as_ref().clone().with_metadata(metadata));
        let table = TableReference::parse_str("test_table");

        let (_, result_schema) =
            parse_refresh_sql(table, "SELECT id, name FROM test_table", schema)?;
        assert_eq!(
            result_schema
                .metadata()
                .get(INFERRED_INDEXES_METADATA_KEY)
                .map(String::as_str),
            Some(r#"[{"columns":["name"],"unique":false}]"#)
        );
        assert!(
            !result_schema
                .metadata()
                .contains_key(INFERRED_ROW_COUNT_METADATA_KEY),
            "a row count inferred over the whole source does not describe a projection"
        );
        Ok(())
    }

    #[test]
    fn test_invalid_column() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id, invalid_column FROM test_table";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::ColumnNotFoundInSource { .. })));
    }

    #[test]
    fn test_invalid_table() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id FROM wrong_table";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::InvalidSqlStatement { .. })));
    }

    #[test]
    fn test_invalid_expression() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id + 1 FROM test_table";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::OnlyColumnReferences { .. })));
    }

    #[test]
    fn test_invalid_alias() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id as user_id FROM test_table";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::OnlyColumnReferences { .. })));
    }

    #[test]
    fn test_invalid_group_by() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id FROM test_table GROUP BY id";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(result, Err(Error::UnexpectedExpression { .. })));
    }

    #[test]
    fn test_multiple_statements() {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id FROM test_table; SELECT name FROM test_table";

        let result = parse_refresh_sql(table, sql, Arc::clone(&schema));
        assert!(matches!(
            result,
            Err(Error::ExpectedSingleSqlStatement { .. })
        ));
    }

    #[test]
    fn test_valid_select_columns_with_embeddings() -> Result<()> {
        let schema = create_test_schema_with_enmbeddings();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT id, name FROM test_table";

        let (_, result_schema) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 4);
        assert_eq!(result_schema.field(0).name(), "id");
        assert_eq!(result_schema.field(1).name(), "name");
        assert_eq!(result_schema.field(2).name(), "name_embedding");
        assert_eq!(result_schema.field(3).name(), "name_offset");
        Ok(())
    }

    #[test]
    fn test_valid_where_with_scalar_function() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("organization_id", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let table = TableReference::parse_str("test_table");

        // bucket() in a WHERE clause should be accepted by refresh SQL validation.
        let sql = "SELECT * FROM test_table WHERE bucket(50, organization_id) = 0";
        let (refresh_sql, result_schema) =
            parse_refresh_sql(table.clone(), sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 3);
        assert_eq!(
            refresh_sql.to_sql(),
            "SELECT * FROM test_table WHERE bucket(50, organization_id) = 0"
        );

        // Multiple OR'd bucket() filters (as generated by partition assignment).
        let sql = "SELECT * FROM test_table WHERE bucket(50, organization_id) = 0 OR bucket(50, organization_id) = 1";
        let (refresh_sql, result_schema) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 3);
        // The OR expression stays as a single filter since OR is not top-level AND
        assert_eq!(
            refresh_sql.to_sql(),
            "SELECT * FROM test_table WHERE bucket(50, organization_id) = 0 OR bucket(50, organization_id) = 1"
        );

        Ok(())
    }

    #[test]
    fn test_where_with_and() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT * FROM test_table WHERE id > 10 AND name = 'foo'";

        let (refresh_sql, _) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        // Should split on top-level AND into 2 user_filters
        let reconstructed = refresh_sql.to_sql();
        assert!(reconstructed.contains("WHERE"));
        assert!(reconstructed.contains("id > 10"));
        assert!(reconstructed.contains("name = 'foo'"));
        Ok(())
    }

    #[test]
    fn test_with_limit() -> Result<()> {
        let schema = create_test_schema();
        let table = TableReference::parse_str("test_table");
        let sql = "SELECT * FROM test_table LIMIT 100";

        let (refresh_sql, _) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert!(refresh_sql.to_sql().contains("LIMIT 100"));
        Ok(())
    }

    #[test]
    fn test_split_conjunction() {
        use sqlparser::ast::{BinaryOperator, Value};

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Number("1".to_string(), false).into())),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(Value::Number("2".to_string(), false).into())),
                op: BinaryOperator::And,
                right: Box::new(Expr::Value(Value::Number("3".to_string(), false).into())),
            }),
        };

        let parts = split_conjunction(expr);
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn test_quoted_identifiers_preserved() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("MyCol", DataType::Int64, false),
            Field::new("another col", DataType::Utf8, false),
        ]));
        let table = TableReference::parse_str("test_table");
        let sql = r#"SELECT "MyCol", "another col" FROM test_table"#;

        let (refresh_sql, result_schema) = parse_refresh_sql(table, sql, Arc::clone(&schema))?;
        assert_eq!(result_schema.fields().len(), 2);
        // to_sql() should preserve the double-quoting
        let reconstructed = refresh_sql.to_sql();
        assert!(
            reconstructed.contains(r#""MyCol""#),
            "Expected quoted identifier in: {reconstructed}"
        );
        assert!(
            reconstructed.contains(r#""another col""#),
            "Expected quoted identifier in: {reconstructed}"
        );
        Ok(())
    }

    fn distinct_on_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("occurred_at", DataType::Int64, true),
            Field::new("v", DataType::Utf8, true),
        ]))
    }

    fn parse_events(sql: &str) -> Result<RefreshSQL> {
        parse_refresh_sql(
            TableReference::parse_str("events"),
            sql,
            distinct_on_schema(),
        )
        .map(|(refresh_sql, _)| refresh_sql)
    }

    #[test]
    fn test_distinct_on_round_trips_and_scans_without_it() -> Result<()> {
        let sql = "SELECT DISTINCT ON (id) * FROM events WHERE v IS NOT NULL ORDER BY id, occurred_at DESC NULLS LAST";
        let refresh_sql = parse_events(sql)?;
        assert_eq!(refresh_sql.to_sql(), sql);
        assert_eq!(
            refresh_sql.to_scan_sql(),
            "SELECT * FROM events WHERE v IS NOT NULL"
        );
        let distinct_on = refresh_sql.distinct_on().expect("DISTINCT ON is parsed");
        assert_eq!(distinct_on.on.len(), 1);
        assert_eq!(distinct_on.selecting_columns().len(), 1);
        assert!(!distinct_on.selecting_columns()[0].is_ascending());
        assert_eq!(
            distinct_on.selecting_columns()[0].options.nulls_first,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn test_distinct_on_composite_key_in_any_order() -> Result<()> {
        let refresh_sql = parse_events(
            "SELECT DISTINCT ON (id, region) id, region, occurred_at, v FROM events ORDER BY region, id, occurred_at DESC",
        )?;
        refresh_sql.validate_distinct_on_primary_key(&["region", "id"])?;
        Ok(())
    }

    #[test]
    fn test_order_by_without_distinct_on_is_still_rejected() {
        let result = parse_events("SELECT * FROM events ORDER BY occurred_at");
        assert!(matches!(
            result,
            Err(Error::UnexpectedExpression {
                expr: "ORDER BY",
                ..
            })
        ));
    }

    #[test]
    fn test_plain_distinct_is_still_rejected() {
        let result = parse_events("SELECT DISTINCT * FROM events");
        assert!(matches!(
            result,
            Err(Error::UnexpectedExpression {
                expr: "DISTINCT",
                ..
            })
        ));
    }

    #[test]
    fn test_invalid_distinct_on_shapes_are_rejected() {
        for (sql, expected) in [
            (
                "SELECT DISTINCT ON (id) * FROM events",
                "There is no ORDER BY",
            ),
            (
                "SELECT DISTINCT ON (id) * FROM events ORDER BY occurred_at DESC, id",
                "must start with the DISTINCT ON columns (id)",
            ),
            (
                "SELECT DISTINCT ON (id) * FROM events ORDER BY id",
                "ORDER BY lists only the DISTINCT ON columns",
            ),
            (
                "SELECT DISTINCT ON (id) * FROM events ORDER BY id, occurred_at DESC LIMIT 10",
                "LIMIT cannot be combined with DISTINCT ON",
            ),
            (
                "SELECT DISTINCT ON (id) * FROM events ORDER BY id, occurred_at + 1 DESC",
                "is not a column name",
            ),
            (
                "SELECT DISTINCT ON (id) * FROM events ORDER BY id, missing DESC",
                "Column 'missing' in ORDER BY is not selected by the refresh SQL",
            ),
            (
                "SELECT DISTINCT ON (id) id, v FROM events ORDER BY id, occurred_at DESC",
                "Column 'occurred_at' in ORDER BY is not selected by the refresh SQL",
            ),
        ] {
            let err = parse_events(sql).expect_err("invalid DISTINCT ON should be rejected");
            assert!(
                matches!(err, Error::InvalidDistinctOn { .. }),
                "{sql}: unexpected error {err}"
            );
            assert!(err.to_string().contains(expected), "{sql}: {err}");
        }
    }

    #[test]
    fn test_distinct_on_must_match_primary_key() -> Result<()> {
        let refresh_sql =
            parse_events("SELECT DISTINCT ON (id) * FROM events ORDER BY id, occurred_at DESC")?;
        let missing = refresh_sql
            .validate_distinct_on_primary_key::<&str>(&[])
            .expect_err("DISTINCT ON without a primary key is rejected");
        assert_eq!(
            missing.to_string(),
            "The refresh SQL keeps one row per (id) with DISTINCT ON, but the dataset has no 'acceleration.primary_key', so rows cannot be matched to the ones already loaded. Set 'acceleration.primary_key' to the DISTINCT ON columns. See: https://spiceai.org/docs/reference/spicepod/datasets#accelerationrefresh_sql"
        );
        let mismatch = refresh_sql
            .validate_distinct_on_primary_key(&["id", "region"])
            .expect_err("DISTINCT ON columns that differ from the primary key are rejected");
        assert!(matches!(
            mismatch,
            Error::DistinctOnPrimaryKeyMismatch { .. }
        ));
        refresh_sql.validate_distinct_on_primary_key(&["id"])?;
        Ok(())
    }

    #[test]
    fn test_null_ordering_notice_only_for_desc_without_nulls_clause() -> Result<()> {
        let dataset = TableReference::parse_str("events");
        let notices = parse_events(
            "SELECT DISTINCT ON (id) * FROM events ORDER BY id DESC, occurred_at DESC",
        )?
        .null_ordering_notices(&dataset);
        assert_eq!(
            notices,
            vec![distinct_on_nulls_first_notice(&dataset, "occurred_at")],
            "only the ordering column after the key is reported, not the key itself"
        );
        assert!(notices[0].contains("'occurred_at' DESC with no NULLS clause"));
        for sql in [
            "SELECT DISTINCT ON (id) * FROM events ORDER BY id, occurred_at DESC NULLS LAST",
            "SELECT DISTINCT ON (id) * FROM events ORDER BY id, occurred_at DESC NULLS FIRST",
            "SELECT DISTINCT ON (id) * FROM events ORDER BY id, occurred_at",
        ] {
            assert!(
                parse_events(sql)?
                    .null_ordering_notices(&dataset)
                    .is_empty(),
                "{sql}"
            );
        }
        Ok(())
    }
}
