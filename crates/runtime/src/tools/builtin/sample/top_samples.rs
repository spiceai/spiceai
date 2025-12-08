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

use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use super::SampleFrom;
use crate::datafusion::DataFusion;
use arrow::{array::RecordBatch, compute::concat_batches};
use datafusion::{
    error::DataFusionError,
    sql::{
        parser::{DFParser, Statement},
        sqlparser::{
            ast::{
                Expr as SqlExpr, Ident, OrderBy, OrderByExpr, OrderByKind, Query,
                Statement as SQLStatement,
            },
            dialect::PostgreSqlDialect,
        },
    },
};
use futures::TryStreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu, ensure};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse order_by '{order_by}' for top_n_sample: {source}"))]
    UnableToParseOrderBy {
        source: DataFusionError,
        order_by: String,
    },

    #[snafu(display(
        "Invalid order_by '{order_by}' for top_n_sample. Provide a single column reference optionally followed by ASC or DESC."
    ))]
    InvalidOrderBy { order_by: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TopSamplesParams {
    #[serde(rename = "dataset")]
    /// The SQL dataset to sample data from.
    pub tbl: String,
    /// The number of rows, each with distinct values per column, to sample.
    pub limit: usize,

    /// How to order the samples before retrieving the top N.
    pub order_by: String,
}

impl Display for TopSamplesParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

impl SampleFrom for TopSamplesParams {
    async fn sample(
        &self,
        df: Arc<DataFusion>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let order_by = sanitize_order_by(self.order_by.as_str())
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;

        let batches = df
            .query_builder(&format!(
                "SELECT * FROM {tbl} ORDER BY {order_by} LIMIT {limit}",
                limit = self.limit,
                tbl = self.tbl,
            ))
            .build()
            .run()
            .await
            .boxed()?
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .boxed()?;

        let schema = Arc::new(df.get_arrow_schema(self.tbl.as_str()).await.boxed()?);

        concat_batches(&schema, batches.iter()).boxed()
    }
}

fn sanitize_order_by(order_by_raw: &str) -> Result<String> {
    let raw_lower = order_by_raw.to_ascii_lowercase();
    if raw_lower.contains("with fill") || raw_lower.trim() == "all" {
        return Err(invalid_order_by(order_by_raw));
    }

    let order_by = strip_order_by_prefix(order_by_raw)?;
    let order_expr = parse_order_by_expression(order_by, order_by_raw)?;
    format_order_by_clause(&order_expr, order_by_raw)
}

fn strip_order_by_prefix(order_by_raw: &str) -> Result<&str> {
    let order_by_trimmed = order_by_raw.trim();
    let normalized_order_by = order_by_trimmed.to_ascii_lowercase();

    let order_by = if normalized_order_by.starts_with("order by") {
        order_by_trimmed
            .get(8..)
            .map_or(order_by_trimmed, str::trim_start)
    } else {
        order_by_trimmed
    };

    ensure!(
        !order_by.is_empty(),
        InvalidOrderBySnafu {
            order_by: order_by_raw.to_string()
        }
    );

    Ok(order_by)
}

fn parse_order_by_expression(order_by: &str, order_by_raw: &str) -> Result<OrderByExpr> {
    let sql = format!("SELECT * FROM tbl ORDER BY {order_by}");
    let mut statements = DFParser::parse_sql_with_dialect(sql.as_str(), &PostgreSqlDialect {})
        .context(UnableToParseOrderBySnafu {
            order_by: order_by_raw.to_string(),
        })?;

    if statements.len() != 1 {
        return Err(invalid_order_by(order_by_raw));
    }

    let Some(statement) = statements.pop_front() else {
        return Err(invalid_order_by(order_by_raw));
    };

    let Statement::Statement(statement) = statement else {
        return Err(invalid_order_by(order_by_raw));
    };

    let SQLStatement::Query(query) = statement.as_ref() else {
        return Err(invalid_order_by(order_by_raw));
    };

    let order_by_clause = extract_order_by_clause(query, order_by_raw)?;

    let OrderByKind::Expressions(exprs) = &order_by_clause.kind else {
        return Err(invalid_order_by(order_by_raw));
    };

    if exprs.len() != 1 {
        return Err(invalid_order_by(order_by_raw));
    }

    let Some(expr) = exprs.first() else {
        return Err(invalid_order_by(order_by_raw));
    };

    if expr.with_fill.is_some() || expr.options.nulls_first.is_some() {
        return Err(invalid_order_by(order_by_raw));
    }

    Ok(expr.clone())
}

fn extract_order_by_clause<'a>(query: &'a Query, order_by_raw: &str) -> Result<&'a OrderBy> {
    if query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(invalid_order_by(order_by_raw));
    }

    let Some(order_by_clause) = &query.order_by else {
        return Err(invalid_order_by(order_by_raw));
    };

    if order_by_clause.interpolate.is_some() {
        return Err(invalid_order_by(order_by_raw));
    }

    Ok(order_by_clause)
}

fn format_order_by_clause(order_expr: &OrderByExpr, order_by_raw: &str) -> Result<String> {
    let mut sanitized = match &order_expr.expr {
        SqlExpr::Identifier(ident) => {
            if ident.value.eq_ignore_ascii_case("all") {
                return Err(invalid_order_by(order_by_raw));
            }
            ident.to_string()
        }
        SqlExpr::CompoundIdentifier(idents) => idents_to_string(idents),
        _ => return Err(invalid_order_by(order_by_raw)),
    };

    match order_expr.options.asc {
        Some(true) => sanitized.push_str(" ASC"),
        Some(false) => sanitized.push_str(" DESC"),
        None => {}
    }

    Ok(sanitized)
}

fn invalid_order_by(order_by_raw: &str) -> Error {
    Error::InvalidOrderBy {
        order_by: order_by_raw.to_string(),
    }
}

fn idents_to_string(idents: &[Ident]) -> String {
    idents
        .iter()
        .map(Ident::to_string)
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::{Error, sanitize_order_by};

    #[test]
    fn parses_simple_identifier() {
        let order_by = sanitize_order_by("column").expect("order_by to parse");
        assert_eq!(order_by, "column");
    }

    #[test]
    fn parses_prefixed_order_by_keyword() {
        let order_by = sanitize_order_by("ORDER BY column").expect("order_by to parse");
        assert_eq!(order_by, "column");
    }

    #[test]
    fn parses_quoted_identifier() {
        let order_by =
            sanitize_order_by("\"quoted.column\"").expect("order_by with quotes to parse");
        assert_eq!(order_by, "\"quoted.column\"");
    }

    #[test]
    fn parses_compound_identifier_with_direction() {
        let order_by = sanitize_order_by("schema.table.column DESC").expect("order_by to parse");
        assert_eq!(order_by, "schema.table.column DESC");
    }

    #[test]
    fn parses_identifier_with_asc() {
        let order_by = sanitize_order_by("column asc").expect("order_by to parse");
        assert_eq!(order_by, "column ASC");
    }

    #[test]
    fn rejects_multiple_ordering_expressions() {
        assert!(matches!(
            sanitize_order_by("col1, col2"),
            Err(Error::InvalidOrderBy { .. })
        ));
    }

    #[test]
    fn rejects_non_column_expression() {
        assert!(matches!(
            sanitize_order_by("col1 + 1"),
            Err(Error::InvalidOrderBy { .. })
        ));
    }

    #[test]
    fn rejects_nulls_ordering() {
        assert!(matches!(
            sanitize_order_by("col1 NULLS LAST"),
            Err(Error::InvalidOrderBy { .. })
        ));
    }

    #[test]
    fn rejects_with_fill() {
        assert!(matches!(
            sanitize_order_by("col1 WITH FILL"),
            Err(Error::InvalidOrderBy { .. })
        ));

        assert!(matches!(
            sanitize_order_by("col1 with fill"),
            Err(Error::InvalidOrderBy { .. })
        ));
    }

    #[test]
    fn rejects_order_by_all() {
        assert!(matches!(
            sanitize_order_by("ALL"),
            Err(Error::InvalidOrderBy { .. })
        ));

        assert!(matches!(
            sanitize_order_by("all"),
            Err(Error::InvalidOrderBy { .. })
        ));
    }

    #[test]
    fn rejects_limit_injection() {
        assert!(matches!(
            sanitize_order_by("col1 DESC LIMIT 1"),
            Err(Error::InvalidOrderBy { .. })
        ));
    }

    #[test]
    fn rejects_statement_injection() {
        assert!(matches!(
            sanitize_order_by("col1 DESC; SELECT * FROM secret"),
            Err(Error::InvalidOrderBy { .. })
        ));
    }
}
