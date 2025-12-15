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

use std::sync::Arc;

use datafusion::{
    catalog::TableProvider,
    common::Column,
    datasource::DefaultTableSource,
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    sql::{
        TableReference,
        sqlparser::{
            ast::{Expr as SqlExpr, Value, ValueWithSpan},
            dialect::GenericDialect,
            parser::Parser,
            tokenizer::Token,
        },
    },
};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, SortExpr, col, ident, lit};
use itertools::Itertools;
use snafu::{ResultExt, Snafu};

use crate::{
    SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME, VectorSearchGenerationResult,
    aggregation::{self, AggregationResult, CandidateAggregation, Error as AggregationError},
    generation::{self, CandidateGeneration},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error occurred retrieving candidate search results: {source}"))]
    CandidateGenerationError { source: generation::Error },

    #[snafu(display("Error occurred aggregating candidate search results: {source}"))]
    CandidateAggregationError { source: aggregation::Error },

    #[snafu(display(
        "An unexpected error occurred preparing search request. Report an issue on GitHub: https://github.com/spiceai/spiceai/issues.\nDetails: {source}"
    ))]
    SearchRequestConstructionError { source: DataFusionError },

    #[snafu(display("An invalid keyword was specified: {keyword}"))]
    InvalidKeyword { keyword: String },
}

impl Error {
    #[must_use]
    pub fn is_user_error(&self) -> bool {
        matches!(self, Error::CandidateAggregationError { source } if source.is_user_error())
    }
}

/// [`QueryEngine`] defines the minimal interface needed to execute [`LogicalPlan`].
///
/// This allows extensibility beyond [`SessionContext::execute_logical_plan`] then [`DataFrame::execute_stream`].
#[async_trait::async_trait]
pub trait QueryEngine: Send + Sync {
    async fn run(&self, plan: LogicalPlan) -> Result<SendableRecordBatchStream, DataFusionError>;
}

pub struct SearchPipeline<A>
where
    A: CandidateAggregation,
{
    generators: Vec<Arc<dyn CandidateGeneration>>,
    aggregator: A,
    engine: Arc<dyn QueryEngine>,
}

impl<A: CandidateAggregation> SearchPipeline<A> {
    #[must_use]
    pub fn new(
        generators: Vec<Arc<dyn CandidateGeneration>>,
        aggregator: A,
        engine: Arc<dyn QueryEngine>,
    ) -> Self {
        SearchPipeline {
            generators,
            aggregator,
            engine,
        }
    }

    /// Runs the search pipeline with the provided parameters.
    #[expect(clippy::too_many_arguments)]
    pub async fn run(
        &self,
        query: String,
        tbl: &TableReference,
        opt_filter: Option<Expr>,
        addition_projection: Vec<Expr>,
        primary_keys: Vec<Column>,
        keywords: Vec<String>,
        limit: usize,
    ) -> std::result::Result<Option<AggregationResult>, Error> {
        let columns: Vec<_> = [
            primary_keys.iter().map(|c| col(c.clone())).collect(),
            addition_projection,
            vec![ident(SEARCH_SCORE_COLUMN_NAME)],
        ]
        .concat()
        .into_iter()
        .unique()
        .collect();

        let generation_results: Vec<VectorSearchGenerationResult> =
            futures::future::try_join_all(self.generators.iter().map(|g| async {
                let content_col = g.value_derived_from();

                // The column name for each `.generator` will be different, and therefore the
                // keyword filter [`Expr`] must be made differently.
                let mut filters = prepare_keywords(&keywords.clone(), &content_col)?;
                if let Some(ref f) = opt_filter {
                    filters.push(f.clone());
                }

                let mut columns = columns.clone();
                columns.push(ident(g.value_projection_name()).alias(SEARCH_VALUE_COLUMN_NAME));

                let lp = construct_logical_plan(
                    g.search(query.clone())
                        .context(SearchRequestConstructionSnafu)?,
                    tbl,
                    columns,
                    filters,
                    Some(limit),
                )
                .context(SearchRequestConstructionSnafu)?;

                let data =
                    self.engine
                        .run(lp)
                        .await
                        .map_err(|e| Error::CandidateGenerationError {
                            source: generation::Error::QueryError { source: e },
                        })?;

                Ok(VectorSearchGenerationResult {
                    data,
                    derived_from: content_col,
                })
            }))
            .await?;

        match self
            .aggregator
            .aggregate(generation_results, primary_keys, limit)
            .await
        {
            Ok(a) => Ok(Some(a)),
            Err(AggregationError::NoCandidatesGenerated) => Ok(None),
            Err(e) => Err(e).context(CandidateAggregationSnafu),
        }
    }
}

fn construct_logical_plan(
    tbl: Arc<dyn TableProvider>,
    name: &TableReference,
    columns: Vec<Expr>,
    filters: Vec<Expr>,
    limit: Option<usize>,
) -> Result<LogicalPlan, DataFusionError> {
    let mut scan =
        LogicalPlanBuilder::scan(name.clone(), Arc::new(DefaultTableSource::new(tbl)), None)?;

    if let Some(filter) = filters.into_iter().reduce(Expr::and) {
        scan = scan.filter(filter)?;
    }
    scan.project(columns)?
        .sort_with_limit(
            vec![SortExpr::new(ident(SEARCH_SCORE_COLUMN_NAME), false, false)],
            limit,
        )?
        .build()
}

/// Convert each keyword into an `ILIKE %keyword%` [`Expr`].
///
/// Also validates keywords against being SQL injections.
fn prepare_keywords(keywords: &[String], column: &str) -> Result<Vec<Expr>, Error> {
    keywords
        .iter()
        .map(|k| validate_keyword_to_ilike(k, column))
        .collect::<Result<Vec<Expr>, Error>>()
}

/// Ensure the provided keywords are valid string literal, useable as a keyword in an ILIKE expression (i.e. no SQL injection).
pub fn valid_keywords(keywords: &[String]) -> Result<Vec<String>, Error> {
    keywords
        .iter()
        .map(|k| {
            validate_keyword_to_ilike(k.as_str(), "target_column")?; // emulate the use of the keyword in the query.
            Ok(k.clone())
        })
        .collect::<Result<Vec<String>, _>>()
}

pub fn validate_keyword_to_ilike(k: &str, target_column: &str) -> Result<Expr, Error> {
    let expression = format!("{target_column} ILIKE '%{}%'", k.to_lowercase());
    let parser = Parser::new(&GenericDialect {});
    let mut parser = parser.try_with_sql(&expression).map_err(|err| {
        tracing::trace!("failed to parse 'keywords' for search. {err}");
        Error::InvalidKeyword {
            keyword: k.to_string(),
        }
    })?;

    // The keyword will exist on its own if nothing else is present.
    let ilike_expr = parser.parse_expr().map_err(|err| {
        tracing::trace!("failed to parse 'keywords' for search. {err}");
        Error::InvalidKeyword {
            keyword: k.to_string(),
        }
    })?;

    let SqlExpr::ILike { expr, pattern, .. } = &ilike_expr else {
        tracing::trace!(
            "failed to parse 'keywords' for search. expected ILIKE, but got {ilike_expr:?}"
        );
        return Err(Error::InvalidKeyword {
            keyword: k.to_string(),
        });
    };

    if let (
        SqlExpr::Identifier(id),
        SqlExpr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(v),
            ..
        }),
    ) = (*expr.clone(), *pattern.clone())
    {
        if id.value != target_column {
            tracing::trace!(
                "failed to parse 'keywords' for search. expected {target_column}, but got {}",
                id.value
            );
            return Err(Error::InvalidKeyword {
                keyword: k.to_string(),
            });
        }

        if v != format!("%{}%", k.to_lowercase()) {
            tracing::trace!(
                "failed to parse 'keywords' for search. expected '%{}%', but got {}",
                k.to_lowercase(),
                v
            );
            return Err(Error::InvalidKeyword {
                keyword: k.to_string(),
            });
        }
    } else {
        tracing::trace!(
            "failed to parse 'keywords' for search. expected identifiers, but got {expr:?} - {pattern:?}"
        );
        return Err(Error::InvalidKeyword {
            keyword: k.to_string(),
        });
    }

    // Ensure the expression is the last token.
    let next_token = parser.next_token();
    if next_token != Token::EOF {
        tracing::trace!(
            "failed to parse 'keywords' for search. expected EOF, but got {next_token:?}"
        );
        return Err(Error::InvalidKeyword {
            keyword: k.to_string(),
        });
    }

    Ok(ident(target_column).ilike(lit(format!("%{}%", k.to_lowercase()))))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn test_search_request_prepare_keywords() {
        let keywords = vec![
            "keyword1".to_string(),
            "\"key word2\"".to_string(),
            "key word3".to_string(),
            "keYwOrD4".to_string(),
        ];
        // Test all lowercase
        insta::assert_snapshot!(format!("{:?}", prepare_keywords(&keywords, "hello")), @r#"Ok([Like(Like { negated: false, expr: Column(Column { relation: None, name: "hello" }), pattern: Literal(Utf8("%keyword1%"), None), escape_char: None, case_insensitive: true }), Like(Like { negated: false, expr: Column(Column { relation: None, name: "hello" }), pattern: Literal(Utf8("%"key word2"%"), None), escape_char: None, case_insensitive: true }), Like(Like { negated: false, expr: Column(Column { relation: None, name: "hello" }), pattern: Literal(Utf8("%key word3%"), None), escape_char: None, case_insensitive: true }), Like(Like { negated: false, expr: Column(Column { relation: None, name: "hello" }), pattern: Literal(Utf8("%keyword4%"), None), escape_char: None, case_insensitive: true })])"#);

        // Test with casing
        insta::assert_snapshot!(format!("{:?}", prepare_keywords(&keywords, "hElLo")), @r#"Ok([Like(Like { negated: false, expr: Column(Column { relation: None, name: "hElLo" }), pattern: Literal(Utf8("%keyword1%"), None), escape_char: None, case_insensitive: true }), Like(Like { negated: false, expr: Column(Column { relation: None, name: "hElLo" }), pattern: Literal(Utf8("%"key word2"%"), None), escape_char: None, case_insensitive: true }), Like(Like { negated: false, expr: Column(Column { relation: None, name: "hElLo" }), pattern: Literal(Utf8("%key word3%"), None), escape_char: None, case_insensitive: true }), Like(Like { negated: false, expr: Column(Column { relation: None, name: "hElLo" }), pattern: Literal(Utf8("%keyword4%"), None), escape_char: None, case_insensitive: true })])"#);
    }

    #[test]
    fn test_search_request_parse_keywords() {
        let keywords = vec!["keyword1".to_string(), "keyword2".to_string()];
        let result = valid_keywords(&keywords);
        result.expect("should be valid search keywords");

        // Test keyword with a space
        let keywords = vec!["keyword 1".to_string()];
        let result = valid_keywords(&keywords);
        result.expect("should be valid search keywords");

        // Test empty keyword
        let keywords = vec![String::new()];
        let result = valid_keywords(&keywords);
        result.expect("should be valid search keywords");

        // Test escaping keyword
        let keywords = vec!["'); DROP TABLE testing;".to_string()];
        let result = valid_keywords(&keywords);
        result.expect_err("should be invalid search keywords");
    }
}
