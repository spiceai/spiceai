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
    common::TableReference,
    datasource::DefaultTableSource,
    error::DataFusionError,
    sql::sqlparser::{
        ast::{Expr as SqlExpr, Value, ValueWithSpan},
        dialect::GenericDialect,
        parser::Parser,
        tokenizer::Token,
    },
};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, SortExpr, col, ident, lit};
use itertools::Itertools;
use snafu::{ResultExt, Snafu};
use util::format_datafusion_error;

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
        "An unexpected error occurred preparing search request. Report an issue on GitHub: https://github.com/spiceai/spiceai/issues. Details: {}",
        format_datafusion_error(source)
    ))]
    SearchRequestConstructionError { source: DataFusionError },

    #[snafu(display("An invalid keyword was specified: {keyword}"))]
    InvalidKeyword { keyword: String },

    /// More search keywords than [`MAX_SEARCH_KEYWORDS`].
    ///
    /// Each keyword becomes one predicate. `DataFusion` re-folds a conjunction
    /// into a left-deep tree (`reduce(Expr::and)`) and then walks it
    /// recursively, so the keyword count — not only the tree this pipeline
    /// builds — has to stay under the depth that overflows a worker stack.
    #[snafu(display(
        "Search request rejected: {count} keywords were provided, and at most {max} are accepted. Shorten the `keywords` list."
    ))]
    TooManyKeywords { count: usize, max: usize },
}

impl Error {
    #[must_use]
    pub fn is_user_error(&self) -> bool {
        match self {
            Error::TooManyKeywords { .. } => true,
            Error::CandidateAggregationError { source } => source.is_user_error(),
            Error::CandidateGenerationError { .. }
            | Error::SearchRequestConstructionError { .. }
            | Error::InvalidKeyword { .. } => false,
        }
    }
}

/// Most keywords accepted on one search request.
///
/// A flat keyword list is folded into an `AND` tree. On a debug build with a
/// default ~2 MiB worker stack, a left-deep chain of these predicates still
/// plans at 750 keywords and aborts the process near 1000. `DataFusion`'s
/// optimizer splits that conjunction and rebuilds it with `reduce(Expr::and)`,
/// so a later pass sees a left-deep tree even when the pipeline balanced the
/// one it constructed. 512 is the same fixed cap used for SQL expression depth:
/// under the observed abort, above any realistic lexical pre-filter, and not a
/// configuration knob.
pub const MAX_SEARCH_KEYWORDS: usize = 512;

pub use runtime_query_engine::query_engine::QueryEngine;

pub struct SearchPipeline<A>
where
    A: CandidateAggregation,
{
    generators: Vec<Arc<dyn CandidateGeneration>>,
    aggregator: A,
    engine: Arc<dyn QueryEngine>,
}

fn candidate_pool_limit<A: CandidateAggregation>(
    aggregator: &A,
    generator_count: usize,
    limit: usize,
) -> usize {
    // A single generator is returned directly by RRF, so it must retain the
    // user-visible limit. Only multiple generators can benefit from a wider
    // pre-fusion candidate pool.
    if generator_count > 1 {
        aggregator.candidate_pool_size(limit)
    } else {
        limit
    }
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
        keywords: &[String],
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

        let candidate_limit = candidate_pool_limit(&self.aggregator, self.generators.len(), limit);

        let generation_results: Vec<VectorSearchGenerationResult> =
            futures::future::try_join_all(self.generators.iter().map(|g| async {
                let content_col = g.value_derived_from();

                // The column name for each `.generator` will be different, and therefore the
                // keyword filter [`Expr`] must be made differently.
                let mut filters = prepare_keywords(keywords, &content_col)?;
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
                    &primary_keys,
                    Some(candidate_limit),
                )
                .context(SearchRequestConstructionSnafu)?;

                let data = self.engine.execute_plan(lp).await.map_err(|e| {
                    Error::CandidateGenerationError {
                        source: generation::Error::InternalError {
                            source: Box::new(e),
                        },
                    }
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
    primary_keys: &[Column],
    limit: Option<usize>,
) -> Result<LogicalPlan, DataFusionError> {
    let mut scan =
        LogicalPlanBuilder::scan(name.clone(), Arc::new(DefaultTableSource::new(tbl)), None)?;

    // Balance the conjunction. `reduce(Expr::and)` is left-deep, with one stack
    // frame per keyword in every recursive planning pass. AND is associative, so
    // a balanced tree is the same predicate at depth log2(n). The keyword-count
    // cap is what keeps DataFusion's own later left-deep rebuild under the
    // stack limit; this fold keeps the tree this function hands over shallow.
    if let Some(filter) = conjoin_balanced(filters) {
        scan = scan.filter(filter)?;
    }
    let mut sort_exprs = vec![SortExpr::new(ident(SEARCH_SCORE_COLUMN_NAME), false, false)];
    sort_exprs.extend(
        primary_keys
            .iter()
            .map(|pk| SortExpr::new(col(pk.clone()), true, true)),
    );
    scan.project(columns)?
        .sort_with_limit(sort_exprs, limit)?
        .build()
}

/// Fold `exprs` with logical AND into a balanced binary tree.
///
/// Returns `None` when `exprs` is empty. A single expression is returned
/// unchanged. AND is associative, so the result matches a left fold.
fn conjoin_balanced(mut exprs: Vec<Expr>) -> Option<Expr> {
    if exprs.is_empty() {
        return None;
    }
    while exprs.len() > 1 {
        let mut paired = Vec::with_capacity(exprs.len().div_ceil(2));
        let mut rest = exprs.into_iter();
        while let Some(left) = rest.next() {
            if let Some(right) = rest.next() {
                paired.push(left.and(right));
            } else {
                paired.push(left);
            }
        }
        exprs = paired;
    }
    exprs.pop()
}

fn ensure_keyword_count(count: usize) -> Result<(), Error> {
    if count > MAX_SEARCH_KEYWORDS {
        return Err(Error::TooManyKeywords {
            count,
            max: MAX_SEARCH_KEYWORDS,
        });
    }
    Ok(())
}

/// Convert each keyword into an `ILIKE %keyword%` [`Expr`].
///
/// Also validates keywords against being SQL injections.
fn prepare_keywords(keywords: &[String], column: &str) -> Result<Vec<Expr>, Error> {
    ensure_keyword_count(keywords.len())?;
    keywords
        .iter()
        .map(|k| validate_keyword_to_ilike(k, column))
        .collect::<Result<Vec<Expr>, Error>>()
}

/// Ensure the provided keywords are valid string literal, useable as a keyword in an ILIKE expression (i.e. no SQL injection).
pub fn valid_keywords(keywords: &[String]) -> Result<Vec<String>, Error> {
    ensure_keyword_count(keywords.len())?;
    keywords
        .iter()
        .map(|k| {
            validate_keyword_to_ilike(k.as_str(), "target_column")?; // emulate the use of the keyword in the query.
            Ok(k.clone())
        })
        .collect::<Result<Vec<String>, _>>()
}

pub fn validate_keyword_to_ilike(k: &str, target_column: &str) -> Result<Expr, Error> {
    let lower = k.to_lowercase();
    let pattern = format!("%{lower}%");
    let expression = format!("{target_column} ILIKE '{pattern}'");
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

    let SqlExpr::ILike {
        expr,
        pattern: parsed_pattern,
        ..
    } = &ilike_expr
    else {
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
    ) = (expr.as_ref(), parsed_pattern.as_ref())
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

        if v != &pattern {
            tracing::trace!(
                "failed to parse 'keywords' for search. expected '{pattern}', but got {v}"
            );
            return Err(Error::InvalidKeyword {
                keyword: k.to_string(),
            });
        }
    } else {
        tracing::trace!(
            "failed to parse 'keywords' for search. expected identifiers, but got {expr:?} - {parsed_pattern:?}"
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

    Ok(ident(target_column).ilike(lit(pattern)))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::aggregation::reciprocal_rank::ReciprocalRankFusion;

    /// Regression test for #12242: the HTTP pipeline gives every RRF leg a
    /// wider candidate pool, but does not widen a direct single-leg response.
    #[test]
    fn rrf_candidate_limit_is_widened_only_for_fusion() {
        assert_eq!(candidate_pool_limit(&ReciprocalRankFusion, 2, 10), 40);
        assert_eq!(candidate_pool_limit(&ReciprocalRankFusion, 1, 10), 10);
    }

    #[test]
    fn test_search_request_prepare_keywords() {
        let keywords = vec![
            "keyword1".to_string(),
            "\"key word2\"".to_string(),
            "key word3".to_string(),
            "keYwOrD4".to_string(),
        ];
        // Test all lowercase
        insta::assert_snapshot!(
            "prepare_keywords_lowercase_column",
            format!("{:?}", prepare_keywords(&keywords, "hello"))
        );

        // Test with casing
        insta::assert_snapshot!(
            "prepare_keywords_mixed_case_column",
            format!("{:?}", prepare_keywords(&keywords, "hElLo"))
        );
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

    fn keyword_list(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("k{i}")).collect()
    }

    /// AND-nesting along the longest path, measured with an explicit stack.
    fn and_depth(root: &Expr) -> usize {
        let mut stack = vec![(root, 0usize)];
        let mut max_depth = 0usize;
        while let Some((expr, depth)) = stack.pop() {
            if let Expr::BinaryExpr(binary) = expr
                && binary.op == datafusion_expr::Operator::And
            {
                let next = depth + 1;
                max_depth = max_depth.max(next);
                stack.push((binary.left.as_ref(), next));
                stack.push((binary.right.as_ref(), next));
            }
        }
        max_depth
    }

    fn conjunction_leaves(root: &Expr) -> usize {
        let mut stack = vec![root];
        let mut leaves = 0usize;
        while let Some(expr) = stack.pop() {
            if let Expr::BinaryExpr(binary) = expr
                && binary.op == datafusion_expr::Operator::And
            {
                stack.push(binary.left.as_ref());
                stack.push(binary.right.as_ref());
            } else {
                leaves += 1;
            }
        }
        leaves
    }

    #[test]
    fn rejects_too_many_keywords() {
        let over = keyword_list(MAX_SEARCH_KEYWORDS + 1);
        let err = valid_keywords(&over).expect_err("a keyword list over the cap is rejected");
        let message = err.to_string();
        assert!(
            message.contains(&MAX_SEARCH_KEYWORDS.to_string())
                && message.contains(&(MAX_SEARCH_KEYWORDS + 1).to_string())
                && message.contains("keywords"),
            "unexpected error: {message}"
        );
        let _ =
            prepare_keywords(&over, "body").expect_err("prepare_keywords enforces the same cap");
        assert!(err.is_user_error(), "too many keywords is a client error");
    }

    #[test]
    fn allows_keyword_count_at_the_cap() {
        let keywords = keyword_list(MAX_SEARCH_KEYWORDS);
        valid_keywords(&keywords).expect("a keyword list at the cap is accepted");
    }

    /// Four keyword predicates folded left-deep have AND-depth 3. The balanced
    /// fold has depth 2, and a list at the cap stays logarithmic with every
    /// keyword still present as a leaf.
    #[test]
    fn keyword_conjunction_is_balanced() {
        let four = prepare_keywords(&keyword_list(4), "body").expect("four keywords parse");
        let four = conjoin_balanced(four).expect("four keywords conjoin");
        assert_eq!(and_depth(&four), 2);
        assert_eq!(conjunction_leaves(&four), 4);

        let capped = prepare_keywords(&keyword_list(MAX_SEARCH_KEYWORDS), "body")
            .expect("keywords at the cap parse");
        let capped = conjoin_balanced(capped).expect("keywords at the cap conjoin");
        assert_eq!(conjunction_leaves(&capped), MAX_SEARCH_KEYWORDS);
        assert_eq!(and_depth(&capped), 9);
    }

    /// Optimizing and physically planning the deepest accepted keyword filter
    /// must return. A left-deep chain of this width is what overflows the
    /// worker stack inside `DataFusion`.
    #[tokio::test]
    async fn keyword_filter_at_cap_plans() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![Some("hello")]))],
        )
        .expect("one-row batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");
        let filters = prepare_keywords(&keyword_list(MAX_SEARCH_KEYWORDS), "body")
            .expect("keywords at the cap");
        let filter = conjoin_balanced(filters).expect("conjunction");
        let plan = LogicalPlanBuilder::scan(
            TableReference::bare("docs"),
            Arc::new(DefaultTableSource::new(Arc::new(table))),
            None,
        )
        .expect("scan")
        .filter(filter)
        .expect("filter")
        .build()
        .expect("logical plan");

        let ctx = SessionContext::new();
        let optimized = ctx
            .state()
            .optimize(&plan)
            .expect("optimizing the keyword filter");
        ctx.state()
            .create_physical_plan(&optimized)
            .await
            .expect("physical plan of the keyword filter");
    }
}
