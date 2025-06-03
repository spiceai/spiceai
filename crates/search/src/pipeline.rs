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
    logical_expr::sqlparser::ast::Expr,
    sql::sqlparser::{
        ast::{Value, ValueWithSpan},
        dialect::GenericDialect,
        parser::Parser,
        tokenizer::Token,
    },
};
use snafu::{ResultExt, Snafu};

use crate::{
    VectorSearchGenerationResult,
    aggregation::{self, AggregationResult, CandidateAggregation},
    generation::{self, CandidateGeneration},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error occurred retrieving candidate search results: {source}"))]
    CandidateGenerationError { source: generation::Error },

    #[snafu(display("Error occurred aggregating candidate search results: {source}"))]
    CandidateAggregationError { source: aggregation::Error },

    #[snafu(display("An invalid keyword was specified: {keyword}"))]
    InvalidKeyword { keyword: String },
}

impl Error {
    #[must_use]
    pub fn is_user_error(&self) -> bool {
        matches!(self, Error::CandidateAggregationError { source } if source.is_user_error())
    }
}

pub struct SearchPipeline<A>
where
    A: CandidateAggregation,
{
    generators: Vec<Arc<dyn CandidateGeneration>>,
    aggregator: A,
}

impl<A: CandidateAggregation> SearchPipeline<A> {
    #[must_use]
    pub fn new(generators: Vec<Arc<dyn CandidateGeneration>>, aggregator: A) -> Self {
        SearchPipeline {
            generators,
            aggregator,
        }
    }

    /// Runs the search pipeline with the provided parameters.
    pub async fn run(
        &self,
        query: String,
        opt_filters: Vec<Expr>,
        addition_projection: Vec<Expr>,
        primary_keys: Vec<String>,
        keywords: Vec<String>,
        limit: usize,
    ) -> std::result::Result<AggregationResult, Error> {
        let proj_ref: &[&Expr] = &addition_projection.iter().collect::<Vec<_>>();

        let generation_results: Vec<VectorSearchGenerationResult> =
            futures::future::try_join_all(self.generators.iter().map(|g| async {
                let content_col = g.value_derived_from();

                // The column name for each `.generator` will be different, and therefore the
                // keyword filter [`Expr`] must be made differently.
                let filters = [
                    prepare_keywords(&keywords.clone(), &content_col)?,
                    opt_filters.clone(),
                ]
                .concat();

                let data = g
                    .search(
                        query.clone(),
                        &filters.iter().collect::<Vec<_>>(),
                        proj_ref,
                        limit,
                    )
                    .await
                    .context(CandidateGenerationSnafu)?;

                // TODO: Filter results after the fact for filters that aren't supported by [`CandidateGeneration::supports_filter_pushdown`]. https://github.com/spiceai/spiceai/issues/5849

                // TODO: Retrieve columns from projection that aren't provided by candidate generator (see [`CandidateGeneration::supports_columns`]) https://github.com/spiceai/spiceai/issues/5850
                Ok(VectorSearchGenerationResult {
                    data,
                    derived_from: content_col,
                })
            }))
            .await?;

        self.aggregator
            .aggregate(generation_results, primary_keys, limit)
            .await
            .context(CandidateAggregationSnafu)
    }
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
        tracing::trace!("vector_search keyword parsing failed. {err}");
        Error::InvalidKeyword {
            keyword: k.to_string(),
        }
    })?;

    // The keyword will exist on its own if nothing else is present.
    let ilike_expr = parser.parse_expr().map_err(|err| {
        tracing::trace!("vector_search keyword parsing failed. {err}");
        Error::InvalidKeyword {
            keyword: k.to_string(),
        }
    })?;

    let Expr::ILike { expr, pattern, .. } = &ilike_expr else {
        tracing::trace!(
            "vector_search keyword parsing failed. expected ILIKE, but got {ilike_expr:?}"
        );
        return Err(Error::InvalidKeyword {
            keyword: k.to_string(),
        });
    };

    if let (
        Expr::Identifier(id),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(v),
            ..
        }),
    ) = (*expr.clone(), *pattern.clone())
    {
        if id.value.to_lowercase() != target_column {
            tracing::trace!(
                "vector_search keyword parsing failed. expected {target_column}, but got {}",
                id.value
            );
            return Err(Error::InvalidKeyword {
                keyword: k.to_string(),
            });
        }

        if v != format!("%{}%", k.to_lowercase()) {
            tracing::trace!(
                "vector_search keyword parsing failed. expected '%{}%', but got {}",
                k.to_lowercase(),
                v
            );
            return Err(Error::InvalidKeyword {
                keyword: k.to_string(),
            });
        }
    } else {
        tracing::trace!(
            "vector_search keyword parsing failed. expected identifiers, but got {expr:?} - {pattern:?}"
        );
        return Err(Error::InvalidKeyword {
            keyword: k.to_string(),
        });
    }

    // Ensure the expression is the last token.
    let next_token = parser.next_token();
    if next_token != Token::EOF {
        tracing::trace!(
            "vector_search keyword parsing failed. expected EOF, but got {next_token:?}"
        );
        return Err(Error::InvalidKeyword {
            keyword: k.to_string(),
        });
    }

    Ok(ilike_expr)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn test_search_request_parse_keywords() {
        let keywords = vec!["keyword1".to_string(), "keyword2".to_string()];
        let result = valid_keywords(&keywords);
        assert!(result.is_ok());

        // Test keyword with a space
        let keywords = vec!["keyword 1".to_string()];
        let result = valid_keywords(&keywords);
        assert!(result.is_ok());

        // Test empty keyword
        let keywords = vec![String::new()];
        let result = valid_keywords(&keywords);
        assert!(result.is_ok());

        // Test escaping keyword
        let keywords = vec!["'); DROP TABLE testing;".to_string()];
        let result = valid_keywords(&keywords);
        assert!(result.is_err());
    }
}
