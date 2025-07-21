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
use cache::key::SearchKey;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::{
    Expr, SelectItem, TableFactor, TableWithJoins, Value, ValueWithSpan,
};
use datafusion::sql::sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use itertools::Itertools;
use schemars::JsonSchema;
use search::pipeline::valid_keywords;
use serde::{Deserialize, Serialize};

use super::{Error, Result};

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub struct SearchRequestBaseJson {
    /// The text to search documents for similarity
    pub text: String,

    /// The datasets to search for similarity. If None, search across all datasets. For available datasets, use the `list_datasets` tool and ensure `can_search_documents==true`.
    #[serde(default)]
    pub datasets: Option<Vec<String>>,

    /// Number of documents to return for each dataset
    #[serde(default)]
    pub limit: Option<usize>,

    /// An SQL filter predicate to apply. Format: 'WHERE `where_cond`'.
    #[serde(rename = "where", default)]
    pub where_cond: Option<String>,

    /// Additional columns to return from the dataset. If the column is a primary key, it will be
    /// returned within the response under `.primary_key`, not `.data`.
    #[serde(default)]
    pub additional_columns: Vec<String>,
}

/// HTTP request schema is separate from AI requests, so that keywords can be supplied as an optional field for HTTP calls.
/// `schemars` doesn't allow setting `#[serde(default)]` as well as `#[schemars(required)]` - the field does not become required.
/// When the field is not required, the model ignores it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub struct SearchRequestHTTPJson {
    #[serde(flatten)]
    pub base: SearchRequestBaseJson,

    // A list of optional keywords, to pre-filter on the embedding column in SQL before performing the vector search.
    #[serde(default)]
    pub keywords: Option<Vec<String>>,
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct SearchRequestAIJson {
    #[serde(flatten)]
    pub base: SearchRequestBaseJson,

    /// At least one keyword should be supplied for a vector search. Keywords should be individual words.
    /// Keywords are used to pre-filter the embedding column, applied as a `WHERE col LIKE '%keyword%'` condition.
    /// Keywords should not contain column names, special characters, or other operators.
    pub keywords: Vec<String>,
}

impl From<SearchRequestHTTPJson> for SearchRequestAIJson {
    fn from(req: SearchRequestHTTPJson) -> Self {
        SearchRequestAIJson {
            base: req.base,
            keywords: req.keywords.unwrap_or_default(),
        }
    }
}

impl TryFrom<SearchRequestAIJson> for SearchRequest {
    type Error = String;

    fn try_from(req: SearchRequestAIJson) -> Result<Self, Self::Error> {
        Ok(SearchRequest::new(
            req.base.text,
            req.base.datasets,
            req.base.limit.unwrap_or(default_limit()),
            req.base
                .where_cond
                .map(|r| SearchRequest::parse_where_cond(r).map_err(|e| e.to_string()))
                .transpose()?,
            SearchRequest::parse_additional_columns(&req.base.additional_columns)
                .map_err(|e| e.to_string())?,
            valid_keywords(&req.keywords).map_err(|e| e.to_string())?,
        ))
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::doc_markdown)]
pub struct SearchRequest {
    /// The text to search documents for similarity
    pub text: String,

    /// The datasets to search for similarity. If None, search across all datasets. For available datasets, use the 'list_datasets' tool and ensure `can_search_documents==true`.
    pub datasets: Option<Vec<String>>,

    /// Number of documents to return for each dataset
    pub limit: usize,

    /// An SQL filter predicate to apply. Format: 'WHERE <where_cond>'.
    pub where_cond: Option<Expr>,

    /// Additional columns to return from the dataset.
    pub additional_columns: Vec<String>,

    /// Keywords to perform a lexical search and pre-filter the embedding column.
    pub keywords: Vec<String>,
}

impl From<SearchRequest> for SearchKey {
    fn from(req: SearchRequest) -> Self {
        SearchKey::new(
            req.text.into(),
            req.datasets
                .map(|d| d.into_iter().map(Into::into).collect()),
            req.limit,
            req.where_cond,
            Some(req.additional_columns.into_iter().map(Into::into).collect()),
            req.keywords.into_iter().map(Into::into).collect(),
        )
    }
}

#[must_use]
fn default_limit() -> usize {
    3
}

impl SearchRequest {
    /// Create new [`SearchRequest`].
    ///
    /// [`where_cond`] should already be sanitized. For raw WHERE conditions,
    /// use [`TryFrom<SearchRequestJson>`].
    #[must_use]
    pub fn new(
        text: String,
        datasets: Option<Vec<String>>,
        limit: usize,
        where_cond: Option<Expr>,
        additional_columns: Vec<String>,
        keywords: Vec<String>,
    ) -> Self {
        SearchRequest {
            text,
            datasets,
            limit,
            where_cond,
            additional_columns,
            keywords,
        }
    }

    pub fn parse_where_cond(where_cond: String) -> Result<Expr> {
        let parser = Parser::new(&PostgreSqlDialect {});
        let mut parser =
            parser
                .try_with_sql(&where_cond)
                .map_err(|_| Error::InvalidWhereCondition {
                    where_cond: where_cond.clone(),
                })?;

        // Parse the WHERE keyword if its there, otherwise ignore it.
        let _ = parser.parse_keyword(Keyword::WHERE);

        // Parse the expression after the WHERE keyword.
        let expr = parser
            .parse_expr()
            .map_err(|_| Error::InvalidWhereCondition {
                where_cond: where_cond.clone(),
            })?;

        // Ensure the WHERE clause is the last token.
        let next_token = parser.next_token();
        if next_token != Token::EOF {
            return Err(Error::InvalidWhereCondition { where_cond });
        }

        Ok(expr)
    }

    pub fn parse_additional_columns(additional_columns: &[String]) -> super::Result<Vec<String>> {
        additional_columns
            .iter()
            .map(|c| {
                let select_statement = format!("SELECT {c} FROM testing");
                let parser = Parser::new(&GenericDialect);
                let mut parser = parser.try_with_sql(&select_statement).map_err(|err| {
                    tracing::trace!("vector_search additional column parsing failed. {err}");
                    Error::InvalidAdditionalColumns {
                        additional_column: c.clone(),
                    }
                })?;

                // parse the SELECT
                let expr = parser.parse_select().map_err(|err| {
                    tracing::trace!("vector_search additional column parsing failed. {err}");
                    Error::InvalidAdditionalColumns{
                        additional_column: c.clone(),
                    }
                })?;

                if expr.projection.len() > 1 || expr.from.len() > 1 {
                    tracing::trace!("vector_search additional column parsing failed. expected 1 projection and 1 table, but got {expr:?}");
                    return Err(Error::InvalidAdditionalColumns {
                        additional_column: c.clone(),
                    });
                }


                let Some(SelectItem::UnnamedExpr(select_expr)) = expr.projection.first() else {
                    tracing::trace!("vector_search additional column parsing failed. expected an identifier, but got {expr:?}");
                    return Err(Error::InvalidAdditionalColumns {
                        additional_column: c.clone(),
                    });
                };

                // Find the projected column. Must handle single and compound identifiers separately.
                let proj_value = match select_expr {
                    Expr::Identifier(sqlparser::ast::Ident {
                        value,
                        ..
                    }) => value,
                    Expr::CompoundIdentifier(idents) => &idents.iter().map(|i| i.value.clone()).join("."),
                    _ => {
                        tracing::trace!("vector_search additional column parsing failed. expected an identifier, but got {expr:?}");
                        return Err(Error::InvalidAdditionalColumns {
                            additional_column: c.clone(),
                        });
                    }
                };

                // Check equality whilst ignoring quotation.
                if proj_value != c.trim_matches('"') {
                    tracing::trace!("vector_search additional column parsing failed. expected {c}, but got {proj_value}");
                    return Err(Error::InvalidAdditionalColumns {
                        additional_column: c.clone(),
                    });
                }

                let Some(TableWithJoins { relation, .. }) = expr.from.first() else {
                    tracing::trace!("vector_search additional column parsing failed. expected a table, but got {expr:?}");
                    return Err(super::Error::InvalidAdditionalColumns{
                        additional_column: c.clone(),
                    });
                };

                let TableFactor::Table { name, .. } = relation else {
                    tracing::trace!("vector_search additional column parsing failed. expected a table, but got {relation:?}");
                    return Err(super::Error::InvalidAdditionalColumns{
                        additional_column: c.clone(),
                    });
                };

                if name.to_string() != "testing" {
                    tracing::trace!("vector_search additional column parsing failed. expected 'testing', but got {name}");
                    return Err(super::Error::InvalidAdditionalColumns{
                        additional_column: c.clone(),
                    });
                }

                let next_token = parser.next_token();
                if next_token != Token::EOF {
                    tracing::trace!("vector_search additional column parsing failed. expected EOF, but got {next_token:?}");
                    return Err(super::Error::InvalidAdditionalColumns{
                        additional_column: c.clone(),
                    });
                }

                Ok(c.clone())
            })
            .collect::<Result<Vec<String>>>()
    }

    pub fn validate_keyword_to_ilike(k: &str, target_column: &str) -> Result<Expr> {
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
                    "vector_search keyword parsing failed. expected 'target_column', but got {}",
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
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr};
    use schemars::schema_for;
    use snafu::ResultExt;

    #[tokio::test]
    async fn test_search_request_schema() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        serde_json::to_value(schema_for!(SearchRequestAIJson)).boxed()?;
        Ok(())
    }

    #[test]
    fn test_parse_additional_columns_basic() {
        let mut resp = match SearchRequest::parse_additional_columns(&["column".to_string()]) {
            Ok(r) => r,
            Err(e) => panic!("failed to parse additional columns: {e}"),
        };
        assert_eq!(resp.pop(), Some("column".to_string()));
    }

    #[test]
    fn test_parse_additional_columns_quoted() {
        let mut resp =
            match SearchRequest::parse_additional_columns(&["\"quoted_column\"".to_string()]) {
                Ok(r) => r,
                Err(e) => panic!("failed to parse additional columns: {e}"),
            };
        assert_eq!(resp.pop(), Some("\"quoted_column\"".to_string()));
    }

    #[test]
    fn test_parse_additional_columns_qualified() {
        let mut resp =
            match SearchRequest::parse_additional_columns(&["qualified.column".to_string()]) {
                Ok(r) => r,
                Err(e) => panic!("failed to parse additional columns: {e}"),
            };

        assert_eq!(resp.pop(), Some("qualified.column".to_string()));
    }

    #[test]
    fn test_parse_additional_columns_quoted_qualified() {
        let mut resp = match SearchRequest::parse_additional_columns(&[
            "\"qualified.quoted_column\"".to_string(),
        ]) {
            Ok(r) => r,
            Err(e) => panic!("failed to parse additional columns: {e}"),
        };

        assert_eq!(resp.pop(), Some("\"qualified.quoted_column\"".to_string()));
    }

    #[test]
    fn test_valid_where_conditions() {
        // Test basic comparison
        match SearchRequest::parse_where_cond("column = 'value'".to_string()) {
            Ok(r) => assert_eq!(r.to_string(), "column = 'value'"),
            Err(e) => panic!("{}", e),
        }

        // Test with WHERE keyword
        let result = SearchRequest::parse_where_cond("WHERE column = 'value'".to_string());
        assert!(result.is_ok());

        // Test numeric comparison
        let result = SearchRequest::parse_where_cond("age > 18".to_string());
        assert!(result.is_ok());

        // Test boolean condition
        let result = SearchRequest::parse_where_cond("is_active = true".to_string());
        assert!(result.is_ok());

        // Test AND condition
        let result = SearchRequest::parse_where_cond("age > 18 AND is_active = true".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_malformed_conditions() {
        // Test semicolon injection
        let result =
            SearchRequest::parse_where_cond("column = 'value'; DROP TABLE users;".to_string());
        assert!(result.is_err(), "{}", result.expect("!"));

        // Test UNION injection
        let result = SearchRequest::parse_where_cond(
            "column = 'value' UNION SELECT * FROM users".to_string(),
        );
        assert!(result.is_err());

        // Test multiple statements
        let result =
            SearchRequest::parse_where_cond("column = 'value'; SELECT * FROM users".to_string());
        assert!(result.is_err());

        // Test stacked queries
        let result = SearchRequest::parse_where_cond(
            "column = 'value'); SELECT * FROM users; --".to_string(),
        );
        assert!(result.is_err());

        // Test incomplete expression
        let result = SearchRequest::parse_where_cond("column =".to_string());
        assert!(result.is_err());

        // Test invalid operator
        let result = SearchRequest::parse_where_cond("column === value".to_string());
        assert!(result.is_err());

        // Test unclosed string
        let result = SearchRequest::parse_where_cond("column = 'value".to_string());
        assert!(result.is_err());

        // Test invalid column name
        let result = SearchRequest::parse_where_cond("'column' = 'value'".to_string());
        assert!(result.is_ok()); // Note: This is actually valid SQL syntax

        // Test empty condition
        let result = SearchRequest::parse_where_cond(String::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_valid_conditions() {
        // Test nested AND/OR
        let result = SearchRequest::parse_where_cond(
            "age > 18 AND (is_active = true OR role = 'admin')".to_string(),
        );
        assert!(result.is_ok());

        // Test IN clause
        let result = SearchRequest::parse_where_cond("status IN ('active', 'pending')".to_string());
        assert!(result.is_ok());

        // Test BETWEEN
        let result = SearchRequest::parse_where_cond("age BETWEEN 18 AND 65".to_string());
        assert!(result.is_ok());

        // Test IS NULL
        let result = SearchRequest::parse_where_cond("last_login IS NULL".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_expression_structure() {
        // Test basic equality expression structure
        let result = SearchRequest::parse_where_cond("column = 'value'".to_string())
            .expect("Should parse successfully");

        if let Expr::BinaryOp {
            left: _,
            op,
            right: _,
        } = result
        {
            assert_eq!(op, BinaryOperator::Eq);
        } else {
            panic!("Expected BinaryOp expression");
        }

        // Test AND expression structure
        let result = SearchRequest::parse_where_cond("col1 = 'val1' AND col2 = 'val2'".to_string())
            .expect("Should parse successfully");

        if let Expr::BinaryOp {
            left: _,
            op,
            right: _,
        } = result
        {
            assert_eq!(op, BinaryOperator::And);
        } else {
            panic!("Expected BinaryOp expression");
        }
    }
}
