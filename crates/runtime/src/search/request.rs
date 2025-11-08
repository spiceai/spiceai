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
use datafusion::common::Column;
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{Expr, SelectItem, TableFactor, TableWithJoins};
use datafusion::sql::sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
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
    pub additional_columns: Vec<Column>,

    /// Keywords to perform a lexical search and pre-filter the embedding column.
    pub keywords: Vec<String>,
}

impl TryFrom<SearchRequestHTTPJson> for SearchRequest {
    type Error = String;

    fn try_from(req: SearchRequestHTTPJson) -> Result<Self, Self::Error> {
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
            valid_keywords(&req.keywords.unwrap_or_default()).map_err(|e| e.to_string())?,
        ))
    }
}

impl TryFrom<SearchRequestBaseJson> for SearchRequest {
    type Error = String;

    fn try_from(req: SearchRequestBaseJson) -> Result<Self, Self::Error> {
        Ok(SearchRequest::new(
            req.text,
            req.datasets,
            req.limit.unwrap_or(default_limit()),
            req.where_cond
                .map(|r| SearchRequest::parse_where_cond(r).map_err(|e| e.to_string()))
                .transpose()?,
            SearchRequest::parse_additional_columns(&req.additional_columns)
                .map_err(|e| e.to_string())?,
            Vec::new(),
        ))
    }
}

impl From<SearchRequest> for SearchKey {
    fn from(req: SearchRequest) -> Self {
        SearchKey::new(
            req.text.into(),
            req.datasets
                .map(|d| d.into_iter().map(Into::into).collect()),
            req.limit,
            req.where_cond,
            Some(
                req.additional_columns
                    .into_iter()
                    .map(|c| c.to_string().as_str().into())
                    .collect(),
            ),
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
        additional_columns: Vec<Column>,
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

    pub fn parse_additional_columns(additional_columns: &[String]) -> super::Result<Vec<Column>> {
        additional_columns
            .iter()
            .map(|c| {
                let select_statement = format!("SELECT {c} FROM testing");
                let parser = Parser::new(&GenericDialect);
                let mut parser = parser.try_with_sql(&select_statement).map_err(|err| {
                    tracing::trace!("parsing 'additional_columns' for search failed. {err}");
                    Error::InvalidAdditionalColumns {
                        additional_column: c.clone(),
                    }
                })?;

                // parse the SELECT
                let expr = parser.parse_select().map_err(|err| {
                    tracing::trace!("parsing 'additional_columns' for search failed. {err}");
                    Error::InvalidAdditionalColumns{
                        additional_column: c.clone(),
                    }
                })?;

                if expr.projection.len() > 1 || expr.from.len() > 1 {
                    tracing::trace!("parsing 'additional_columns' for search failed. Expected 1 projection and 1 table, but got {expr:?}");
                    return Err(Error::InvalidAdditionalColumns {
                        additional_column: c.clone(),
                    });
                }

                let Some(SelectItem::UnnamedExpr(select_expr)) = expr.projection.first() else {
                    tracing::trace!("parsing 'additional_columns' for search failed. Expected an identifier, but got {expr:?}");
                    return Err(Error::InvalidAdditionalColumns {
                        additional_column: c.clone(),
                    });
                };

                // Find the projected column. Must handle single and compound identifiers separately.
                let idents = match select_expr {
                    Expr::Identifier(ident) => {
                        vec![ident.clone()]
                    }
                    Expr::CompoundIdentifier(idents) => {
                        idents.clone()
                    }
                    _ => {
                        tracing::trace!("parsing 'additional_columns' for search failed. Expected an identifier, but got {expr:?}");
                        return Err(Error::InvalidAdditionalColumns {
                            additional_column: c.clone(),
                        });
                    }
                };

                let Some(TableWithJoins { relation, .. }) = expr.from.first() else {
                    tracing::trace!("parsing 'additional_columns' for search failed. Expected a table, but got {expr:?}");
                    return Err(super::Error::InvalidAdditionalColumns{
                        additional_column: c.clone(),
                    });
                };

                let TableFactor::Table { name, .. } = relation else {
                    tracing::trace!("parsing 'additional_columns' for search failed. Expected a table, but got {relation:?}");
                    return Err(super::Error::InvalidAdditionalColumns{
                        additional_column: c.clone(),
                    });
                };

                if name.to_string() != "testing" {
                    tracing::trace!("parsing 'additional_columns' for search failed. Expected 'testing', but got {name}");
                    return Err(super::Error::InvalidAdditionalColumns{
                        additional_column: c.clone(),
                    });
                }

                let next_token = parser.next_token();
                if next_token != Token::EOF {
                    tracing::trace!("parsing 'additional_columns' for search failed. Expected EOF, but got {next_token:?}");
                    return Err(super::Error::InvalidAdditionalColumns{
                        additional_column: c.clone(),
                    });
                }

                // `..._ignore_case` actually means preserve case.
                let col = Column::from_qualified_name_ignore_case(c);

                // Check equality whilst ignoring quotation.
                let mut parts = col.relation.as_ref().map(TableReference::to_vec).unwrap_or_default();
                parts.push(col.name.clone());
                let from_ident: Vec<_> = idents.iter().map(|i| i.value.clone()).collect();
                if parts != from_ident {
                    tracing::trace!("parsing 'additional_columns' for search failed. Expected final column {parts:?} to be like parsed AST {from_ident:?}");
                    return Err(Error::InvalidAdditionalColumns {
                        additional_column: c.clone(),
                    });
                }

                Ok(col)
            })
            .collect::<Result<Vec<Column>>>()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr};

    fn run_parse_additional_columns(input: &[&str]) -> String {
        let input: Vec<_> = input.iter().map(|s| (*s).to_string()).collect();
        let resp = SearchRequest::parse_additional_columns(&input)
            .expect("failed to parse additional columns");
        format!("{resp:?}")
    }

    #[test]
    fn test_parse_additional_columns_good() {
        insta::assert_snapshot!(
            run_parse_additional_columns(&["column"]).as_str(),
            @r#"[Column { relation: None, name: "column" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["\"quoted_column\""]).as_str(),
            @r#"[Column { relation: None, name: "quoted_column" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["tbl.column"]).as_str(),
            @r#"[Column { relation: Some(Bare { table: "tbl" }), name: "column" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["schema.tbl.column"]).as_str(),
            @r#"[Column { relation: Some(Partial { schema: "schema", table: "tbl" }), name: "column" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["catalog.schema.tbl.column"]).as_str(),
            @r#"[Column { relation: Some(Full { catalog: "catalog", schema: "schema", table: "tbl" }), name: "column" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["catalog.schema.tbl.\"quoted_column\""]).as_str(),
            @r#"[Column { relation: Some(Full { catalog: "catalog", schema: "schema", table: "tbl" }), name: "quoted_column" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["catalog.schema.tbl.\"quoted.with_dot\""]).as_str(),
            @r#"[Column { relation: Some(Full { catalog: "catalog", schema: "schema", table: "tbl" }), name: "quoted.with_dot" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["schema.tbl.\"quoted.with_dot\""]).as_str(),
            @r#"[Column { relation: Some(Partial { schema: "schema", table: "tbl" }), name: "quoted.with_dot" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["tbl.\"quoted.with_dot\""]).as_str(),
            @r#"[Column { relation: Some(Bare { table: "tbl" }), name: "quoted.with_dot" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["\"quoted.with_dot\""]).as_str(),
            @r#"[Column { relation: None, name: "quoted.with_dot" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["table.\"column with spaces\""]).as_str(),
            @r#"[Column { relation: Some(Bare { table: "table" }), name: "column with spaces" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["schema.\"table with spaces\".column"]).as_str(),
            @r#"[Column { relation: Some(Partial { schema: "schema", table: "table with spaces" }), name: "column" }]"#
        );
    }

    #[test]
    fn test_parse_additional_columns_casing() {
        insta::assert_snapshot!(
            run_parse_additional_columns(&["CoLuMn"]).as_str(),
            @r#"[Column { relation: None, name: "CoLuMn" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["CaTaLoG.sChEmA.tBl.\"QuOtEd.WiTh_DoT\""]).as_str(),
            @r#"[Column { relation: Some(Full { catalog: "CaTaLoG", schema: "sChEmA", table: "tBl" }), name: "QuOtEd.WiTh_DoT" }]"#
        );
        insta::assert_snapshot!(
            run_parse_additional_columns(&["CaTaLoG.sChEmA.tBl.CoLuMn"]).as_str(),
            @r#"[Column { relation: Some(Full { catalog: "CaTaLoG", schema: "sChEmA", table: "tBl" }), name: "CoLuMn" }]"#
        );
    }

    #[test]
    fn test_parse_additional_columns_bad() {
        for bad in [
            vec!["COUNT(*)".to_string()],
            vec!["col1, col2".to_string()],
            // SQL injection attempts
            vec!["column; DROP TABLE users".to_string()],
            vec!["column' OR '1'='1".to_string()],
            vec!["column--".to_string()],
            vec!["column); DROP TABLE users; --".to_string()],
            // Function calls and expressions
            vec!["SUM(column)".to_string()],
            vec!["AVG(column)".to_string()],
            vec!["MAX(column)".to_string()],
            vec!["column + 1".to_string()],
            vec!["column * 2".to_string()],
            vec!["column = 'value'".to_string()],
            vec!["CASE WHEN column THEN 1 END".to_string()],
            vec!["CAST(column AS INT)".to_string()],
            vec!["column::integer".to_string()],
            // Subqueries
            vec!["(SELECT * FROM table)".to_string()],
            vec!["column IN (SELECT id FROM table)".to_string()],
            // Multiple columns/comma-separated
            vec!["col1, col2, col3".to_string()],
            vec!["table.col1, table.col2".to_string()],
            // Wildcards
            vec!["*".to_string()],
            vec!["table.*".to_string()],
            vec!["schema.table.*".to_string()],
            // Invalid quoting
            vec!["'column'".to_string()],
            vec!["\"unclosed".to_string()],
            vec!["unclosed\"".to_string()],
            vec!["column\"".to_string()],
            vec!["\"column".to_string()],
            // Special characters and operators
            vec!["column > 10".to_string()],
            vec!["column AND other".to_string()],
            vec!["column OR other".to_string()],
            vec!["NOT column".to_string()],
            vec!["column IS NULL".to_string()],
            vec!["column LIKE '%test%'".to_string()],
            vec!["column BETWEEN 1 AND 10".to_string()],
            // Empty or whitespace
            vec![String::new()],
            vec![" ".to_string()],
            vec!["  \t\n  ".to_string()],
            // Too many parts
            vec!["a.b.c.d.e".to_string()],
            vec!["catalog.schema.table.column.extra".to_string()],
            // Parentheses without functions
            vec!["(column)".to_string()],
            vec!["table.(column)".to_string()],
        ] {
            assert!(
                SearchRequest::parse_additional_columns(&bad).is_err(),
                "'additional_columns'={bad:?} is not allowed"
            );
        }
    }

    #[test]
    fn test_parse_additional_columns_empty() {
        assert_eq!(
            SearchRequest::parse_additional_columns(&[])
                .expect("failed to parse additional columns")
                .len(),
            0
        );
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
