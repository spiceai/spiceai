/*
Copyright 2025 The Spice.ai OSS Authors

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

#![allow(clippy::missing_errors_doc)]

use datafusion::sql::{
    TableReference,
    sqlparser::{
        ast::Ident,
        dialect::GenericDialect,
        parser::{Parser, ParserError},
        tokenizer::Token,
    },
};
use globset::{Glob, GlobSet};

#[derive(Debug, Clone, Default)]
pub struct ResolvedTableAwareAllowlist {
    default_catalog: String,
    default_schema: String,
    allowed: GlobSet,
}

impl ResolvedTableAwareAllowlist {
    pub fn with_defaults(
        default_catalog: impl Into<String>,
        default_schema: impl Into<String>,
    ) -> Self {
        Self {
            default_catalog: default_catalog.into(),
            default_schema: default_schema.into(),
            allowed: GlobSet::default(),
        }
    }

    /// Create a new [`ResolvedTableAwareAllowlist`] from the provided table patterns.
    pub fn with_table_patterns(self, tables: Vec<String>) -> Result<Self, globset::Error> {
        let mut bldr = GlobSet::builder();
        for t in tables {
            let resolved = Self::parse_table_with_wildcards(&t)
                .resolve(&self.default_catalog, &self.default_schema);

            bldr.add(Glob::new(
                format!(
                    r"{}.{}.{}",
                    resolved.catalog, resolved.schema, resolved.table
                )
                .as_str(),
            )?);
        }
        Ok(Self {
            default_catalog: self.default_catalog,
            default_schema: self.default_schema,
            allowed: bldr.build()?,
        })
    }

    /// Check if the provided [`TableReference`] is allowed by the allowlist.
    #[must_use]
    pub fn table_is_allowed(&self, table: &TableReference) -> bool {
        let resolved = table
            .clone()
            .resolve(&self.default_catalog, &self.default_schema);

        self.allowed.is_match(
            format!(
                r"{}.{}.{}",
                resolved.catalog, resolved.schema, resolved.table
            )
            .as_str(),
        )
    }

    /// An implementation of [`TableReference::parse_str`] that will handle `*`.
    fn parse_table_with_wildcards(t: &str) -> TableReference {
        let Ok(parts) = Self::make_parts(t) else {
            return TableReference::Bare { table: t.into() };
        };

        match &parts[..] {
            [] => TableReference::Bare { table: t.into() },
            [table] => TableReference::Bare {
                table: table.as_str().into(),
            },
            [schema, table] => TableReference::Partial {
                schema: schema.as_str().into(),
                table: table.as_str().into(),
            },
            [catalog, schema, table] => TableReference::Full {
                catalog: catalog.as_str().into(),
                schema: schema.as_str().into(),
                table: table.as_str().into(),
            },
            [catalog, schema, rest @ ..] => TableReference::Full {
                catalog: catalog.as_str().into(),
                schema: schema.as_str().into(),
                table: rest.concat().as_str().into(),
            },
        }
    }

    fn make_parts(s: &str) -> Result<Vec<String>, ParserError> {
        let dialect = GenericDialect;
        let mut parser = Parser::new(&dialect).try_with_sql(s)?;
        let mut idents = vec![];

        // expecting at least one word for identifier
        let next_token = parser.next_token();
        match next_token.token {
            Token::Word(w) => idents.push(w.into_ident(next_token.span)),
            Token::Mul => idents.push(Ident::new("*")),
            Token::EOF => {
                return Err(ParserError::ParserError(
                    "Empty input when parsing identifier".to_string(),
                ))?;
            }
            token => {
                return Err(ParserError::ParserError(format!(
                    "Unexpected token in identifier: {token}"
                )))?;
            }
        }

        loop {
            match parser.next_token().token {
                // ensure that optional period is succeeded by another identifier
                Token::Period => match parser.next_token().token {
                    Token::Word(w) => idents.push(w.into_ident(next_token.span)),
                    Token::Mul => idents.push(Ident::new("*")),
                    Token::EOF => {
                        return Err(ParserError::ParserError(
                            "Trailing period in identifier".to_string(),
                        ))?;
                    }
                    token => {
                        return Err(ParserError::ParserError(format!(
                            "Unexpected token following period in identifier: {token}"
                        )))?;
                    }
                },
                Token::EOF => break,
                token => {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token in identifier: {token}"
                    )))?;
                }
            }
        }

        Ok(idents
            .into_iter()
            .map(|id| match id.quote_style {
                Some(_) => id.value,
                _ => id.value.to_ascii_lowercase(),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {

    use datafusion::sql::TableReference;

    use super::ResolvedTableAwareAllowlist;

    #[test]
    fn test_allowlist() {
        let allowlist =
            ResolvedTableAwareAllowlist::with_defaults("spice".to_string(), "public".to_string())
                .with_table_patterns(vec![
                    "spice.public.*".to_string(),
                    "spice.my_schema.my_table".to_string(),
                    r#"spice."My.Schema".*"#.to_string(),
                    "spice.other_schema.*".to_string(),
                    "*.my_schema.another_table".to_string(),
                ])
                .expect("Failed to create allowlist");
        assert!(allowlist.table_is_allowed(&TableReference::parse_str("another_table")));
        assert!(allowlist.table_is_allowed(&TableReference::parse_str("public.another_table")));
        assert!(
            allowlist.table_is_allowed(&TableReference::parse_str("spice.public.another_table"))
        );
        assert!(allowlist.table_is_allowed(&TableReference::parse_str("spice.my_schema.my_table")));
        assert!(
            allowlist.table_is_allowed(&TableReference::parse_str("spice.other_schema.some_table"))
        );
        assert!(
            allowlist.table_is_allowed(&TableReference::parse_str("spice.my_schema.another_table"))
        );

        assert!(
            !allowlist
                .table_is_allowed(&TableReference::parse_str("spice.my_schema.unlisted_table"))
        );
        assert!(allowlist.table_is_allowed(&TableReference::parse_str(
            "other_catalog.my_schema.another_table"
        )));

        assert!(
            !allowlist.table_is_allowed(&TableReference::parse_str(r#"spice."my.schema".table1"#))
        );
        assert!(
            allowlist.table_is_allowed(&TableReference::parse_str(r#"spice."My.Schema".table1"#))
        );
    }
}
