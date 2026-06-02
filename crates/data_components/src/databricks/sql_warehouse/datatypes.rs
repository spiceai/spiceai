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

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
use logos::{Lexer, Logos};
use std::collections::HashMap;
use std::sync::Arc;

use crate::DESCRIPTION_METADATA_KEY;

const MAX_RECURSION_DEPTH: usize = 100;

#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(skip r"[ \t\n\f]+")] // Skip whitespace
pub enum Token<'input> {
    #[regex("(?i)BIGINT")]
    BigInt,
    #[regex("(?i)BINARY")]
    Binary,
    #[regex("(?i)BOOLEAN")]
    Boolean,
    #[regex("(?i)DATE")]
    Date,
    #[regex("(?i)DECIMAL")]
    Decimal,
    #[regex("(?i)DOUBLE")]
    Double,
    #[regex("(?i)FLOAT")]
    Float,
    #[regex("(?i)INT")]
    Int,
    #[regex("(?i)LONG")]
    Long,
    #[regex("(?i)VOID")]
    Void,
    #[regex("(?i)SMALLINT")]
    SmallInt,
    #[regex("(?i)STRING")]
    String,
    #[regex("(?i)TIMESTAMP")]
    Timestamp,
    #[regex("(?i)TIMESTAMP_NTZ")]
    TimestampNtz,
    #[regex("(?i)TINYINT")]
    TinyInt,
    #[regex("(?i)ARRAY")]
    Array,
    #[regex("(?i)MAP")]
    Map,
    #[regex("(?i)STRUCT")]
    Struct,
    #[regex("(?i)VARIANT")]
    Variant,
    #[regex("(?i)NOT")]
    Not,
    #[regex("(?i)NULL")]
    Null,
    #[regex("(?i)COMMENT")]
    Comment,
    #[token("<")]
    LAngle,
    #[token(">")]
    RAngle,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token(",")]
    Comma,
    #[token(":")]
    Colon,
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", |lex| lex.slice())]
    Identifier(&'input str),
    #[regex(r"[0-9]+", |lex| lex.slice().parse().ok())]
    Number(u32),
    #[regex(r"'[^']*'", |lex| lex.slice().trim_matches('\'').to_string())]
    QuotedString(String),
}

pub struct Parser<'input> {
    lexer: Lexer<'input, Token<'input>>,
    current: Option<Result<Token<'input>, ()>>,
}

impl<'input> Parser<'input> {
    pub fn new(input: &'input str) -> Self {
        let mut lexer = Token::lexer(input);
        let current = lexer.next();
        Parser { lexer, current }
    }

    fn advance(&mut self) {
        self.current = self.lexer.next();
    }

    fn expect(&mut self, token: &Token<'input>) -> Result<(), String> {
        match &self.current {
            Some(Ok(current_token)) if current_token == token => {
                self.advance();
                Ok(())
            }
            _ => Err(format!("Expected {token:?}, found {:?}", self.current)),
        }
    }

    /// Expects the current token to be an `Identifier` matching `name`
    /// (case-insensitive). Advances past it on success.
    fn expect_identifier(&mut self, name: &str, context: &str) -> Result<(), String> {
        match &self.current {
            Some(Ok(Token::Identifier(id))) if id.eq_ignore_ascii_case(name) => {
                self.advance();
                Ok(())
            }
            _ => Err(format!(
                "Expected '{name}' after {context}, found {:?}",
                self.current
            )),
        }
    }

    pub fn parse(&mut self) -> Result<ArrowDataType, String> {
        self.parse_data_type_with_depth(0)
    }

    fn parse_decimal(&mut self) -> Result<ArrowDataType, String> {
        self.advance();
        let params = if self.current == Some(Ok(Token::LParen)) {
            self.advance();
            let precision = if let Some(Ok(Token::Number(p))) = self.current {
                self.advance();
                p
            } else {
                return Err("Expected number for DECIMAL precision".to_string());
            };
            self.expect(&Token::Comma)?;
            let scale = if let Some(Ok(Token::Number(s))) = self.current {
                self.advance();
                s
            } else {
                return Err("Expected number for DECIMAL scale".to_string());
            };
            self.expect(&Token::RParen)?;
            Some((precision, scale))
        } else {
            None
        };
        Ok(match params {
            Some((p, s)) => {
                let precision =
                    u8::try_from(p).map_err(|e| format!("truncated Decimal precision: {e}"))?;
                let scale = i8::try_from(s).map_err(|e| format!("truncated Decimal scale: {e}"))?;
                if precision > 38 {
                    return Err(format!(
                        "DECIMAL precision {precision} exceeds maximum of 38"
                    ));
                }
                if u8::try_from(s).is_ok_and(|su| su > precision) {
                    return Err(format!(
                        "DECIMAL scale {scale} out of range for precision {precision}"
                    ));
                }
                ArrowDataType::Decimal128(precision, scale)
            }
            None => ArrowDataType::Decimal128(38, 10), // Default precision and scale
        })
    }

    fn parse_data_type_with_depth(&mut self, depth: usize) -> Result<ArrowDataType, String> {
        if depth > MAX_RECURSION_DEPTH {
            return Err(format!(
                "Maximum schema recursion depth exceeded ({MAX_RECURSION_DEPTH})"
            ));
        }

        match self.current.clone() {
            Some(Ok(Token::BigInt | Token::Long)) => {
                self.advance();
                Ok(ArrowDataType::Int64)
            }
            Some(Ok(Token::Binary)) => {
                self.advance();
                Ok(ArrowDataType::Binary)
            }
            Some(Ok(Token::Boolean)) => {
                self.advance();
                Ok(ArrowDataType::Boolean)
            }
            Some(Ok(Token::Date)) => {
                self.advance();
                Ok(ArrowDataType::Date32)
            }
            Some(Ok(Token::Decimal)) => self.parse_decimal(),
            Some(Ok(Token::Double)) => {
                self.advance();
                // Consume optional trailing "precision" from source-native
                // type name (e.g. PostgreSQL "double precision").
                if matches!(&self.current, Some(Ok(Token::Identifier(id))) if id.eq_ignore_ascii_case("precision"))
                {
                    self.advance();
                }
                Ok(ArrowDataType::Float64)
            }
            Some(Ok(Token::Float)) => {
                self.advance();
                Ok(ArrowDataType::Float32)
            }
            Some(Ok(Token::Int)) => {
                self.advance();
                Ok(ArrowDataType::Int32)
            }
            Some(Ok(Token::Void)) => {
                self.advance();
                Ok(ArrowDataType::Null)
            }
            Some(Ok(Token::SmallInt)) => {
                self.advance();
                Ok(ArrowDataType::Int16)
            }
            Some(Ok(Token::String | Token::Variant)) => {
                self.advance();
                Ok(ArrowDataType::Utf8)
            }
            Some(Ok(Token::Timestamp)) => {
                self.advance();
                // Handle source-native multi-word timestamp types from
                // Lakehouse Federation (e.g. PostgreSQL).
                if matches!(&self.current, Some(Ok(Token::Identifier(id))) if id.eq_ignore_ascii_case("without"))
                {
                    // "timestamp without time zone" → TimestampNtz
                    self.advance(); // consume "without"
                    self.expect_identifier("time", "'timestamp without'")?;
                    self.expect_identifier("zone", "'timestamp without time'")?;
                    return Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None));
                }
                if matches!(&self.current, Some(Ok(Token::Identifier(id))) if id.eq_ignore_ascii_case("with"))
                {
                    // "timestamp with time zone" → Timestamp(UTC)
                    self.advance(); // consume "with"
                    self.expect_identifier("time", "'timestamp with'")?;
                    self.expect_identifier("zone", "'timestamp with time'")?;
                    return Ok(ArrowDataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("UTC".into()),
                    ));
                }
                Ok(ArrowDataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                ))
            }
            Some(Ok(Token::TimestampNtz)) => {
                self.advance();
                Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
            }
            Some(Ok(Token::TinyInt)) => {
                self.advance();
                Ok(ArrowDataType::Int8)
            }
            Some(Ok(Token::Array)) => {
                self.advance();
                if self.current == Some(Ok(Token::LAngle)) {
                    self.advance();
                    let inner_type = self.parse_data_type_with_depth(depth + 1)?;
                    self.expect(&Token::RAngle)?;
                    let field = ArrowField::new("item", inner_type, true);
                    Ok(ArrowDataType::List(Arc::new(field)))
                } else {
                    // Fallback: no element type specified (e.g. from data_type column)
                    let field = ArrowField::new("item", ArrowDataType::Utf8, true);
                    Ok(ArrowDataType::List(Arc::new(field)))
                }
            }
            Some(Ok(Token::Map)) => self.parse_map_with_depth(depth),
            Some(Ok(Token::Struct)) => self.parse_struct_with_depth(depth),
            // GEOMETRY is not a first-class Arrow type; treat as Binary (WKB).
            // The lexer emits it as Identifier("GEOMETRY") since it has no
            // dedicated token. Consume any trailing `(SRID)` if present.
            Some(Ok(Token::Identifier(id))) if id.eq_ignore_ascii_case("GEOMETRY") => {
                self.advance();
                // Skip optional parenthesized SRID, e.g. `geometry(5070)`
                if self.current == Some(Ok(Token::LParen)) {
                    self.advance(); // skip `(`
                    self.advance(); // skip SRID number
                    self.expect(&Token::RParen)?;
                }
                Ok(ArrowDataType::Binary)
            }
            // Source-native type names from Lakehouse Federation. These
            // appear in `information_schema.columns.data_type` for foreign
            // tables backed by PostgreSQL, MySQL, SQL Server, etc.
            Some(Ok(Token::Identifier(id))) if id.eq_ignore_ascii_case("INTEGER") => {
                self.advance();
                Ok(ArrowDataType::Int32)
            }
            Some(Ok(Token::Identifier(id))) if id.eq_ignore_ascii_case("TEXT") => {
                self.advance();
                Ok(ArrowDataType::Utf8)
            }
            Some(Ok(Token::Identifier(id))) if id.eq_ignore_ascii_case("NUMERIC") => {
                self.advance();
                // Accept optional (precision, scale) like DECIMAL.
                if self.current == Some(Ok(Token::LParen)) {
                    // Re-use the DECIMAL parser by faking a rewind.
                    // We already advanced past "NUMERIC", and parse_decimal
                    // also starts after advancing past "DECIMAL". However,
                    // parse_decimal expects to be called with current = LParen
                    // after its own advance. We need to inline the param
                    // parsing here.
                    self.advance(); // skip `(`
                    let precision = if let Some(Ok(Token::Number(p))) = self.current {
                        self.advance();
                        p
                    } else {
                        return Err("Expected number for NUMERIC precision".to_string());
                    };
                    self.expect(&Token::Comma)?;
                    let scale = if let Some(Ok(Token::Number(s))) = self.current {
                        self.advance();
                        s
                    } else {
                        return Err("Expected number for NUMERIC scale".to_string());
                    };
                    self.expect(&Token::RParen)?;
                    let p = u8::try_from(precision)
                        .map_err(|e| format!("truncated NUMERIC precision: {e}"))?;
                    let s =
                        i8::try_from(scale).map_err(|e| format!("truncated NUMERIC scale: {e}"))?;
                    if p > 38 {
                        return Err(format!("NUMERIC precision {p} exceeds maximum of 38"));
                    }
                    // scale (originally u32, now i8) must not exceed precision
                    if u8::try_from(scale).is_ok_and(|su| su > p) {
                        return Err(format!("NUMERIC scale {s} out of range for precision {p}"));
                    }
                    Ok(ArrowDataType::Decimal128(p, s))
                } else {
                    Ok(ArrowDataType::Decimal128(38, 10))
                }
            }
            Some(Ok(Token::Identifier(id))) if id.eq_ignore_ascii_case("REAL") => {
                self.advance();
                Ok(ArrowDataType::Float32)
            }
            Some(Ok(Token::Identifier(id)))
                if id.eq_ignore_ascii_case("CHARACTER") || id.eq_ignore_ascii_case("VARCHAR") =>
            {
                self.advance();
                // Consume optional trailing "varying" from "character varying"
                if matches!(&self.current, Some(Ok(Token::Identifier(id))) if id.eq_ignore_ascii_case("varying"))
                {
                    self.advance();
                }
                // Consume optional (length) from "varchar(255)"
                if self.current == Some(Ok(Token::LParen)) {
                    self.advance(); // skip `(`
                    if !matches!(self.current, Some(Ok(Token::Number(_)))) {
                        return Err("Expected number for CHARACTER/VARCHAR length".to_string());
                    }
                    self.advance(); // skip length
                    self.expect(&Token::RParen)?;
                }
                Ok(ArrowDataType::Utf8)
            }
            _ => Err(format!("Unexpected token: {:?}", self.current)),
        }
    }

    fn parse_map_with_depth(&mut self, depth: usize) -> Result<ArrowDataType, String> {
        self.advance();
        if self.current != Some(Ok(Token::LAngle)) {
            // Fallback: no type parameters (e.g. from data_type column)
            let key_field = Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false));
            let value_field = Arc::new(ArrowField::new("value", ArrowDataType::Utf8, true));
            let entry_struct = Arc::new(ArrowField::new_struct(
                "entries",
                vec![key_field, value_field],
                false,
            ));
            return Ok(ArrowDataType::Map(entry_struct, false));
        }
        self.advance();
        let key_type = self.parse_data_type_with_depth(depth + 1)?;
        self.expect(&Token::Comma)?;
        let value_type = self.parse_data_type_with_depth(depth + 1)?;
        self.expect(&Token::RAngle)?;
        let key_field = Arc::new(ArrowField::new("key", key_type, false));
        let value_field = Arc::new(ArrowField::new("value", value_type, true));
        let entry_struct = Arc::new(ArrowField::new_struct(
            "entries",
            vec![key_field, value_field],
            false,
        ));
        Ok(ArrowDataType::Map(entry_struct, false))
    }

    fn parse_struct_with_depth(&mut self, depth: usize) -> Result<ArrowDataType, String> {
        self.advance();
        if self.current != Some(Ok(Token::LAngle)) {
            // Fallback: no field definitions (e.g. from data_type column)
            return Ok(ArrowDataType::Utf8);
        }
        self.expect(&Token::LAngle)?;
        let mut fields = Vec::new();
        if self.current != Some(Ok(Token::RAngle)) {
            loop {
                let field = self.parse_field_with_depth(depth + 1)?;
                fields.push(field);
                if self.current == Some(Ok(Token::Comma)) {
                    self.advance();
                    if self.current == Some(Ok(Token::RAngle)) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        self.expect(&Token::RAngle)?;
        Ok(ArrowDataType::Struct(fields.into()))
    }

    fn token_is_identifier(token: &Token<'input>) -> bool {
        matches!(
            token,
            Token::BigInt
                | Token::Binary
                | Token::Boolean
                | Token::Date
                | Token::Decimal
                | Token::Double
                | Token::Float
                | Token::Int
                | Token::Long
                | Token::Void
                | Token::SmallInt
                | Token::String
                | Token::Timestamp
                | Token::TimestampNtz
                | Token::TinyInt
                | Token::Array
                | Token::Map
                | Token::Struct
                | Token::Variant
                | Token::Not
                | Token::Null
                | Token::Comment
        )
    }

    fn parse_field_with_depth(&mut self, depth: usize) -> Result<ArrowField, String> {
        let name = match self.current.clone() {
            Some(Ok(Token::Identifier(name))) => {
                self.advance();
                name.to_string()
            }
            Some(Ok(token)) if Self::token_is_identifier(&token) => {
                let name = self.lexer.slice().to_string();
                self.advance();
                name
            }
            _ => return Err("Expected identifier for field name".to_string()),
        };
        self.expect(&Token::Colon)?;
        let data_type = self.parse_data_type_with_depth(depth)?;
        let nullable = if self.current == Some(Ok(Token::Not)) {
            self.advance();
            self.expect(&Token::Null)?;
            false
        } else {
            true
        };
        let metadata = if self.current == Some(Ok(Token::Comment)) {
            self.advance();
            if let Some(Ok(Token::QuotedString(s))) = self.current.clone() {
                self.advance();
                let mut metadata = HashMap::new();
                metadata.insert(DESCRIPTION_METADATA_KEY.to_string(), s);
                metadata
            } else {
                return Err("Expected quoted string for COMMENT".to_string());
            }
        } else {
            HashMap::new()
        };
        Ok(ArrowField::new(name, data_type, nullable).with_metadata(metadata))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};

    #[test]
    fn test_scalar_types() {
        let inputs = vec![
            "BIGINT",
            "bigint",
            "BiGiNt",
            "BINARY",
            "binary",
            "BOOLEAN",
            "boolEAN",
            "DATE",
            "date",
            "DOUBLE",
            "double",
            "FLOAT",
            "float",
            "INT",
            "int",
            "SMALLINT",
            "smallint",
            "STRING",
            "string",
            "TIMESTAMP",
            "timestamp",
            "TIMESTAMP_NTZ",
            "timestamp_ntz",
            "TINYINT",
            "tinyint",
            "VOID",
            "void",
            "VARIANT",
            "variant",
        ];
        let expected = vec![
            ArrowDataType::Int64,
            ArrowDataType::Int64,
            ArrowDataType::Int64,
            ArrowDataType::Binary,
            ArrowDataType::Binary,
            ArrowDataType::Boolean,
            ArrowDataType::Boolean,
            ArrowDataType::Date32,
            ArrowDataType::Date32,
            ArrowDataType::Float64,
            ArrowDataType::Float64,
            ArrowDataType::Float32,
            ArrowDataType::Float32,
            ArrowDataType::Int32,
            ArrowDataType::Int32,
            ArrowDataType::Int16,
            ArrowDataType::Int16,
            ArrowDataType::Utf8,
            ArrowDataType::Utf8,
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            ArrowDataType::Int8,
            ArrowDataType::Int8,
            ArrowDataType::Null,
            ArrowDataType::Null,
            ArrowDataType::Utf8,
            ArrowDataType::Utf8,
        ];

        for (input, expected) in inputs.iter().zip(expected.iter()) {
            let mut parser = Parser::new(input);
            let result = parser.parse().expect("parse success");
            assert_eq!(result, *expected, "Failed for input: {input}");
        }
    }

    #[test]
    fn test_struct_mixed_case() {
        let inputs = vec![
            "STRUCT<field1: INT NOT NULL COMMENT 'id field', field2: STRING>",
            "struct<field1: int NOT NULL COMMENT 'id field', field2: string>",
        ];
        let expected = ArrowDataType::Struct(
            vec![
                ArrowField::new("field1", ArrowDataType::Int32, false).with_metadata(
                    HashMap::from([(DESCRIPTION_METADATA_KEY.to_string(), "id field".to_string())]),
                ),
                ArrowField::new("field2", ArrowDataType::Utf8, true),
            ]
            .into(),
        );

        for input in inputs {
            let mut parser = Parser::new(input);
            let result = parser.parse().expect("parse success");
            assert_eq!(result, expected, "Failed for input: {input}");
        }
    }

    #[test]
    fn test_parse_rejects_excessive_recursion_depth() {
        let input = format!(
            "{}INT{}",
            "ARRAY<".repeat(MAX_RECURSION_DEPTH + 1),
            ">".repeat(MAX_RECURSION_DEPTH + 1)
        );

        let mut parser = Parser::new(&input);
        let err = parser.parse().expect_err("must reject excessive recursion");

        assert!(err.contains("Maximum schema recursion depth exceeded"));
    }

    #[test]
    fn test_struct_reserved_field_names() {
        let input = "STRUCT<date: STRING, value: INT>";

        let expected = ArrowDataType::Struct(
            vec![
                ArrowField::new("date", ArrowDataType::Utf8, true),
                ArrowField::new("value", ArrowDataType::Int32, true),
            ]
            .into(),
        );

        let mut parser = Parser::new(input);
        let result = parser.parse().expect("parse success");
        assert_eq!(result, expected, "Failed for input: {input}");
    }

    #[test]
    fn test_nested_type() {
        let input = "ARRAY<STRUCT<field1: INT, field2: MAP<STRING, DECIMAL(10,2)>>>";

        let expected = ArrowDataType::List(Arc::new(ArrowField::new(
            "item",
            ArrowDataType::Struct(
                vec![
                    ArrowField::new("field1", ArrowDataType::Int32, true),
                    ArrowField::new(
                        "field2",
                        {
                            let key_field =
                                Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false));
                            let value_field = Arc::new(ArrowField::new(
                                "value",
                                ArrowDataType::Decimal128(10, 2),
                                true,
                            ));
                            let entry_struct = Arc::new(ArrowField::new_struct(
                                "entries",
                                vec![key_field, value_field],
                                false,
                            ));
                            ArrowDataType::Map(entry_struct, false)
                        },
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        )));

        let mut parser = Parser::new(input);
        let result = parser.parse().expect("parse success");
        assert_eq!(result, expected, "Failed for input: {input}");
    }

    #[test]
    fn test_parameterless_complex_types() {
        // When using `data_type` column instead of `full_data_type`, complex types
        // come without type parameters (e.g. just "ARRAY" instead of "ARRAY<STRING>").
        let mut parser = Parser::new("ARRAY");
        let result = parser.parse().expect("parse ARRAY without params");
        let expected_array =
            ArrowDataType::List(Arc::new(ArrowField::new("item", ArrowDataType::Utf8, true)));
        assert_eq!(result, expected_array);

        let mut parser = Parser::new("MAP");
        let result = parser.parse().expect("parse MAP without params");
        let key = Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false));
        let val = Arc::new(ArrowField::new("value", ArrowDataType::Utf8, true));
        let entries = Arc::new(ArrowField::new_struct("entries", vec![key, val], false));
        let expected_map = ArrowDataType::Map(entries, false);
        assert_eq!(result, expected_map);

        let mut parser = Parser::new("STRUCT");
        let result = parser.parse().expect("parse STRUCT without params");
        assert_eq!(result, ArrowDataType::Utf8);

        let mut parser = Parser::new("DECIMAL");
        let result = parser.parse().expect("parse DECIMAL without params");
        assert_eq!(result, ArrowDataType::Decimal128(38, 10));

        // GEOMETRY with SRID → Binary
        let mut parser = Parser::new("geometry(5070)");
        let result = parser.parse().expect("parse GEOMETRY with SRID");
        assert_eq!(result, ArrowDataType::Binary);

        // GEOMETRY without SRID → Binary
        let mut parser = Parser::new("GEOMETRY");
        let result = parser.parse().expect("parse GEOMETRY without SRID");
        assert_eq!(result, ArrowDataType::Binary);
    }

    /// Databricks sends Arrow IPC data with `Timestamp(Microsecond, ...)` but
    /// the schema parser previously declared `Timestamp(Nanosecond, ...)`,
    /// causing arithmetic overflow when casting far-future sentinel values
    /// (e.g. year 9999: 253402300799999000 µs × 1000 > `i64::MAX`).
    ///
    /// This test ensures the parser declares Microsecond to match the wire format.
    #[test]
    fn test_timestamp_uses_microsecond_to_prevent_overflow() {
        let mut parser = Parser::new("TIMESTAMP");
        let result = parser.parse().expect("parse TIMESTAMP");
        assert_eq!(
            result,
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "TIMESTAMP must use Microsecond to match Databricks Arrow IPC format"
        );

        let mut parser = Parser::new("TIMESTAMP_NTZ");
        let result = parser.parse().expect("parse TIMESTAMP_NTZ");
        assert_eq!(
            result,
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            "TIMESTAMP_NTZ must use Microsecond to match Databricks Arrow IPC format"
        );

        // Verify the sentinel value 9999-12-31T23:59:59.999 fits in i64 microseconds
        // but would overflow in nanoseconds (253402300799999000 * 1000 > i64::MAX).
        let sentinel_us: i64 = 253_402_300_799_999_000;
        assert!(
            sentinel_us.checked_mul(1000).is_none(),
            "year-9999 sentinel must overflow when converting µs→ns"
        );
    }

    /// Databricks `data_type` column returns `LONG` for what `full_data_type`
    /// calls `bigint`. Both must parse to `Int64`.
    #[test]
    fn test_long_maps_to_int64() {
        for input in ["LONG", "long", "Long"] {
            let mut parser = Parser::new(input);
            let result = parser.parse().expect("should parse LONG variant");
            assert_eq!(result, ArrowDataType::Int64, "Failed for input: {input}");
        }
    }

    /// Covers the exact `full_data_type` values from a real Databricks
    /// `information_schema.columns` dump: bigint, string, timestamp,
    /// boolean, double.
    #[test]
    fn test_full_data_type_column_values() {
        let cases = vec![
            ("bigint", ArrowDataType::Int64),
            ("string", ArrowDataType::Utf8),
            (
                "timestamp",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            ("boolean", ArrowDataType::Boolean),
            ("double", ArrowDataType::Float64),
        ];
        for (input, expected) in &cases {
            let mut parser = Parser::new(input);
            let result = parser.parse().expect("should parse full_data_type value");
            assert_eq!(
                result, *expected,
                "Failed for full_data_type input: {input}"
            );
        }
    }

    /// Covers the exact `data_type` column values from a real Databricks
    /// `information_schema.columns` dump: LONG, STRING, TIMESTAMP,
    /// BOOLEAN, DOUBLE. These are what the fallback path receives.
    #[test]
    fn test_data_type_column_values() {
        let cases = vec![
            ("LONG", ArrowDataType::Int64),
            ("STRING", ArrowDataType::Utf8),
            (
                "TIMESTAMP",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            ("BOOLEAN", ArrowDataType::Boolean),
            ("DOUBLE", ArrowDataType::Float64),
        ];
        for (input, expected) in &cases {
            let mut parser = Parser::new(input);
            let result = parser.parse().expect("should parse data_type value");
            assert_eq!(result, *expected, "Failed for data_type input: {input}");
        }
    }

    /// Source-native type names from Lakehouse Federation foreign tables
    /// (e.g. `PostgreSQL`). These appear in `information_schema.columns.data_type`
    /// when querying federated tables.
    #[test]
    fn test_source_native_types() {
        let cases = vec![
            ("integer", ArrowDataType::Int32),
            ("INTEGER", ArrowDataType::Int32),
            ("text", ArrowDataType::Utf8),
            ("TEXT", ArrowDataType::Utf8),
            ("numeric", ArrowDataType::Decimal128(38, 10)),
            ("NUMERIC", ArrowDataType::Decimal128(38, 10)),
            ("numeric(10,2)", ArrowDataType::Decimal128(10, 2)),
            ("NUMERIC(18,4)", ArrowDataType::Decimal128(18, 4)),
            ("real", ArrowDataType::Float32),
            ("REAL", ArrowDataType::Float32),
            ("double precision", ArrowDataType::Float64),
            ("DOUBLE PRECISION", ArrowDataType::Float64),
            ("character varying", ArrowDataType::Utf8),
            ("CHARACTER VARYING", ArrowDataType::Utf8),
            ("varchar", ArrowDataType::Utf8),
            ("varchar(255)", ArrowDataType::Utf8),
            ("VARCHAR(100)", ArrowDataType::Utf8),
            (
                "timestamp without time zone",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "TIMESTAMP WITHOUT TIME ZONE",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "timestamp with time zone",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                "TIMESTAMP WITH TIME ZONE",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
        ];
        for (input, expected) in &cases {
            let mut parser = Parser::new(input);
            let result = parser
                .parse()
                .unwrap_or_else(|e| panic!("should parse native type '{input}': {e}"));
            assert_eq!(result, *expected, "Failed for source-native type: {input}");
        }
    }

    /// Schema test for the Neon `PostgreSQL` foreign table from a real
    /// DESCRIBE TABLE response (Spark SQL types).
    #[test]
    fn test_neon_pg_describe_table_types() {
        let cases = vec![
            ("int", ArrowDataType::Int32),
            ("string", ArrowDataType::Utf8),
            ("decimal(10,2)", ArrowDataType::Decimal128(10, 2)),
            (
                "timestamp",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            ("boolean", ArrowDataType::Boolean),
        ];
        for (input, expected) in &cases {
            let mut parser = Parser::new(input);
            let result = parser.parse().expect("should parse DESCRIBE TABLE type");
            assert_eq!(result, *expected, "Failed for DESCRIBE TABLE type: {input}");
        }
    }

    /// Schema test for the Neon `PostgreSQL` foreign table from
    /// `information_schema.columns.data_type` (source-native types).
    #[test]
    fn test_neon_pg_information_schema_native_types() {
        let cases = vec![
            ("integer", ArrowDataType::Int32),
            ("text", ArrowDataType::Utf8),
            ("numeric", ArrowDataType::Decimal128(38, 10)),
            (
                "timestamp without time zone",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            ("boolean", ArrowDataType::Boolean),
        ];
        for (input, expected) in &cases {
            let mut parser = Parser::new(input);
            let result = parser.parse().expect("should parse native type");
            assert_eq!(
                result, *expected,
                "Failed for information_schema native type: {input}"
            );
        }
    }
}
