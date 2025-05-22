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

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
use logos::{Lexer, Logos};
use std::collections::HashMap;
use std::sync::Arc;

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

    pub fn parse(&mut self) -> Result<ArrowDataType, String> {
        self.parse_data_type()
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
            Some((p, s)) => ArrowDataType::Decimal128(
                u8::try_from(p).map_err(|e| format!("truncated Decimal precision: {e}"))?,
                i8::try_from(s).map_err(|e| format!("truncated Decimal scale: {e}"))?,
            ),
            None => ArrowDataType::Decimal128(38, 10), // Default precision and scale
        })
    }

    fn parse_data_type(&mut self) -> Result<ArrowDataType, String> {
        match self.current.clone() {
            Some(Ok(Token::BigInt)) => {
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
                Ok(ArrowDataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some("UTC".into()),
                ))
            }
            Some(Ok(Token::TimestampNtz)) => {
                self.advance();
                Ok(ArrowDataType::Timestamp(TimeUnit::Nanosecond, None))
            }
            Some(Ok(Token::TinyInt)) => {
                self.advance();
                Ok(ArrowDataType::Int8)
            }
            Some(Ok(Token::Array)) => {
                self.advance();
                self.expect(&Token::LAngle)?;
                let inner_type = self.parse_data_type()?;
                self.expect(&Token::RAngle)?;
                let field = ArrowField::new("item", inner_type, true);
                Ok(ArrowDataType::List(Arc::new(field)))
            }
            Some(Ok(Token::Map)) => self.parse_map(),
            Some(Ok(Token::Struct)) => self.parse_struct(),
            _ => Err(format!("Unexpected token: {:?}", self.current)),
        }
    }

    fn parse_map(&mut self) -> Result<ArrowDataType, String> {
        self.advance();
        self.expect(&Token::LAngle)?;
        let key_type = self.parse_data_type()?;
        self.expect(&Token::Comma)?;
        let value_type = self.parse_data_type()?;
        self.expect(&Token::RAngle)?;
        let key_field = Arc::new(ArrowField::new("key", key_type, false));
        let value_field = Arc::new(ArrowField::new("value", value_type, true));
        let entry_struct = Arc::new(ArrowField::new_struct(
            "entries",
            vec![key_field, value_field],
            true,
        ));
        Ok(ArrowDataType::Map(entry_struct, false))
    }

    fn parse_struct(&mut self) -> Result<ArrowDataType, String> {
        self.advance();
        self.expect(&Token::LAngle)?;
        let mut fields = Vec::new();
        if self.current != Some(Ok(Token::RAngle)) {
            loop {
                let field = self.parse_field()?;
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

    fn parse_field(&mut self) -> Result<ArrowField, String> {
        let name = if let Some(Ok(Token::Identifier(name))) = self.current.clone() {
            self.advance();
            name.to_string()
        } else {
            return Err("Expected identifier for field name".to_string());
        };
        self.expect(&Token::Colon)?;
        let data_type = self.parse_data_type()?;
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
                metadata.insert("comment".to_string(), s);
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
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
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
                    HashMap::from([("comment".to_string(), "id field".to_string())]),
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
                                true,
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
}
