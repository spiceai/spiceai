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

//! Lenient parser for `columns[].type` declarations on datasets.
//!
//! Accepts three families of type expressions:
//!
//!   1. **Arrow display forms** — `Int64`, `Utf8`, `Float64`, `Bool`,
//!      `Date32`, `Timestamp(Nanosecond, UTC)`, `List<Int64>`,
//!      `Decimal128(p, s)`, etc. Routed through Arrow's
//!      `DataType::from_str` for primitive lookup.
//!   2. **Postgres / SQL forms** via `DataFusion`'s `sqlparser` —
//!      `BIGINT`, `INTEGER`, `INT`, `SMALLINT`, `TEXT`, `VARCHAR(n)`,
//!      `CHAR(n)`, `BOOLEAN`, `BOOL`, `REAL`, `DOUBLE PRECISION`,
//!      `NUMERIC(p,s)` / `DECIMAL(p,s)`, `DATE`, `TIME`, `TIMESTAMP`,
//!      `TIMESTAMP WITH TIME ZONE`, `JSONB` → `Utf8`, `BYTEA` → `Binary`,
//!      etc.
//!   3. **Postgres aliases not in `sqlparser`** — `int2`, `int4`, `int8`,
//!      `float4`, `float8`, `serial`, `bigserial`, `timestamptz`, `uuid`,
//!      and the `T[]` array suffix.
//!
//! In addition, three surface syntaxes for Arrow `Map`:
//!
//!   * `Map<K, V>` — friendly Arrow-flavored shorthand
//!     (e.g. `Map<Utf8, Int64>`).
//!   * `map<K, V>` — lowercase variant for users coming from Hive,
//!     Spark, or `ClickHouse`.
//!   * The full Arrow canonical display form (whatever `DataType::from_str`
//!     emits) if a user pastes it back from Arrow tooling.
//!
//! Map keys default to non-nullable, values to nullable, matching Arrow
//! conventions and the existing HTTP envelope `response_headers` field
//! (`Map<Utf8 not null, Utf8>`). The standard `entries` / `keys` /
//! `values` field naming is used; `keys_sorted` is `false`.
//!
//! Postgres has no native `MAP` type. Users who want a map column on a
//! Postgres source typically model it as `JSONB`, which this parser maps
//! to `Utf8` (we do not introspect arbitrary JSONB structure).
//!
//! ## Implementation
//!
//! The parser tokenises the input with `logos` (matching the convention
//! used by `runtime-secrets::lexer`) and then walks the token stream as a
//! small recursive-descent parser. The grammar is:
//!
//! ```text
//! type        := primary array_suffix*
//! array_suffix:= '[' ']'
//! primary     := map | list | leaf
//! map         := ('Map'|'map') '<' type ',' type '>'
//! list        := ('List'|'list') '<' type '>'
//! leaf        := IDENT IDENT* paren_args?
//! paren_args  := '(' .* ')'   // matched by paren depth
//! ```
//!
//! Leaf type lookup is delegated, in order, to:
//!
//!   1. The Postgres alias table (`parse_pg_alias`).
//!   2. Arrow's `DataType::from_str`.
//!   3. `sqlparser::Parser::parse_data_type` mapped to `arrow::DataType`.
//!
//! Tokenising before parsing means bracket and comma structure is always
//! handled at the `logos` layer, keeping the recursive descent free of
//! ad-hoc string scans and ensuring trailing-junk inputs (e.g.
//! `Int64<>`) cleanly fail rather than being silently truncated by
//! `DataType::from_str` or `parse_data_type`.

use std::str::FromStr;

use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use datafusion::sql::sqlparser::{
    ast::{DataType as SqlDataType, ExactNumberInfo, TimezoneInfo},
    dialect::PostgreSqlDialect,
    parser::Parser,
    tokenizer::Token,
};
use logos::Logos;
use snafu::Snafu;

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum ParseTypeError {
    #[snafu(display(
        "Could not parse column type `{input}`. \
         Accepted forms include Postgres types (e.g. `bigint`, `text`, `numeric(18,4)`, \
         `timestamptz`, `text[]`), Arrow display forms (e.g. `Int64`, `Utf8`, \
         `Timestamp(Nanosecond, UTC)`, `List<Int64>`, `Decimal128(18, 4)`), and \
         Map types (`Map<K, V>` / `map<k, v>`)."
    ))]
    Unrecognized { input: String },

    #[snafu(display(
        "Could not parse map type `{input}`: {reason}. \
         Expected `Map<KeyType, ValueType>` (e.g. `Map<Utf8, Int64>`)."
    ))]
    InvalidMap { input: String, reason: String },

    #[snafu(display("Could not parse element type of array `{input}`: {source}"))]
    InvalidArrayElement {
        input: String,
        #[snafu(source(from(ParseTypeError, Box::new)))]
        source: Box<ParseTypeError>,
    },
}

/// Parse a user-supplied type string into an Arrow `DataType`.
///
/// See the module-level docs for the full list of accepted forms.
pub fn parse_declared_type(input: &str) -> Result<DataType, ParseTypeError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ParseTypeError::Unrecognized {
            input: input.to_string(),
        });
    }

    let tokens = tokenise(input)?;
    let mut parser = TokenParser {
        tokens: &tokens,
        pos: 0,
        original: input,
    };
    let ty = parser.parse_type()?;
    if parser.pos != tokens.len() {
        return Err(ParseTypeError::Unrecognized {
            input: input.to_string(),
        });
    }
    Ok(ty)
}

#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(skip r"[ \t\n\r\f]+")]
enum TypeToken {
    /// Identifier or keyword (case-insensitive comparisons happen at the
    /// parser layer). Matches Arrow primitive names (`Int64`, `Utf8`),
    /// Postgres types (`bigint`, `timestamptz`), keywords inside type
    /// arguments (`Microsecond`, `UTC`, `with`, `time`, `zone`), etc.
    #[regex(r"[A-Za-z_][A-Za-z0-9_]*", |lex| lex.slice().to_owned())]
    Ident(String),

    /// Unsigned decimal literal. Used for type arguments such as
    /// `Decimal128(18, 4)` and `varchar(255)`.
    #[regex(r"[0-9]+", |lex| lex.slice().to_owned())]
    Number(String),

    /// Double- or single-quoted string. Used inside Arrow display
    /// forms such as `Timestamp(Nanosecond, "UTC")`. Surrounding
    /// quotes are preserved in the token text so that the leaf
    /// reconstruction step round-trips the original input.
    #[regex(r#""[^"]*""#, |lex| lex.slice().to_owned())]
    #[regex(r#"'[^']*'"#, |lex| lex.slice().to_owned())]
    QuotedString(String),

    /// Punctuation that may appear inside type arguments (notably
    /// timezone offsets like `+05:30`).
    #[token(":")]
    Colon,
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,

    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("<")]
    LAngle,
    #[token(">")]
    RAngle,
    #[token(",")]
    Comma,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
}

struct TokenSpan {
    token: TypeToken,
    span: std::ops::Range<usize>,
}

fn tokenise(input: &str) -> Result<Vec<TokenSpan>, ParseTypeError> {
    let mut lexer = TypeToken::lexer(input);
    let mut out = Vec::new();
    while let Some(result) = lexer.next() {
        match result {
            Ok(token) => out.push(TokenSpan {
                token,
                span: lexer.span(),
            }),
            Err(()) => {
                return Err(ParseTypeError::Unrecognized {
                    input: input.to_string(),
                });
            }
        }
    }
    Ok(out)
}

struct TokenParser<'a> {
    tokens: &'a [TokenSpan],
    pos: usize,
    original: &'a str,
}

impl<'a> TokenParser<'a> {
    fn peek(&self) -> Option<&'a TypeToken> {
        self.tokens.get(self.pos).map(|t| &t.token)
    }

    fn peek_at(&self, offset: usize) -> Option<&'a TypeToken> {
        self.tokens.get(self.pos + offset).map(|t| &t.token)
    }

    fn advance(&mut self) -> Option<&'a TokenSpan> {
        let t = self.tokens.get(self.pos)?;
        self.pos += 1;
        Some(t)
    }

    fn expect(&mut self, expected: &TypeToken) -> Result<(), ParseTypeError> {
        match self.peek() {
            Some(actual) if std::mem::discriminant(actual) == std::mem::discriminant(expected) => {
                self.pos += 1;
                Ok(())
            }
            _ => Err(ParseTypeError::Unrecognized {
                input: self.original.to_string(),
            }),
        }
    }

    fn parse_type(&mut self) -> Result<DataType, ParseTypeError> {
        let mut ty = self.parse_primary()?;
        // Trailing array suffixes: T[] → List<T>, applied repeatedly.
        while matches!(self.peek(), Some(TypeToken::LBracket)) {
            self.advance();
            self.expect(&TypeToken::RBracket)?;
            ty = list_of(ty);
        }
        Ok(ty)
    }

    fn parse_primary(&mut self) -> Result<DataType, ParseTypeError> {
        // Check for Map<K, V> or List<T> shorthand.
        if let Some(TypeToken::Ident(name)) = self.peek()
            && matches!(self.peek_at(1), Some(TypeToken::LAngle))
        {
            if name.eq_ignore_ascii_case("map") {
                self.advance(); // map
                self.advance(); // <
                let key = self.parse_type().map_err(|e| ParseTypeError::InvalidMap {
                    input: self.original.to_string(),
                    reason: format!("invalid key type: {e}"),
                })?;
                self.expect(&TypeToken::Comma)
                    .map_err(|_| ParseTypeError::InvalidMap {
                        input: self.original.to_string(),
                        reason: "expected `,` between key and value types".to_string(),
                    })?;
                let value = self.parse_type().map_err(|e| ParseTypeError::InvalidMap {
                    input: self.original.to_string(),
                    reason: format!("invalid value type: {e}"),
                })?;
                self.expect(&TypeToken::RAngle)
                    .map_err(|_| ParseTypeError::InvalidMap {
                        input: self.original.to_string(),
                        reason: "expected closing `>`".to_string(),
                    })?;
                return Ok(map_of(key, value));
            }
            if name.eq_ignore_ascii_case("list") {
                self.advance(); // list
                self.advance(); // <
                let element =
                    self.parse_type()
                        .map_err(|e| ParseTypeError::InvalidArrayElement {
                            input: self.original.to_string(),
                            source: Box::new(e),
                        })?;
                self.expect(&TypeToken::RAngle)?;
                return Ok(list_of(element));
            }
        }

        self.parse_leaf()
    }

    fn parse_leaf(&mut self) -> Result<DataType, ParseTypeError> {
        let start = self.pos;

        // Consume one or more identifiers (handles multi-word types like
        // `double precision` and `timestamp with time zone`).
        let mut consumed_any = false;
        while matches!(self.peek(), Some(TypeToken::Ident(_))) {
            self.advance();
            consumed_any = true;
        }
        if !consumed_any {
            return Err(ParseTypeError::Unrecognized {
                input: self.original.to_string(),
            });
        }

        // Optional parenthesised arguments — consume the matching paren
        // group as a flat run; the inner sqlparser/Arrow parser
        // re-interprets it.
        if matches!(self.peek(), Some(TypeToken::LParen)) {
            self.advance();
            let mut depth = 1usize;
            while depth > 0 {
                match self.advance() {
                    Some(t) => match t.token {
                        TypeToken::LParen => depth += 1,
                        TypeToken::RParen => depth -= 1,
                        _ => {}
                    },
                    None => {
                        return Err(ParseTypeError::Unrecognized {
                            input: self.original.to_string(),
                        });
                    }
                }
            }
        }

        // Reconstruct the leaf substring from token spans (preserving the
        // user's original capitalization and whitespace), then look up.
        let span_start = self.tokens[start].span.start;
        let span_end = self.tokens[self.pos - 1].span.end;
        let leaf = &self.original[span_start..span_end];

        leaf_lookup(leaf).ok_or_else(|| ParseTypeError::Unrecognized {
            input: self.original.to_string(),
        })
    }
}

fn list_of(element: DataType) -> DataType {
    DataType::List(std::sync::Arc::new(Field::new("item", element, true)))
}

fn map_of(key: DataType, value: DataType) -> DataType {
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("keys", key, false),
                Field::new("values", value, true),
            ]
            .into(),
        ),
        false,
    );
    DataType::Map(std::sync::Arc::new(entries), false)
}

/// Look up a leaf type expression (one without Map/List shorthand and
/// without trailing array brackets) by trying, in order:
///
///   1. The Postgres / Arrow alias table (`parse_pg_alias`).
///   2. Arrow's `DataType::from_str`.
///   3. `sqlparser::Parser::parse_data_type` mapped to `arrow::DataType`.
fn leaf_lookup(s: &str) -> Option<DataType> {
    if let Some(dt) = parse_pg_alias(s) {
        return Some(dt);
    }
    if let Some(dt) = parse_arrow_timestamp_display(s) {
        return Some(dt);
    }
    if let Ok(dt) = DataType::from_str(s) {
        return Some(dt);
    }
    parse_via_sqlparser(s).ok()
}

/// Parse Arrow's `Display` form for `Timestamp(<unit>[, <tz>])` and
/// `Time32`/`Time64(<unit>)`. Arrow's own `DataType::from_str` only
/// accepts the no-arguments and `None`-timezone variants for these,
/// so users cannot otherwise round-trip `"{:?}", DataType::Timestamp(...)`
/// through the declared-type parser. Examples this accepts:
///
///   * `Timestamp(Nanosecond, UTC)`
///   * `Timestamp(Microsecond, "UTC")`
///   * `Timestamp(Millisecond, +05:30)`
///   * `Time64(Microsecond)`
///   * `Time32(Second)`
fn parse_arrow_timestamp_display(s: &str) -> Option<DataType> {
    let s = s.trim();
    let (head, args) = split_call(s)?;
    let head_lower = head.to_ascii_lowercase();
    match head_lower.as_str() {
        "timestamp" => {
            let mut parts = args.splitn(2, ',');
            let unit = parse_time_unit(parts.next()?.trim())?;
            let tz = match parts.next() {
                None => None,
                Some(rest) => match rest.trim() {
                    "None" => None,
                    other => Some(strip_quotes(other).to_string().into()),
                },
            };
            Some(DataType::Timestamp(unit, tz))
        }
        "time32" => Some(DataType::Time32(parse_time_unit(args.trim())?)),
        "time64" => Some(DataType::Time64(parse_time_unit(args.trim())?)),
        _ => None,
    }
}

fn split_call(s: &str) -> Option<(&str, &str)> {
    let open = s.find('(')?;
    if !s.ends_with(')') {
        return None;
    }
    Some((&s[..open], &s[open + 1..s.len() - 1]))
}

fn parse_time_unit(s: &str) -> Option<TimeUnit> {
    match s.to_ascii_lowercase().as_str() {
        "second" | "s" => Some(TimeUnit::Second),
        "millisecond" | "ms" => Some(TimeUnit::Millisecond),
        "microsecond" | "us" => Some(TimeUnit::Microsecond),
        "nanosecond" | "ns" => Some(TimeUnit::Nanosecond),
        _ => None,
    }
}

fn strip_quotes(s: &str) -> &str {
    let s = s.trim();
    if (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
        || (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
    {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

/// Postgres aliases that `sqlparser` either lacks or maps to a different
/// width than what feels natural, plus lowercase aliases for common Arrow
/// primitives so users can write `utf8` / `int64` inside shortcuts and
/// elsewhere.
///
/// Note: there is intentionally no lowercase alias for Arrow's `Int8` —
/// Postgres `int8` is `bigint`/`Int64`. Users wanting Arrow `Int8`
/// should write `Int8` (capitalized), routed through `DataType::from_str`.
fn parse_pg_alias(input: &str) -> Option<DataType> {
    match input.to_ascii_lowercase().as_str() {
        // Postgres integer aliases
        "serial" => {
            tracing::debug!(
                "Parsed Postgres `serial` as Int32; sequence semantics are not modeled."
            );
            Some(DataType::Int32)
        }
        "bigserial" => {
            tracing::debug!(
                "Parsed Postgres `bigserial` as Int64; sequence semantics are not modeled."
            );
            Some(DataType::Int64)
        }
        "smallserial" => {
            tracing::debug!(
                "Parsed Postgres `smallserial` as Int16; sequence semantics are not modeled."
            );
            Some(DataType::Int16)
        }
        "timestamptz" => Some(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".into()),
        )),
        // Lowercase aliases for common Arrow primitives.
        "i8" => Some(DataType::Int8),
        "int2" | "int16" | "i16" => Some(DataType::Int16),
        "int4" | "int32" | "i32" => Some(DataType::Int32),
        "int8" | "int64" | "i64" => Some(DataType::Int64),
        "uint8" | "u8" => Some(DataType::UInt8),
        "uint16" | "u16" => Some(DataType::UInt16),
        "uint32" | "u32" => Some(DataType::UInt32),
        "uint64" | "u64" => Some(DataType::UInt64),
        "float4" | "float32" | "f32" => Some(DataType::Float32),
        "float8" | "float64" | "f64" => Some(DataType::Float64),
        "uuid" | "utf8" => Some(DataType::Utf8),
        "largeutf8" | "large_utf8" => Some(DataType::LargeUtf8),
        "binary" => Some(DataType::Binary),
        "largebinary" | "large_binary" => Some(DataType::LargeBinary),
        "boolean" | "bool" => Some(DataType::Boolean),
        "date32" => Some(DataType::Date32),
        "date64" => Some(DataType::Date64),
        _ => None,
    }
}

fn parse_via_sqlparser(input: &str) -> Result<DataType, ParseTypeError> {
    let dialect = PostgreSqlDialect {};
    let mut parser =
        Parser::new(&dialect)
            .try_with_sql(input)
            .map_err(|_| ParseTypeError::Unrecognized {
                input: input.to_string(),
            })?;
    let sql_dt = parser
        .parse_data_type()
        .map_err(|_| ParseTypeError::Unrecognized {
            input: input.to_string(),
        })?;
    // Reject inputs with trailing tokens after a successful parse,
    // otherwise sqlparser silently accepts e.g. `Int64<>` as `Int64`.
    if !matches!(parser.peek_token().token, Token::EOF) {
        return Err(ParseTypeError::Unrecognized {
            input: input.to_string(),
        });
    }
    sql_to_arrow_type(&sql_dt).ok_or_else(|| ParseTypeError::Unrecognized {
        input: input.to_string(),
    })
}

fn sql_to_arrow_type(sql: &SqlDataType) -> Option<DataType> {
    Some(match sql {
        // Integers
        SqlDataType::TinyInt(_) | SqlDataType::Int8(_) => DataType::Int8,
        SqlDataType::SmallInt(_) | SqlDataType::Int2(_) => DataType::Int16,
        SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::Int4(_) => DataType::Int32,
        SqlDataType::BigInt(_) | SqlDataType::Int64 => DataType::Int64,
        SqlDataType::Unsigned | SqlDataType::UInt32 => DataType::UInt32,
        SqlDataType::UInt8 => DataType::UInt8,
        SqlDataType::UInt16 => DataType::UInt16,
        SqlDataType::UInt64 => DataType::UInt64,

        // Booleans
        SqlDataType::Boolean | SqlDataType::Bool => DataType::Boolean,

        // Floats
        SqlDataType::Float(_) | SqlDataType::Float4 | SqlDataType::Float32 | SqlDataType::Real => {
            DataType::Float32
        }
        SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float64
        | SqlDataType::Double(_) => DataType::Float64,

        // Decimals / numerics
        SqlDataType::Decimal(info)
        | SqlDataType::Numeric(info)
        | SqlDataType::Dec(info)
        | SqlDataType::BigDecimal(info)
        | SqlDataType::BigNumeric(info) => match info {
            ExactNumberInfo::None => DataType::Decimal128(38, 10),
            ExactNumberInfo::Precision(p) => DataType::Decimal128(u8::try_from(*p).ok()?, 0),
            ExactNumberInfo::PrecisionAndScale(p, s) => {
                DataType::Decimal128(u8::try_from(*p).ok()?, i8::try_from(*s).ok()?)
            }
        },

        // Strings
        SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::CharVarying(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::String(_)
        | SqlDataType::Nvarchar(_)
        | SqlDataType::JSON
        | SqlDataType::JSONB
        | SqlDataType::Uuid => DataType::Utf8,

        // Binary
        SqlDataType::Binary(_)
        | SqlDataType::Varbinary(_)
        | SqlDataType::Blob(_)
        | SqlDataType::Bytea
        | SqlDataType::Bytes(_) => DataType::Binary,

        // Date / time
        SqlDataType::Date | SqlDataType::Date32 => DataType::Date32,
        SqlDataType::Time(_, _) => DataType::Time64(TimeUnit::Microsecond),
        SqlDataType::Datetime(_) => DataType::Timestamp(TimeUnit::Nanosecond, None),
        SqlDataType::Timestamp(_, tz) => match tz {
            TimezoneInfo::Tz | TimezoneInfo::WithTimeZone => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            }
            TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
        },
        SqlDataType::Interval { .. } => DataType::Interval(IntervalUnit::MonthDayNano),

        // Arrays via SQL (e.g. `INT ARRAY`, `TEXT ARRAY`).
        SqlDataType::Array(spec) => {
            use datafusion::sql::sqlparser::ast::ArrayElemTypeDef;
            let element = match spec {
                ArrayElemTypeDef::AngleBracket(inner)
                | ArrayElemTypeDef::SquareBracket(inner, _)
                | ArrayElemTypeDef::Parenthesis(inner) => sql_to_arrow_type(inner)?,
                ArrayElemTypeDef::None => return None,
            };
            list_of(element)
        }

        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> DataType {
        parse_declared_type(input)
            .unwrap_or_else(|e| panic!("expected `{input}` to parse, got error: {e}"))
    }

    fn fail(input: &str) {
        match parse_declared_type(input) {
            Err(_) => {}
            Ok(dt) => panic!("expected `{input}` to fail to parse, got {dt:?}"),
        }
    }

    #[test]
    fn postgres_integer_aliases() {
        assert_eq!(parse("int2"), DataType::Int16);
        assert_eq!(parse("INT2"), DataType::Int16);
        assert_eq!(parse("int4"), DataType::Int32);
        assert_eq!(parse("int8"), DataType::Int64);
        assert_eq!(parse("smallint"), DataType::Int16);
        assert_eq!(parse("integer"), DataType::Int32);
        assert_eq!(parse("INT"), DataType::Int32);
        assert_eq!(parse("bigint"), DataType::Int64);
        assert_eq!(parse("BIGINT"), DataType::Int64);
    }

    #[test]
    fn postgres_float_aliases() {
        assert_eq!(parse("float4"), DataType::Float32);
        assert_eq!(parse("float8"), DataType::Float64);
        assert_eq!(parse("real"), DataType::Float32);
        assert_eq!(parse("double precision"), DataType::Float64);
    }

    #[test]
    fn postgres_serial_logs_and_maps() {
        assert_eq!(parse("serial"), DataType::Int32);
        assert_eq!(parse("bigserial"), DataType::Int64);
        assert_eq!(parse("smallserial"), DataType::Int16);
    }

    #[test]
    fn postgres_string_types() {
        assert_eq!(parse("text"), DataType::Utf8);
        assert_eq!(parse("TEXT"), DataType::Utf8);
        assert_eq!(parse("varchar(255)"), DataType::Utf8);
        assert_eq!(parse("varchar"), DataType::Utf8);
        assert_eq!(parse("char(10)"), DataType::Utf8);
        assert_eq!(parse("uuid"), DataType::Utf8);
        assert_eq!(parse("jsonb"), DataType::Utf8);
        assert_eq!(parse("json"), DataType::Utf8);
    }

    #[test]
    fn postgres_binary_types() {
        assert_eq!(parse("bytea"), DataType::Binary);
    }

    #[test]
    fn postgres_boolean_aliases() {
        assert_eq!(parse("boolean"), DataType::Boolean);
        assert_eq!(parse("bool"), DataType::Boolean);
    }

    #[test]
    fn postgres_decimals() {
        assert_eq!(parse("numeric(18,4)"), DataType::Decimal128(18, 4));
        assert_eq!(parse("numeric(18, 4)"), DataType::Decimal128(18, 4));
        assert_eq!(parse("decimal(10, 2)"), DataType::Decimal128(10, 2));
        assert_eq!(parse("decimal(5)"), DataType::Decimal128(5, 0));
    }

    #[test]
    fn postgres_timestamps() {
        // Postgres `timestamptz` / `timestamp with time zone` are
        // returned by the Postgres connector as `Nanosecond, UTC`,
        // so the declared-type parser uses the same precision to
        // avoid spurious schema mismatches in the deferred
        // registration path.
        assert_eq!(
            parse("timestamptz"),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(
            parse("timestamp with time zone"),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(
            parse("timestamp"),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(parse("date"), DataType::Date32);
    }

    #[test]
    fn postgres_arrays() {
        let expected = DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        )));
        assert_eq!(parse("text[]"), expected);
        assert_eq!(parse("TEXT []"), expected);
    }

    #[test]
    fn postgres_nested_arrays() {
        let inner = DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Int64,
            true,
        )));
        let outer = DataType::List(std::sync::Arc::new(Field::new("item", inner, true)));
        assert_eq!(parse("bigint[][]"), outer);
    }

    #[test]
    fn arrow_display_forms() {
        assert_eq!(parse("Int64"), DataType::Int64);
        assert_eq!(parse("Utf8"), DataType::Utf8);
        assert_eq!(parse("Float64"), DataType::Float64);
        assert_eq!(parse("Boolean"), DataType::Boolean);
        assert_eq!(parse("Date32"), DataType::Date32);
        assert_eq!(parse("Binary"), DataType::Binary);
    }

    #[test]
    fn arrow_timestamp_display_with_timezone() {
        assert_eq!(
            parse("Timestamp(Nanosecond, UTC)"),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(
            parse("Timestamp(Microsecond, \"UTC\")"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            parse("Timestamp(Millisecond, +05:30)"),
            DataType::Timestamp(TimeUnit::Millisecond, Some("+05:30".into()))
        );
        assert_eq!(
            parse("Timestamp(Second, None)"),
            DataType::Timestamp(TimeUnit::Second, None)
        );
    }

    #[test]
    fn arrow_time_display_units() {
        assert_eq!(parse("Time32(Second)"), DataType::Time32(TimeUnit::Second));
        assert_eq!(
            parse("Time64(Microsecond)"),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            parse("Time64(Nanosecond)"),
            DataType::Time64(TimeUnit::Nanosecond)
        );
    }

    #[test]
    fn arrow_decimal_display() {
        assert_eq!(parse("Decimal128(18, 4)"), DataType::Decimal128(18, 4));
    }

    #[test]
    fn arrow_list_display() {
        let expected = DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Int64,
            true,
        )));
        assert_eq!(parse("List<Int64>"), expected);
    }

    fn map_of_test(key: DataType, value: DataType) -> DataType {
        map_of(key, value)
    }

    #[test]
    fn map_uppercase() {
        assert_eq!(
            parse("Map<Utf8, Int64>"),
            map_of_test(DataType::Utf8, DataType::Int64)
        );
    }

    #[test]
    fn map_lowercase() {
        assert_eq!(
            parse("map<utf8, int64>"),
            map_of_test(DataType::Utf8, DataType::Int64)
        );
    }

    #[test]
    fn map_postgres_types_inside() {
        assert_eq!(
            parse("Map<text, bigint>"),
            map_of_test(DataType::Utf8, DataType::Int64)
        );
    }

    #[test]
    fn map_nested_value() {
        let inner_list = DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Int64,
            true,
        )));
        assert_eq!(
            parse("Map<Utf8, List<Int64>>"),
            map_of_test(DataType::Utf8, inner_list)
        );
    }

    #[test]
    fn whitespace_tolerated() {
        assert_eq!(parse("  bigint  "), DataType::Int64);
        assert_eq!(
            parse("  Map<  Utf8 ,  Int64  >  "),
            map_of_test(DataType::Utf8, DataType::Int64)
        );
    }

    #[test]
    fn rejects_garbage() {
        fail("");
        fail("not_a_type");
        fail("Map<>");
        fail("Map<Utf8>");
        fail("Map<Utf8, Int64");
        fail("Int64<>");
        fail("Int64 garbage");
        fail("@@@");
    }

    #[test]
    fn rejects_invalid_array_element() {
        fail("not_a_type[]");
    }
}
