/*
Copyright 2026 The Spice.ai OSS Authors

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

use arrow::datatypes::{DataType, Field, TimeUnit};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParseArrowTypeError;

pub(super) fn parse_arrow_type(s: &str) -> Result<DataType, ParseArrowTypeError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(ParseArrowTypeError);
    }

    if let Ok(data_type) = parse_spicepod_arrow_type(trimmed) {
        return Ok(data_type);
    }

    trimmed.parse().map_err(|_| ParseArrowTypeError)
}

fn parse_spicepod_arrow_type(s: &str) -> Result<DataType, ParseArrowTypeError> {
    if let Some(data_type) = parse_scalar_alias(s) {
        return Ok(data_type);
    }

    if let Some(inner) =
        strip_wrapped(s, "list", '<', '>').or_else(|| strip_wrapped(s, "list", '(', ')'))
    {
        return Ok(DataType::List(Arc::new(Field::new_list_field(
            parse_arrow_type(inner)?,
            true,
        ))));
    }

    if let Some(inner) = strip_wrapped(s, "large_list", '<', '>')
        .or_else(|| strip_wrapped(s, "largelist", '<', '>'))
        .or_else(|| strip_wrapped(s, "large_list", '(', ')'))
        .or_else(|| strip_wrapped(s, "largelist", '(', ')'))
    {
        return Ok(DataType::LargeList(Arc::new(Field::new_list_field(
            parse_arrow_type(inner)?,
            true,
        ))));
    }

    if let Some(inner) =
        strip_wrapped(s, "struct", '<', '>').or_else(|| strip_wrapped(s, "struct", '(', ')'))
    {
        return parse_struct(inner);
    }

    if let Some(inner) =
        strip_wrapped(s, "decimal", '(', ')').or_else(|| strip_wrapped(s, "decimal128", '(', ')'))
    {
        let (precision, scale) = parse_decimal_parts(inner)?;
        return Ok(DataType::Decimal128(precision, scale));
    }

    if let Some(inner) = strip_wrapped(s, "decimal256", '(', ')') {
        let (precision, scale) = parse_decimal_parts(inner)?;
        return Ok(DataType::Decimal256(precision, scale));
    }

    if let Some(inner) = strip_wrapped(s, "timestamp", '(', ')') {
        return parse_timestamp(inner);
    }

    Err(ParseArrowTypeError)
}

fn parse_scalar_alias(s: &str) -> Option<DataType> {
    Some(match s.trim().to_ascii_lowercase().as_str() {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" | "int" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" | "float" => DataType::Float32,
        "float64" | "double" => DataType::Float64,
        "utf8" | "string" => DataType::Utf8,
        "large_utf8" | "largeutf8" => DataType::LargeUtf8,
        "boolean" | "bool" => DataType::Boolean,
        "binary" => DataType::Binary,
        "large_binary" | "largebinary" => DataType::LargeBinary,
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        _ => return None,
    })
}

fn parse_struct(inner: &str) -> Result<DataType, ParseArrowTypeError> {
    let trimmed = inner.trim();
    if trimmed.is_empty() {
        return Ok(DataType::Struct(Vec::<Field>::new().into()));
    }

    let fields = split_top_level(trimmed, ',')
        .ok_or(ParseArrowTypeError)?
        .into_iter()
        .map(|part| {
            let (name, data_type) = split_top_level_once(part, ':').ok_or(ParseArrowTypeError)?;
            Ok(Field::new(
                strip_field_name_quotes(name.trim()),
                parse_arrow_type(data_type)?,
                true,
            ))
        })
        .collect::<Result<Vec<_>, ParseArrowTypeError>>()?;

    Ok(DataType::Struct(fields.into()))
}

fn parse_decimal_parts(inner: &str) -> Result<(u8, i8), ParseArrowTypeError> {
    let parts = split_top_level(inner, ',').ok_or(ParseArrowTypeError)?;
    let [precision, scale] = parts.as_slice() else {
        return Err(ParseArrowTypeError);
    };
    Ok((
        precision
            .trim()
            .parse::<u8>()
            .map_err(|_| ParseArrowTypeError)?,
        scale
            .trim()
            .parse::<i8>()
            .map_err(|_| ParseArrowTypeError)?,
    ))
}

fn parse_timestamp(inner: &str) -> Result<DataType, ParseArrowTypeError> {
    let parts = split_top_level(inner, ',').ok_or(ParseArrowTypeError)?;
    let Some(unit) = parts.first() else {
        return Err(ParseArrowTypeError);
    };
    let unit = parse_time_unit(unit.trim()).ok_or(ParseArrowTypeError)?;
    let timezone = match parts.as_slice() {
        [_] => None,
        [_, timezone] => parse_timezone(timezone.trim()),
        _ => return Err(ParseArrowTypeError),
    };
    Ok(DataType::Timestamp(unit, timezone.map(Into::into)))
}

fn parse_time_unit(unit: &str) -> Option<TimeUnit> {
    Some(match unit.to_ascii_lowercase().as_str() {
        "s" | "sec" | "second" => TimeUnit::Second,
        "ms" | "millisecond" => TimeUnit::Millisecond,
        "us" | "microsecond" => TimeUnit::Microsecond,
        "ns" | "nanosecond" => TimeUnit::Nanosecond,
        _ => return None,
    })
}

fn parse_timezone(timezone: &str) -> Option<String> {
    let timezone = timezone.trim();
    if timezone.eq_ignore_ascii_case("none") {
        return None;
    }
    Some(strip_optional_some(strip_string_quotes(timezone)).to_string())
}

fn strip_optional_some(value: &str) -> &str {
    let Some(inner) = strip_wrapped(value, "some", '(', ')') else {
        return value;
    };
    strip_string_quotes(inner.trim())
}

fn strip_field_name_quotes(value: &str) -> String {
    strip_string_quotes(value).to_string()
}

fn strip_string_quotes(value: &str) -> &str {
    let value = value.trim();
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\''))
            || (value.starts_with('`') && value.ends_with('`')))
    {
        &value[1..value.len() - 1]
    } else {
        value
    }
}

fn strip_wrapped<'a>(s: &'a str, prefix: &str, open: char, close: char) -> Option<&'a str> {
    let trimmed = s.trim();
    if trimmed.len() < prefix.len() || !trimmed[..prefix.len()].eq_ignore_ascii_case(prefix) {
        return None;
    }
    let rest = trimmed[prefix.len()..].trim();
    if !rest.starts_with(open) || !rest.ends_with(close) {
        return None;
    }
    let inner = &rest[open.len_utf8()..rest.len() - close.len_utf8()];
    is_balanced(inner).then_some(inner)
}

fn split_top_level(input: &str, separator: char) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut state = SplitState::default();
    for (idx, ch) in input.char_indices() {
        state.advance(ch)?;
        if ch == separator && state.is_top_level() {
            parts.push(input[start..idx].trim());
            start = idx + ch.len_utf8();
        }
    }
    state.is_balanced().then(|| {
        parts.push(input[start..].trim());
        parts
    })
}

fn split_top_level_once(input: &str, separator: char) -> Option<(&str, &str)> {
    let mut state = SplitState::default();
    for (idx, ch) in input.char_indices() {
        state.advance(ch)?;
        if ch == separator && state.is_top_level() {
            return Some((input[..idx].trim(), input[idx + ch.len_utf8()..].trim()));
        }
    }
    None
}

fn is_balanced(input: &str) -> bool {
    let mut state = SplitState::default();
    for ch in input.chars() {
        if state.advance(ch).is_none() {
            return false;
        }
    }
    state.is_balanced()
}

#[derive(Default)]
struct SplitState {
    parens: i32,
    angles: i32,
    brackets: i32,
    quote: Option<char>,
}

impl SplitState {
    fn advance(&mut self, ch: char) -> Option<()> {
        if let Some(quote) = self.quote {
            if ch == quote {
                self.quote = None;
            }
            return Some(());
        }

        match ch {
            '"' | '\'' | '`' => self.quote = Some(ch),
            '(' => self.parens += 1,
            ')' => self.parens -= 1,
            '<' => self.angles += 1,
            '>' => self.angles -= 1,
            '[' => self.brackets += 1,
            ']' => self.brackets -= 1,
            _ => {}
        }

        (self.parens >= 0 && self.angles >= 0 && self.brackets >= 0).then_some(())
    }

    fn is_top_level(&self) -> bool {
        self.quote.is_none() && self.parens == 0 && self.angles == 0 && self.brackets == 0
    }

    fn is_balanced(&self) -> bool {
        self.is_top_level()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_primitives_and_aliases() {
        assert_eq!(parse_arrow_type("int64").expect("int64"), DataType::Int64);
        assert_eq!(parse_arrow_type("INT32").expect("int32"), DataType::Int32);
        assert_eq!(parse_arrow_type("string").expect("string"), DataType::Utf8);
        assert_eq!(parse_arrow_type("bool").expect("bool"), DataType::Boolean);
        assert_eq!(
            parse_arrow_type("LargeUtf8").expect("large utf8"),
            DataType::LargeUtf8
        );
    }

    #[test]
    fn parses_complex_spicepod_syntax() {
        assert_eq!(
            parse_arrow_type("list<int64>").expect("list"),
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)))
        );
        assert_eq!(
            parse_arrow_type("large_list<struct<name:utf8, scores:list<float64>>>")
                .expect("large list of struct"),
            DataType::LargeList(Arc::new(Field::new_list_field(
                DataType::Struct(
                    vec![
                        Field::new("name", DataType::Utf8, true),
                        Field::new(
                            "scores",
                            DataType::List(Arc::new(Field::new_list_field(
                                DataType::Float64,
                                true
                            ))),
                            true,
                        ),
                    ]
                    .into()
                ),
                true,
            )))
        );
        assert_eq!(
            parse_arrow_type("decimal(38, 10)").expect("decimal"),
            DataType::Decimal128(38, 10)
        );
        assert_eq!(
            parse_arrow_type("timestamp(us, UTC)").expect("timestamp with timezone"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn parses_arrow_display_syntax() {
        assert_eq!(
            parse_arrow_type("List(Int64)").expect("arrow list"),
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)))
        );
        assert_eq!(
            parse_arrow_type("Struct(\"x\": Int64, \"y\": Utf8)").expect("arrow struct"),
            DataType::Struct(
                vec![
                    Field::new("x", DataType::Int64, true),
                    Field::new("y", DataType::Utf8, true),
                ]
                .into()
            )
        );
    }
}
