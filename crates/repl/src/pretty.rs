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

//! Pretty printing utilities for Arrow `RecordBatch`es with data type display.

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow::util::pretty::pretty_format_batches;
use std::fmt::Write;
use std::sync::Arc;

/// Formats Arrow `RecordBatch`es with data types displayed below column names.
///
/// # Errors
///
/// Returns an error if the record batches cannot be formatted.
pub fn format_batches_with_types(batches: &[RecordBatch]) -> Result<String, ArrowError> {
    if batches.is_empty() {
        return Ok(String::new());
    }

    let schema = batches[0].schema();
    let type_strings: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format_data_type(f.data_type()))
        .collect();

    // Pad column names so Arrow allocates enough width to fit the type strings.
    let padded_batches = with_padded_names_for_types(batches, &type_strings)?;
    let formatted = pretty_format_batches(&padded_batches)?;
    let output = formatted.to_string();

    Ok(insert_type_row(&output, &schema))
}

/// Formats Arrow `RecordBatch`es in expanded view: each row becomes a vertical
/// stack of `column | value` lines, separated by a `-[ RECORD n ]-` divider.
///
/// This mirrors PostgreSQL's `\x` (expanded) display mode and is useful when
/// tables are wider than the terminal.
///
/// # Errors
///
/// Returns an error if any cell value cannot be formatted.
pub fn format_batches_expanded(batches: &[RecordBatch]) -> Result<String, ArrowError> {
    if batches.is_empty() {
        return Ok(String::new());
    }

    let schema = batches[0].schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    // Width of the label column is the widest column name; capped to keep things sane.
    let name_width = field_names.iter().map(|n| n.len()).max().unwrap_or(0);

    let format_opts = FormatOptions::default().with_display_error(true);
    let mut out = String::new();
    let mut record_num: usize = 0;

    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &format_opts))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for row_idx in 0..batch.num_rows() {
            record_num += 1;
            write_record_header(&mut out, record_num, name_width);
            for (col_idx, name) in field_names.iter().enumerate() {
                let value = formatters[col_idx].value(row_idx).to_string();
                write_record_line(&mut out, name, name_width, &value);
            }
        }
    }

    // Trim trailing newline for consistency with `format_batches_with_types`.
    if out.ends_with('\n') {
        out.pop();
    }
    Ok(out)
}

/// Write the `-[ RECORD n ]-...` separator at the start of an expanded record.
fn write_record_header(out: &mut String, record_num: usize, name_width: usize) {
    // Layout: `-[ RECORD <n> ]` then dashes filling out to where the `|` would go,
    // followed by `-` and a newline.  The `|` column for values lands at
    // `name_width + 1`, so the header is at least that wide.
    let label = format!("-[ RECORD {record_num} ]");
    // Always render at least 3 trailing dashes so the divider reads clearly even
    // for narrow column names.
    let min_trailing = 3;
    let target_width = (name_width + 2).max(label.len() + min_trailing);
    let trailing = target_width.saturating_sub(label.len());
    let _ = writeln!(out, "{label}{dashes}", dashes = "-".repeat(trailing));
}

/// Write a single `name | value` row.  Multi-line values are continued on
/// subsequent lines, aligned under the value column.
fn write_record_line(out: &mut String, name: &str, name_width: usize, value: &str) {
    let mut lines = value.split('\n');
    let first = lines.next().unwrap_or("");
    let _ = writeln!(out, "{name:<name_width$} | {first}");
    for cont in lines {
        // Continuation lines: blank name column, still anchored at the `|`.
        let _ = writeln!(out, "{blank:<name_width$} | {cont}", blank = "");
    }
}

/// Returns copies of `batches` whose schema has each column name padded with
/// trailing spaces so that `pretty_format_batches` will allocate a column width
/// at least as wide as the corresponding type string.  The original schema
/// (with unpadded names) is used for the displayed header row.
fn with_padded_names_for_types(
    batches: &[RecordBatch],
    type_strings: &[String],
) -> Result<Vec<RecordBatch>, ArrowError> {
    let schema = batches[0].schema();
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(type_strings.iter())
        .map(|(field, type_str)| {
            let name = field.name();
            let padded_name = if name.len() < type_str.len() {
                format!("{name:<width$}", width = type_str.len())
            } else {
                name.clone()
            };
            Field::new(padded_name, field.data_type().clone(), field.is_nullable())
        })
        .collect();
    let padded_schema = Arc::new(Schema::new(fields));
    batches
        .iter()
        .map(|batch| RecordBatch::try_new(Arc::clone(&padded_schema), batch.columns().to_vec()))
        .collect()
}

/// Insert a type row after the header row in the formatted table,
/// and center the column names.
fn insert_type_row(formatted: &str, schema: &SchemaRef) -> String {
    let lines: Vec<&str> = formatted.lines().collect();

    // Expected format from pretty_format_batches:
    // Line 0: +----+----+   (top border)
    // Line 1: | a  | b  |   (header row)
    // Line 2: +----+----+   (separator)
    // Line 3+: data rows...
    if lines.len() < 3 {
        return formatted.to_string();
    }

    // Parse column widths from the separator line (line 2)
    let separator = lines[2];
    let widths = parse_column_widths(separator);

    if widths.is_empty() || widths.len() != schema.fields().len() {
        // Fall back to original format if we can't parse properly
        return formatted.to_string();
    }

    // Build the centered header row and type row
    let header_row = build_centered_header_row(&widths, schema);
    let type_row = build_type_row(&widths, schema);

    // Reconstruct output with centered header and type row
    let mut result = String::with_capacity(formatted.len() + type_row.len() + 1);
    result.push_str(lines[0]); // top border
    result.push('\n');
    result.push_str(&header_row); // centered header row
    result.push('\n');
    result.push_str(&type_row); // type row (new)
    result.push('\n');

    // Add remaining lines (separator and data)
    for line in &lines[2..] {
        result.push_str(line);
        result.push('\n');
    }

    // Remove trailing newline to match original format
    if result.ends_with('\n') {
        result.pop();
    }

    result
}

/// Parse column widths from a separator line like "+----+------+"
fn parse_column_widths(separator: &str) -> Vec<usize> {
    let mut widths = Vec::new();
    let mut current_width = 0;
    let mut in_column = false;

    for ch in separator.chars() {
        match ch {
            '+' => {
                if in_column {
                    widths.push(current_width);
                    current_width = 0;
                }
                in_column = true;
            }
            '-' => {
                current_width += 1;
            }
            _ => {}
        }
    }

    widths
}

/// Build a header row string with centered column names.
fn build_centered_header_row(widths: &[usize], schema: &SchemaRef) -> String {
    let mut row = String::from("|");

    for (idx, field) in schema.fields().iter().enumerate() {
        let name = field.name();
        let width = widths[idx];

        // Center the column name within the column width without exceeding it; when padding is odd, bias left
        let padding = width.saturating_sub(name.len());
        let left_pad = padding.div_ceil(2);
        let right_pad = padding - left_pad;

        row.push_str(&" ".repeat(left_pad));
        row.push_str(name);
        row.push_str(&" ".repeat(right_pad));
        row.push('|');
    }

    row
}

/// Build a type row string with centered type names.
fn build_type_row(widths: &[usize], schema: &SchemaRef) -> String {
    let mut row = String::from("|");

    for (idx, field) in schema.fields().iter().enumerate() {
        let type_str = format_data_type(field.data_type());
        let width = widths[idx];

        // Center the type string within the column width without exceeding it; when padding is odd, bias left
        let padding = width.saturating_sub(type_str.len());
        let left_pad = padding.div_ceil(2);
        let right_pad = padding - left_pad;

        row.push_str(&" ".repeat(left_pad));
        row.push_str(&type_str);
        row.push_str(&" ".repeat(right_pad));
        row.push('|');
    }

    row
}

/// Format a `DataType` to a user-friendly string representation.
///
/// This produces shorter, more readable type names similar to `DuckDB`'s format.
#[must_use]
pub fn format_data_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "null".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float16 => "float16".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".to_string(),
        DataType::FixedSizeBinary(size) => format!("binary({size})"),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "time".to_string(),
        DataType::Timestamp(unit, tz) => {
            let tz_suffix = tz.as_ref().map_or(String::new(), |tz| format!(" ({tz})"));
            match unit {
                TimeUnit::Second => format!("timestamp[s]{tz_suffix}"),
                TimeUnit::Millisecond => format!("timestamp[ms]{tz_suffix}"),
                TimeUnit::Microsecond => format!("timestamp[us]{tz_suffix}"),
                TimeUnit::Nanosecond => format!("timestamp[ns]{tz_suffix}"),
            }
        }
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => "duration[s]".to_string(),
            TimeUnit::Millisecond => "duration[ms]".to_string(),
            TimeUnit::Microsecond => "duration[us]".to_string(),
            TimeUnit::Nanosecond => "duration[ns]".to_string(),
        },
        DataType::Interval(unit) => format!("interval[{unit:?}]").to_lowercase(),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => {
            format!("{}[]", format_data_type(field.data_type()))
        }
        DataType::FixedSizeList(field, size) => {
            format!("{}[{size}]", format_data_type(field.data_type()))
        }
        DataType::Struct(_) => "struct".to_string(),
        DataType::Union(_, _) => "union".to_string(),
        DataType::Dictionary(key_type, value_type) => {
            format!(
                "dict<{}, {}>",
                format_data_type(key_type),
                format_data_type(value_type)
            )
        }
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("decimal({precision},{scale})"),
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type()
                && fields.len() == 2
            {
                let key_type = format_data_type(fields[0].data_type());
                let value_type = format_data_type(fields[1].data_type());
                return format!("map<{key_type}, {value_type}>");
            }
            "map".to_string()
        }
        DataType::RunEndEncoded(_, values) => format_data_type(values.data_type()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_format_batches_with_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None])),
            ],
        )
        .expect("creating test batch");

        let formatted = format_batches_with_types(&[batch]).expect("formatting should succeed");

        // Verify output contains column names and types
        assert!(
            formatted.contains("id"),
            "output should contain column name"
        );
        assert!(
            formatted.contains("int32"),
            "output should contain data type int32"
        );
        assert!(
            formatted.contains("name"),
            "output should contain column name 'name'"
        );
        assert!(
            formatted.contains("varchar"),
            "output should contain data type varchar"
        );
    }

    #[test]
    fn test_format_empty_batches() {
        let formatted = format_batches_with_types(&[]).expect("formatting should succeed");
        assert!(
            formatted.is_empty(),
            "empty batches should produce empty output"
        );
    }

    #[test]
    fn test_format_data_type() {
        assert_eq!(format_data_type(&DataType::Int64), "int64");
        assert_eq!(format_data_type(&DataType::Utf8), "varchar");
        assert_eq!(format_data_type(&DataType::LargeUtf8), "varchar");
        assert_eq!(format_data_type(&DataType::Boolean), "boolean");
        assert_eq!(format_data_type(&DataType::Date32), "date");
        assert_eq!(
            format_data_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "timestamp[us]"
        );
        assert_eq!(
            format_data_type(&DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("UTC".into())
            )),
            "timestamp[ms] (UTC)"
        );
    }

    #[test]
    fn test_parse_column_widths() {
        let separator = "+----+------+--+";
        let widths = parse_column_widths(separator);
        assert_eq!(widths, vec![4, 6, 2]);
    }

    #[test]
    fn test_format_batches_expanded_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
            ],
        )
        .expect("creating test batch");

        let formatted = format_batches_expanded(&[batch]).expect("formatting should succeed");

        assert!(formatted.contains("-[ RECORD 1 ]"), "missing RECORD 1 header in:\n{formatted}");
        assert!(formatted.contains("-[ RECORD 2 ]"), "missing RECORD 2 header in:\n{formatted}");
        assert!(formatted.contains("id"), "missing column name `id` in:\n{formatted}");
        assert!(formatted.contains("name"), "missing column name `name` in:\n{formatted}");
        assert!(formatted.contains("Alice"), "missing value `Alice` in:\n{formatted}");
        assert!(formatted.contains("Bob"), "missing value `Bob` in:\n{formatted}");
        // Records are separated by the divider line, so RECORD 1's header
        // should appear before RECORD 2's header.
        let p1 = formatted.find("-[ RECORD 1 ]").expect("RECORD 1 header");
        let p2 = formatted.find("-[ RECORD 2 ]").expect("RECORD 2 header");
        assert!(p1 < p2);
    }

    #[test]
    fn test_format_batches_expanded_empty() {
        let formatted = format_batches_expanded(&[]).expect("formatting should succeed");
        assert!(formatted.is_empty());
    }

    #[test]
    fn test_format_batches_expanded_null_value() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![None::<&str>]))],
        )
        .expect("creating test batch");

        let formatted = format_batches_expanded(&[batch]).expect("formatting should succeed");
        assert!(formatted.contains("-[ RECORD 1 ]"));
        assert!(formatted.contains("name"));
    }

    #[test]
    fn test_format_batches_expanded_record_numbering_across_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let mk_batch = |vals: Vec<i32>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vals)) as _],
            )
            .expect("creating test batch")
        };

        let formatted = format_batches_expanded(&[mk_batch(vec![10, 20]), mk_batch(vec![30])])
            .expect("formatting should succeed");

        // Three records total across two batches: numbering must be continuous.
        assert!(formatted.contains("-[ RECORD 1 ]"));
        assert!(formatted.contains("-[ RECORD 2 ]"));
        assert!(formatted.contains("-[ RECORD 3 ]"));
        assert!(!formatted.contains("-[ RECORD 4 ]"));
    }

    #[test]
    fn test_type_wider_than_column_name() {
        // "id" (2 chars) with type "varchar" (7 chars): the type must not overflow its cell.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![Some("1")])),
                Arc::new(arrow::array::StringArray::from(vec![Some("Alice")])),
            ],
        )
        .expect("creating test batch");

        let formatted = format_batches_with_types(&[batch]).expect("formatting should succeed");

        // Every line must have the same length (borders are straight columns).
        let lines: Vec<&str> = formatted.lines().collect();
        let first_len = lines[0].len();
        for (i, line) in lines.iter().enumerate() {
            assert_eq!(
                line.len(),
                first_len,
                "line {i} has different width than line 0:\n{formatted}"
            );
        }
    }
}
