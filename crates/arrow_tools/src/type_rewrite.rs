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

use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};

/// A rewrite rule applied by [`apply_rules`] to every [`DataType`] node in a schema.
///
/// Rules are applied post-order: children are rewritten before the parent sees them.
pub trait TypeRewriteRule: Send + Sync {
    /// Return `Some(replacement)` when the rule applies, `None` to leave the type unchanged.
    fn rewrite(&self, dt: &DataType) -> Option<DataType>;
}

/// Rewrites `DataType::Dictionary(_, value_type)` → `value_type` (recursively).
///
/// Used by accelerators (`DuckDB`, `SQLite`, Turso) that do not support Arrow Dictionary encoding.
pub struct DictionaryUnwrap;
impl TypeRewriteRule for DictionaryUnwrap {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Dictionary(_, value_type) => Some(value_type.as_ref().clone()),
            _ => None,
        }
    }
}

/// Rewrites `DataType::Null` → `DataType::Int32`.
///
/// `DuckDB` has no Null type and silently coerces it to INT32 when creating tables.
pub struct NullToInt32;
impl TypeRewriteRule for NullToInt32 {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        matches!(dt, DataType::Null).then_some(DataType::Int32)
    }
}

/// Rewrites `DataType::Interval(YearMonth | DayTime)` → `DataType::Interval(MonthDayNano)`.
///
/// `DuckDB`'s native INTERVAL storage uses the `MonthDayNano` layout.
pub struct IntervalToMonthDayNano;
impl TypeRewriteRule for IntervalToMonthDayNano {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime) => {
                Some(DataType::Interval(IntervalUnit::MonthDayNano))
            }
            _ => None,
        }
    }
}

/// Rewrites `DataType::Timestamp(unit, tz)` with a non-microsecond `unit` →
/// `DataType::Timestamp(Microsecond, tz)`, preserving the timezone.
///
/// `DuckDB`'s `TIMESTAMP` and `TIMESTAMP WITH TIME ZONE` types are always
/// microsecond-precision. The Arrow→`DuckDB` column-type mapping in
/// `datafusion-table-providers` collapses every `Timestamp` to `TIMESTAMP` /
/// `TIMESTAMPTZ` regardless of the source `TimeUnit`, so a column registered as
/// `Timestamp(Nanosecond, "UTC")` (e.g. a Postgres `timestamptz` source) is
/// physically stored — and read back — as `Timestamp(Microsecond, "UTC")`. When
/// the registered schema keeps the original non-µs unit, `DataFusion` plans
/// against nanoseconds while `DuckDB` returns microseconds and queries that sort
/// or range-join on the column panic with
/// `RowConverter column schema mismatch, expected Timestamp(ns, "UTC") got Timestamp(µs, "UTC")`.
/// Normalizing the unit to microsecond keeps the registered schema in lockstep
/// with what `DuckDB` stores and returns.
pub struct TimestampToMicrosecond;
impl TypeRewriteRule for TimestampToMicrosecond {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Timestamp(unit, tz) if *unit != TimeUnit::Microsecond => {
                Some(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))
            }
            _ => None,
        }
    }
}

/// Applies `rules` to every type in `schema`, descending post-order into nested container types.
///
/// Returns a new schema with the same metadata; fields whose types are unchanged are cloned as-is.
#[must_use]
pub fn apply_rules(schema: &Schema, rules: &[&dyn TypeRewriteRule]) -> Schema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let new_type = rewrite_data_type(f.data_type(), rules);
            if &new_type == f.data_type() {
                f.as_ref().clone()
            } else {
                f.as_ref().clone().with_data_type(new_type)
            }
        })
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// Replaces Arrow `Dictionary`-encoded fields with the dictionary's value type.
///
/// Data accelerators such as `DuckDB` and `SQLite` do not natively support Arrow
/// Dictionary types.  By unpacking the dictionary encoding at the schema level
/// (and later casting the data via `arrow_cast::cast`), the downstream
/// accelerator receives only primitive types it can handle.
///
/// For example, `Dictionary(Int32, Utf8)` is normalised to `Utf8`.
#[must_use]
pub fn normalize_dictionary_types(schema: &Schema) -> Schema {
    apply_rules(schema, &[&DictionaryUnwrap])
}

/// Post-order recursive type rewriter: children are rewritten before the parent.
fn rewrite_data_type(dt: &DataType, rules: &[&dyn TypeRewriteRule]) -> DataType {
    let dt = match dt {
        DataType::Dictionary(key_type, value_type) => {
            let inner = rewrite_data_type(value_type.as_ref(), rules);
            DataType::Dictionary(key_type.clone(), Box::new(inner))
        }
        DataType::List(field) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::List(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::LargeList(field) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::LargeList(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::FixedSizeList(field, size) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::FixedSizeList(
                Arc::new(field.as_ref().clone().with_data_type(inner)),
                *size,
            )
        }
        DataType::ListView(field) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::ListView(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::LargeListView(field) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::LargeListView(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::Map(field, sorted) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::Map(
                Arc::new(field.as_ref().clone().with_data_type(inner)),
                *sorted,
            )
        }
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields
                .iter()
                .map(|f| {
                    let inner = rewrite_data_type(f.data_type(), rules);
                    f.as_ref().clone().with_data_type(inner)
                })
                .collect();
            DataType::Struct(new_fields.into())
        }
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(type_id, f)| {
                    let inner = rewrite_data_type(f.data_type(), rules);
                    (type_id, Arc::new(f.as_ref().clone().with_data_type(inner)))
                })
                .collect(),
            *mode,
        ),
        DataType::RunEndEncoded(run_ends, values) => {
            let inner = rewrite_data_type(values.data_type(), rules);
            DataType::RunEndEncoded(
                Arc::clone(run_ends),
                Arc::new(values.as_ref().clone().with_data_type(inner)),
            )
        }
        other => other.clone(),
    };
    for rule in rules {
        if let Some(rewritten) = rule.rewrite(&dt) {
            return rewritten;
        }
    }
    dt
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, IntervalUnit, Schema, UnionFields, UnionMode};

    #[test]
    fn null_to_int32_top_level() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("untyped", DataType::Null, true),
            Field::new("name", DataType::Utf8, false),
        ]);
        let result = apply_rules(&schema, &[&NullToInt32]);
        assert_eq!(result.field(0).data_type(), &DataType::Int64);
        assert_eq!(result.field(1).data_type(), &DataType::Int32);
        assert_eq!(result.field(2).data_type(), &DataType::Utf8);
    }

    #[test]
    fn null_to_int32_nested_in_list() {
        let schema = Schema::new(vec![Field::new(
            "col",
            DataType::List(Arc::new(Field::new("item", DataType::Null, true))),
            false,
        )]);
        let result = apply_rules(&schema, &[&NullToInt32]);
        let expected = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert_eq!(result.field(0).data_type(), &expected);
    }

    #[test]
    fn null_to_int32_nested_in_struct() {
        let schema = Schema::new(vec![Field::new(
            "rec",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("val", DataType::Null, true),
                ]
                .into(),
            ),
            false,
        )]);
        let result = apply_rules(&schema, &[&NullToInt32]);
        let expected = DataType::Struct(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("val", DataType::Int32, true),
            ]
            .into(),
        );
        assert_eq!(result.field(0).data_type(), &expected);
    }

    #[test]
    fn interval_year_month_normalized() {
        let schema = Schema::new(vec![Field::new(
            "dur",
            DataType::Interval(IntervalUnit::YearMonth),
            true,
        )]);
        let result = apply_rules(&schema, &[&IntervalToMonthDayNano]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn interval_day_time_normalized() {
        let schema = Schema::new(vec![Field::new(
            "dur",
            DataType::Interval(IntervalUnit::DayTime),
            true,
        )]);
        let result = apply_rules(&schema, &[&IntervalToMonthDayNano]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn interval_month_day_nano_unchanged() {
        let schema = Schema::new(vec![Field::new(
            "dur",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        )]);
        let result = apply_rules(&schema, &[&IntervalToMonthDayNano]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn timestamp_nanosecond_with_tz_normalized_to_microsecond() {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )]);
        let result = apply_rules(&schema, &[&TimestampToMicrosecond]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "timezone must be preserved while the unit is normalized to microsecond"
        );
    }

    #[test]
    fn timestamp_nanosecond_without_tz_normalized_to_microsecond() {
        // DuckDB maps `Timestamp(_, None)` to its microsecond `TIMESTAMP` type
        // regardless of the source unit, so the no-timezone case must normalize too.
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]);
        let result = apply_rules(&schema, &[&TimestampToMicrosecond]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn timestamp_second_and_millisecond_normalized_to_microsecond() {
        let schema = Schema::new(vec![
            Field::new("s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+05:00".into())),
                true,
            ),
        ]);
        let result = apply_rules(&schema, &[&TimestampToMicrosecond]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            result.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+05:00".into()))
        );
    }

    #[test]
    fn timestamp_microsecond_unchanged() {
        let schema = Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("id", DataType::Int64, false),
        ]);
        let result = apply_rules(&schema, &[&TimestampToMicrosecond]);
        assert_eq!(result, schema, "already-microsecond schema must be a no-op");
    }

    #[test]
    fn timestamp_nanosecond_nested_in_list_and_struct_normalized() {
        let schema = Schema::new(vec![
            Field::new(
                "events",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    true,
                ))),
                false,
            ),
            Field::new(
                "rec",
                DataType::Struct(
                    vec![Field::new(
                        "at",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    )]
                    .into(),
                ),
                false,
            ),
        ]);
        let result = apply_rules(&schema, &[&TimestampToMicrosecond]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            )))
        );
        assert_eq!(
            result.field(1).data_type(),
            &DataType::Struct(
                vec![Field::new(
                    "at",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )]
                .into(),
            )
        );
    }

    #[test]
    fn multiple_rules_applied_in_order() {
        let schema = Schema::new(vec![
            Field::new("untyped", DataType::Null, true),
            Field::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("dur", DataType::Interval(IntervalUnit::DayTime), true),
        ]);
        let result = apply_rules(
            &schema,
            &[&DictionaryUnwrap, &NullToInt32, &IntervalToMonthDayNano],
        );
        assert_eq!(result.field(0).data_type(), &DataType::Int32);
        assert_eq!(result.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            result.field(2).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn apply_rules_preserves_metadata() {
        use std::collections::HashMap;
        let mut meta = HashMap::new();
        meta.insert("key".to_string(), "val".to_string());
        let mut fmeta = HashMap::new();
        fmeta.insert("fkey".to_string(), "fval".to_string());
        let schema = Schema::new_with_metadata(
            vec![Field::new("x", DataType::Null, true).with_metadata(fmeta.clone())],
            meta.clone(),
        );
        let result = apply_rules(&schema, &[&NullToInt32]);
        assert_eq!(result.metadata(), &meta);
        assert_eq!(result.field(0).metadata(), &fmeta);
        assert_eq!(result.field(0).data_type(), &DataType::Int32);
    }

    #[test]
    fn noop_when_no_rules_match() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let result = apply_rules(&schema, &[&NullToInt32]);
        assert_eq!(result, schema);
    }

    #[test]
    fn dictionary_inside_dict_unwrap_then_null_rule() {
        // Dictionary(Int32, Null) — after DictionaryUnwrap → Null — after NullToInt32 → Int32
        let schema = Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Null)),
            true,
        )]);
        let result = apply_rules(&schema, &[&DictionaryUnwrap, &NullToInt32]);
        assert_eq!(result.field(0).data_type(), &DataType::Int32);
    }

    #[test]
    fn interval_nested_in_union() {
        let union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("i32", DataType::Int32, false),
                Field::new("dur", DataType::Interval(IntervalUnit::YearMonth), true),
            ],
        )
        .expect("union fields");
        let schema = Schema::new(vec![Field::new(
            "mixed",
            DataType::Union(union_fields, UnionMode::Dense),
            false,
        )]);
        let result = apply_rules(&schema, &[&IntervalToMonthDayNano]);
        let expected_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("i32", DataType::Int32, false),
                Field::new("dur", DataType::Interval(IntervalUnit::MonthDayNano), true),
            ],
        )
        .expect("expected union fields");
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Union(expected_fields, UnionMode::Dense)
        );
    }

    #[test]
    fn test_normalize_dictionary_types() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "category",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8)),
                false,
            ),
        ]);
        let normalized = normalize_dictionary_types(&schema);
        assert_eq!(normalized.field(0).data_type(), &DataType::Int64);
        assert_eq!(normalized.field(1).data_type(), &DataType::Utf8);
        assert!(normalized.field(1).is_nullable());
        assert_eq!(normalized.field(2).data_type(), &DataType::Utf8);
        assert_eq!(normalized.field(3).data_type(), &DataType::LargeUtf8);
        assert!(!normalized.field(3).is_nullable());
    }

    #[test]
    fn test_normalize_schema_without_dictionary_is_noop() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let normalized = normalize_dictionary_types(&schema);
        assert_eq!(schema, normalized);
    }

    #[test]
    fn test_normalize_preserves_metadata() {
        use std::collections::HashMap;
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());
        let mut field_metadata = HashMap::new();
        field_metadata.insert("custom_key".to_string(), "custom_value".to_string());
        let schema = Schema::new_with_metadata(
            vec![
                Field::new(
                    "col",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                )
                .with_metadata(field_metadata.clone()),
            ],
            metadata.clone(),
        );
        let normalized = normalize_dictionary_types(&schema);
        assert_eq!(normalized.metadata(), &metadata);
        assert_eq!(normalized.field(0).data_type(), &DataType::Utf8);
        assert_eq!(
            normalized.field(0).metadata(),
            &field_metadata,
            "field-level metadata must be preserved after normalization"
        );
    }

    #[test]
    fn test_normalize_nested_dictionary_in_list() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ))),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_struct() {
        let schema = Schema::new(vec![Field::new(
            "record",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new(
                        "status",
                        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                        true,
                    ),
                ]
                .into(),
            ),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::Struct(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("status", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_union() {
        let union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("int_val", DataType::Int32, false),
                Field::new(
                    "dict_val",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                ),
            ],
        )
        .expect("union fields");
        let schema = Schema::new(vec![Field::new(
            "mixed",
            DataType::Union(union_fields, UnionMode::Dense),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected_union = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("int_val", DataType::Int32, false),
                Field::new("dict_val", DataType::Utf8, true),
            ],
        )
        .expect("expected union fields");
        assert_eq!(
            normalized.field(0).data_type(),
            &DataType::Union(expected_union, UnionMode::Dense)
        );
    }

    #[test]
    fn test_normalize_nested_dictionary_in_list_view() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::ListView(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ))),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::ListView(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_large_list_view() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::LargeListView(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8)),
                true,
            ))),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected =
            DataType::LargeListView(Arc::new(Field::new("item", DataType::LargeUtf8, true)));
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_run_end_encoded() {
        let schema = Schema::new(vec![Field::new(
            "encoded",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new(
                    "values",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                )),
            ),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        );
        assert_eq!(normalized.field(0).data_type(), &expected);
    }
}
