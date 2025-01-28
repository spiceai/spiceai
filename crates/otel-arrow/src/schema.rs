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

use std::{cell::LazyCell, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

#[must_use]
pub fn schema() -> Arc<Schema> {
    METRICS_SCHEMA.with(|schema| Arc::clone(schema))
}

thread_local! {
    /// This is a flattened schema based on the OpenTelemetry Metrics protobuf.
    /// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
    ///
    /// The flattened schema is also loosely based on https://github.com/open-telemetry/otel-arrow.
    /// The main difference between otel-arrow and this schema, is that this schema combines the attributes and data
    /// into a single table. otel-arrow separates each metric type and attributes into a separate
    /// stream of Arrow records for better performance over the wire, at the cost of implementation complexity and a
    /// combined representation.
    ///
    /// This format is suitable for storage in a single table, where each row represents a single metric. It can be losslessly
    /// converted to the hierarchical OTel Protobuf schema and into the otel-arrow schema.
    ///
    /// The hierarchical OTel Protobuf schema this flattened view is based on is:
    /// ResourceMetrics
    ///   ├── Resource
    ///      ├── Attributes
    ///      └── DroppedAttributesCount
    ///   ├── SchemaURL
    ///   └── ScopeMetrics
    ///      ├── Scope
    ///        ├── Name
    ///        ├── Version
    ///        ├── Attributes
    ///        └── DroppedAttributesCount
    ///      ├── SchemaURL
    ///      └── Metric
    ///         ├── Name
    ///         ├── Description
    ///         ├── Unit
    ///         └── data
    ///            ├── Gauge
    ///              ├── NumberDataPoint
    ///                ├── StartTimeUnixNano
    ///                ├── TimeUnixNano
    ///                ├── Value
    ///                ├── Flags
    ///                └── Attributes
    ///            ├── Sum
    ///              ├── NumberDataPoint
    ///                ├── StartTimeUnixNano
    ///                ├── TimeUnixNano
    ///                ├── Value
    ///                ├── Flags
    ///                └── Attributes
    ///              ├── IsMonotonic
    ///              └── AggregationTemporality
    ///            └── Histogram
    ///              ├── HistogramDataPoint
    ///                ├── StartTimeUnixNano
    ///                ├── TimeUnixNano
    ///                ├── Count
    ///                ├── Sum
    ///                ├── BucketCounts
    ///                ├── ExplicitBounds
    ///                ├── Flags
    ///                ├── Min
    ///                ├── Max
    ///                └── Attributes
    ///              └── AggregationTemporality
    static METRICS_SCHEMA: LazyCell<Arc<Schema>> = LazyCell::new(|| {
        let fields = vec![
            Field::new("time_unix_nano", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
            Field::new(
                "start_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("resource", DataType::Struct(
                resource_fields()
                .into(),
            ), true),
            Field::new("scope", DataType::Struct(
                scope_fields()
                .into(),
            ), true),
            Field::new("schema_url", DataType::Utf8, true),
            // MetricType is the Rust enum corresponding to the UInt8 `metric_type`.
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("unit", DataType::Utf8, true),
            Field::new("aggregation_temporality", DataType::Int32, true),
            Field::new("is_monotonic", DataType::Boolean, true),
            Field::new("flags", DataType::UInt32, true),
            Field::new("attributes", attributes_list_type(), true),

            // Only one of these will be set, based on the metric_type.
            // Gauge and Sum will use data_number, Histogram will use data_histogram.
            Field::new("data_number", number_data_type(), true),
            Field::new("data_histogram", histogram_data_type(), true),
        ];
        Arc::new(Schema::new(fields))
    });
}

pub(crate) fn resource_fields() -> Vec<Field> {
    vec![
        Field::new("schema_url", DataType::Utf8, true),
        Field::new("attributes", attributes_list_type(), true),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
    ]
}

pub(crate) fn scope_fields() -> Vec<Field> {
    vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
        Field::new("attributes", attributes_list_type(), true),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
    ]
}

pub(crate) fn attribute_struct_fields() -> Vec<Field> {
    vec![
        Field::new("key", DataType::Utf8, false),
        // AttributeValueType is the Rust enum corresponding to the UInt8 `type`.
        Field::new("type", DataType::UInt8, false),
        Field::new("str", DataType::Utf8, true),
        Field::new("int", DataType::Int64, true),
        Field::new("double", DataType::Float64, true),
        Field::new("bool", DataType::Boolean, true),
        Field::new("bytes", DataType::Binary, true),
        // cbor encoded map
        Field::new("ser", DataType::Binary, true),
    ]
}

pub(crate) fn attribute_list_field() -> Field {
    Field::new(
        "attributes",
        DataType::Struct(attribute_struct_fields().into()),
        false,
    )
}

pub(crate) fn attributes_list_type() -> DataType {
    DataType::List(Arc::new(attribute_list_field()))
}

pub(crate) fn number_fields() -> Vec<Field> {
    vec![
        Field::new("int_value", DataType::Int64, true),
        Field::new("double_value", DataType::Float64, true),
    ]
}

pub(crate) fn number_data_type() -> DataType {
    DataType::Struct(number_fields().into())
}

pub(crate) fn histogram_data_fields() -> Vec<Field> {
    vec![
        Field::new("count", DataType::UInt64, true),
        Field::new("sum", DataType::Float64, true),
        Field::new(
            "bucket_counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        ),
        Field::new(
            "explicit_bounds",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        ),
        Field::new("min", DataType::Float64, true),
        Field::new("max", DataType::Float64, true),
    ]
}

pub(crate) fn histogram_data_type() -> DataType {
    DataType::Struct(histogram_data_fields().into())
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum MetricType {
    Gauge = 1,
    Sum = 2,
    Histogram = 3,
}

impl MetricType {
    #[must_use]
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    #[must_use]
    pub fn from_u8(value: u8) -> Option<MetricType> {
        match value {
            1 => Some(MetricType::Gauge),
            2 => Some(MetricType::Sum),
            3 => Some(MetricType::Histogram),
            _ => None,
        }
    }
}

impl std::fmt::Display for MetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MetricType::Gauge => "Gauge",
            MetricType::Sum => "Sum",
            MetricType::Histogram => "Histogram",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum AttributeValueType {
    Str = 1,
    Int = 2,
    Double = 3,
    Bool = 4,
    Map = 5,
    Slice = 6,
    Bytes = 7,
    Unknown = 255, // u8::MAX
}

impl AttributeValueType {
    #[must_use]
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    #[must_use]
    pub fn from_u8(value: u8) -> Option<AttributeValueType> {
        match value {
            1 => Some(AttributeValueType::Str),
            2 => Some(AttributeValueType::Int),
            3 => Some(AttributeValueType::Double),
            4 => Some(AttributeValueType::Bool),
            5 => Some(AttributeValueType::Map),
            6 => Some(AttributeValueType::Slice),
            7 => Some(AttributeValueType::Bytes),
            255 => Some(AttributeValueType::Unknown),
            _ => None,
        }
    }
}

impl std::fmt::Display for AttributeValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            AttributeValueType::Str => "Str",
            AttributeValueType::Int => "Int",
            AttributeValueType::Double => "Double",
            AttributeValueType::Bool => "Bool",
            AttributeValueType::Map => "Map",
            AttributeValueType::Slice => "Slice",
            AttributeValueType::Bytes => "Bytes",
            AttributeValueType::Unknown => "Unknown",
        };
        write!(f, "{s}")
    }
}

pub(crate) fn temporality_to_i32(temporality: opentelemetry_sdk::metrics::Temporality) -> i32 {
    // Based on https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto#L278
    match temporality {
        opentelemetry_sdk::metrics::Temporality::Delta => 1,
        opentelemetry_sdk::metrics::Temporality::Cumulative => 2,
        _ => panic!("Invalid temporality"),
    }
}
