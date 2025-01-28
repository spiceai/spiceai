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

#![allow(clippy::struct_field_names)]

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int32Builder,
    Int64Builder, ListArray, ListBuilder, StringBuilder, StructArray, TimestampNanosecondBuilder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{BufferBuilder, NullBufferBuilder, OffsetBuffer};
use opentelemetry::InstrumentationScope;
use opentelemetry_sdk::metrics::data::{Gauge, Histogram, Metric, ResourceMetrics, Sum};
use opentelemetry_sdk::metrics::MetricError;
use opentelemetry_sdk::Resource;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    attribute_list_field, attribute_struct_fields, histogram_data_fields, number_fields,
    resource_fields, scope_fields, temporality_to_i32,
};

pub struct OtelToArrowConverter {
    time_unix_nano_builder: TimestampNanosecondBuilder,
    start_time_unix_nano_builder: TimestampNanosecondBuilder,
    resource_builder: ResourceBuilder,
    scope_builder: ScopeBuilder,
    schema_url_builder: StringBuilder,
    metric_type_builder: UInt8Builder,
    name_builder: StringBuilder,
    description_builder: StringBuilder,
    unit_builder: StringBuilder,
    aggregation_temporality_builder: Int32Builder,
    is_monotonic_builder: BooleanBuilder,
    flags_builder: UInt32Builder,
    attributes_builder: AttributesBuilder,
    data_number_builder: DataNumberBuilder,
    data_histogram_builder: DataHistogramBuilder,
}

struct DataNumberBuilder {
    int_builder: Int64Builder,
    double_builder: Float64Builder,
    null_buffer_builder: NullBufferBuilder,
}

impl DataNumberBuilder {
    fn with_capacity(capacity: usize) -> Self {
        DataNumberBuilder {
            int_builder: Int64Builder::with_capacity(capacity),
            double_builder: Float64Builder::with_capacity(capacity),
            null_buffer_builder: NullBufferBuilder::new(capacity),
        }
    }

    fn append(&mut self, is_valid: bool) {
        self.null_buffer_builder.append(is_valid);

        if !is_valid {
            self.int_builder.append_null();
            self.double_builder.append_null();
        }
    }

    fn finish(&mut self) -> Option<StructArray> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.int_builder.finish()),
            Arc::new(self.double_builder.finish()),
        ];
        match StructArray::try_new(
            number_fields().into(),
            arrays,
            self.null_buffer_builder.finish(),
        ) {
            Ok(array) => Some(array),
            Err(e) => {
                if cfg!(feature = "dev") {
                    panic!("Could not convert number: {e}")
                } else {
                    None
                }
            }
        }
    }
}

struct DataHistogramBuilder {
    count_builder: UInt64Builder,
    sum_builder: Float64Builder,
    bucket_counts_builder: ListBuilder<UInt64Builder>,
    // explicit_bounds specifies buckets with explicitly defined bounds for values.
    // The boundaries for bucket at index i are:
    // (-infinity, explicit_bounds[i]] for i == 0
    // (explicit_bounds[i-1], explicit_bounds[i]] for 0 < i < size(explicit_bounds)
    // (explicit_bounds[i-1], +infinity) for i == size(explicit_bounds)
    // The values in the explicit_bounds array must be strictly increasing.
    explicit_bounds_builder: ListBuilder<Float64Builder>,
    min_builder: Float64Builder,
    max_builder: Float64Builder,
    null_buffer_builder: NullBufferBuilder,
}

impl DataHistogramBuilder {
    fn with_capacity(capacity: usize) -> Self {
        DataHistogramBuilder {
            count_builder: UInt64Builder::with_capacity(capacity),
            sum_builder: Float64Builder::with_capacity(capacity),
            bucket_counts_builder: ListBuilder::new(UInt64Builder::with_capacity(capacity)),
            explicit_bounds_builder: ListBuilder::new(Float64Builder::with_capacity(capacity)),
            min_builder: Float64Builder::with_capacity(capacity),
            max_builder: Float64Builder::with_capacity(capacity),
            null_buffer_builder: NullBufferBuilder::new(capacity),
        }
    }

    fn append(&mut self, is_valid: bool) {
        self.null_buffer_builder.append(is_valid);

        if !is_valid {
            self.count_builder.append_null();
            self.sum_builder.append_null();
            self.bucket_counts_builder.append_null();
            self.explicit_bounds_builder.append_null();
            self.min_builder.append_null();
            self.max_builder.append_null();
        }
    }

    fn finish(&mut self) -> Option<StructArray> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.count_builder.finish()),
            Arc::new(self.sum_builder.finish()),
            Arc::new(self.bucket_counts_builder.finish()),
            Arc::new(self.explicit_bounds_builder.finish()),
            Arc::new(self.min_builder.finish()),
            Arc::new(self.max_builder.finish()),
        ];
        match StructArray::try_new(
            histogram_data_fields().into(),
            arrays,
            self.null_buffer_builder.finish(),
        ) {
            Ok(array) => Some(array),
            Err(e) => {
                if cfg!(feature = "dev") {
                    panic!("Could not convert histogram data: {e}")
                } else {
                    None
                }
            }
        }
    }
}

struct ResourceBuilder {
    schema_url_builder: StringBuilder,
    attributes_builder: AttributesBuilder,
    dropped_attributes_count_builder: UInt32Builder,
    null_buffer_builder: NullBufferBuilder,
}

impl ResourceBuilder {
    fn with_capacity(capacity: usize) -> Self {
        ResourceBuilder {
            schema_url_builder: StringBuilder::with_capacity(capacity, capacity),
            attributes_builder: AttributesBuilder::with_capacity(capacity),
            dropped_attributes_count_builder: UInt32Builder::with_capacity(capacity),
            null_buffer_builder: NullBufferBuilder::new(capacity),
        }
    }

    fn append(&mut self, is_valid: bool) {
        self.null_buffer_builder.append(is_valid);
    }

    fn finish(&mut self) -> Option<StructArray> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.schema_url_builder.finish()),
            Arc::new(self.attributes_builder.finish()?),
            Arc::new(self.dropped_attributes_count_builder.finish()),
        ];
        match StructArray::try_new(
            resource_fields().into(),
            arrays,
            self.null_buffer_builder.finish(),
        ) {
            Ok(array) => Some(array),
            Err(e) => {
                if cfg!(feature = "dev") {
                    panic!("Could not convert resource: {e}")
                } else {
                    None
                }
            }
        }
    }
}

struct ScopeBuilder {
    name_builder: StringBuilder,
    version_builder: StringBuilder,
    attributes_builder: AttributesBuilder,
    dropped_attributes_count_builder: UInt32Builder,
    null_buffer_builder: NullBufferBuilder,
}

impl ScopeBuilder {
    fn with_capacity(capacity: usize) -> Self {
        ScopeBuilder {
            name_builder: StringBuilder::with_capacity(capacity, capacity),
            version_builder: StringBuilder::with_capacity(capacity, capacity),
            attributes_builder: AttributesBuilder::with_capacity(capacity),
            dropped_attributes_count_builder: UInt32Builder::with_capacity(capacity),
            null_buffer_builder: NullBufferBuilder::new(capacity),
        }
    }

    fn append(&mut self, is_valid: bool) {
        self.null_buffer_builder.append(is_valid);
    }

    fn finish(&mut self) -> Option<StructArray> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.name_builder.finish()),
            Arc::new(self.version_builder.finish()),
            Arc::new(self.attributes_builder.finish()?),
            Arc::new(self.dropped_attributes_count_builder.finish()),
        ];
        match StructArray::try_new(
            scope_fields().into(),
            arrays,
            self.null_buffer_builder.finish(),
        ) {
            Ok(array) => Some(array),
            Err(e) => {
                if cfg!(feature = "dev") {
                    panic!("Could not convert scope: {e}")
                } else {
                    None
                }
            }
        }
    }
}

struct AttributesBuilder {
    key_builder: StringBuilder,
    type_builder: UInt8Builder,
    str_builder: StringBuilder,
    int_builder: Int64Builder,
    double_builder: Float64Builder,
    bool_builder: BooleanBuilder,
    bytes_builder: BinaryBuilder,
    ser_builder: BinaryBuilder,
    null_buffer_builder: NullBufferBuilder,

    list_offsets_builder: BufferBuilder<i32>,
    list_null_buffer_builder: NullBufferBuilder,
}

impl AttributesBuilder {
    fn with_capacity(capacity: usize) -> Self {
        let mut offsets_builder = BufferBuilder::new(capacity);
        offsets_builder.append(0);
        AttributesBuilder {
            key_builder: StringBuilder::with_capacity(capacity, capacity),
            type_builder: UInt8Builder::with_capacity(capacity),
            str_builder: StringBuilder::with_capacity(capacity, capacity),
            int_builder: Int64Builder::with_capacity(capacity),
            double_builder: Float64Builder::with_capacity(capacity),
            bool_builder: BooleanBuilder::with_capacity(capacity),
            bytes_builder: BinaryBuilder::with_capacity(capacity, capacity),
            ser_builder: BinaryBuilder::with_capacity(capacity, capacity),
            null_buffer_builder: NullBufferBuilder::new(capacity),
            list_offsets_builder: offsets_builder,
            list_null_buffer_builder: NullBufferBuilder::new(capacity),
        }
    }

    fn append_value(&mut self, is_valid: bool) {
        self.null_buffer_builder.append(is_valid);
    }

    #[allow(clippy::cast_possible_wrap)]
    #[allow(clippy::cast_possible_truncation)]
    fn append(&mut self, is_valid: bool) {
        self.list_offsets_builder
            .append(self.key_builder.len() as i32);
        self.list_null_buffer_builder.append(is_valid);
    }

    fn finish(&mut self) -> Option<ListArray> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.key_builder.finish()),
            Arc::new(self.type_builder.finish()),
            Arc::new(self.str_builder.finish()),
            Arc::new(self.int_builder.finish()),
            Arc::new(self.double_builder.finish()),
            Arc::new(self.bool_builder.finish()),
            Arc::new(self.bytes_builder.finish()),
            Arc::new(self.ser_builder.finish()),
        ];
        let values = match StructArray::try_new(
            attribute_struct_fields().into(),
            arrays,
            self.null_buffer_builder.finish(),
        ) {
            Ok(array) => array,
            Err(e) => {
                if cfg!(feature = "dev") {
                    panic!("Could not convert attributes: {e}")
                } else {
                    return None;
                }
            }
        };

        let offsets = self.list_offsets_builder.finish();
        // Safety: Safe by construction
        let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
        self.list_offsets_builder.append(0);

        match ListArray::try_new(
            Arc::new(attribute_list_field()),
            offsets,
            Arc::new(values),
            self.list_null_buffer_builder.finish(),
        ) {
            Ok(array) => Some(array),
            Err(e) => {
                if cfg!(feature = "dev") {
                    panic!("Could not convert attributes: {e}")
                } else {
                    None
                }
            }
        }
    }
}

impl OtelToArrowConverter {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        OtelToArrowConverter {
            time_unix_nano_builder: TimestampNanosecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some("UTC".into()),
                )),
            start_time_unix_nano_builder: TimestampNanosecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some("UTC".into()),
                )),
            resource_builder: ResourceBuilder::with_capacity(capacity),
            scope_builder: ScopeBuilder::with_capacity(capacity),
            schema_url_builder: StringBuilder::with_capacity(capacity, capacity),
            metric_type_builder: UInt8Builder::with_capacity(capacity),
            name_builder: StringBuilder::with_capacity(capacity, capacity),
            description_builder: StringBuilder::with_capacity(capacity, capacity),
            unit_builder: StringBuilder::with_capacity(capacity, capacity),
            aggregation_temporality_builder: Int32Builder::with_capacity(capacity),
            is_monotonic_builder: BooleanBuilder::with_capacity(capacity),
            flags_builder: UInt32Builder::with_capacity(capacity),
            attributes_builder: AttributesBuilder::with_capacity(capacity),
            data_number_builder: DataNumberBuilder::with_capacity(capacity),
            data_histogram_builder: DataHistogramBuilder::with_capacity(capacity),
        }
    }

    /// Converts the given `ResourceMetrics` into an Arrow `RecordBatch`.
    ///
    /// # Errors
    ///
    /// Returns an error if the conversion fails.
    pub fn convert(
        &mut self,
        resource_metrics: &ResourceMetrics,
    ) -> Result<RecordBatch, MetricError> {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                self.process_metric(metric, &resource_metrics.resource, &scope_metrics.scope)?;
            }
        }

        let Some(resource) = self.resource_builder.finish() else {
            return Err(MetricError::Other("Could not convert resource".into()));
        };
        let Some(scope) = self.scope_builder.finish() else {
            return Err(MetricError::Other("Could not convert scope".into()));
        };
        let Some(attributes) = self.attributes_builder.finish() else {
            return Err(MetricError::Other("Could not convert attributes".into()));
        };
        let Some(data_number) = self.data_number_builder.finish() else {
            return Err(MetricError::Other("Could not convert data number".into()));
        };
        let Some(data_histogram) = self.data_histogram_builder.finish() else {
            return Err(MetricError::Other(
                "Could not convert data histogram".into(),
            ));
        };

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.time_unix_nano_builder.finish()),
            Arc::new(self.start_time_unix_nano_builder.finish()),
            Arc::new(resource),
            Arc::new(scope),
            Arc::new(self.schema_url_builder.finish()),
            Arc::new(self.metric_type_builder.finish()),
            Arc::new(self.name_builder.finish()),
            Arc::new(self.description_builder.finish()),
            Arc::new(self.unit_builder.finish()),
            Arc::new(self.aggregation_temporality_builder.finish()),
            Arc::new(self.is_monotonic_builder.finish()),
            Arc::new(self.flags_builder.finish()),
            Arc::new(attributes),
            Arc::new(data_number),
            Arc::new(data_histogram),
        ];

        RecordBatch::try_new(crate::schema::schema(), arrays)
            .map_err(|e| MetricError::Other(e.to_string()))
    }

    fn process_metric(
        &mut self,
        metric: &Metric,
        resource: &Resource,
        instrument_scope: &InstrumentationScope,
    ) -> Result<(), MetricError> {
        let data = metric.data.as_any();

        // This is unfortunately the only way to downcast a generic trait object
        if let Some(hist) = data.downcast_ref::<Histogram<i64>>() {
            self.process_histogram(hist, metric, resource, instrument_scope);
        } else if let Some(hist) = data.downcast_ref::<Histogram<u64>>() {
            self.process_histogram(hist, metric, resource, instrument_scope);
        } else if let Some(hist) = data.downcast_ref::<Histogram<f64>>() {
            self.process_histogram(hist, metric, resource, instrument_scope);
        } else if let Some(sum) = data.downcast_ref::<Sum<u64>>() {
            self.process_sum(sum, metric, resource, instrument_scope);
        } else if let Some(sum) = data.downcast_ref::<Sum<i64>>() {
            self.process_sum(sum, metric, resource, instrument_scope);
        } else if let Some(sum) = data.downcast_ref::<Sum<f64>>() {
            self.process_sum(sum, metric, resource, instrument_scope);
        } else if let Some(gauge) = data.downcast_ref::<Gauge<u64>>() {
            self.process_gauge(gauge, metric, resource, instrument_scope);
        } else if let Some(gauge) = data.downcast_ref::<Gauge<i64>>() {
            self.process_gauge(gauge, metric, resource, instrument_scope);
        } else if let Some(gauge) = data.downcast_ref::<Gauge<f64>>() {
            self.process_gauge(gauge, metric, resource, instrument_scope);
        } else {
            return Err(MetricError::Other("Unsupported metric type".into()));
        }

        Ok(())
    }

    fn process_sum<T: AppendDataNumber>(
        &mut self,
        sum: &Sum<T>,
        metric: &Metric,
        resource: &Resource,
        instrument_scope: &InstrumentationScope,
    ) {
        for data_point in &sum.data_points {
            self.time_unix_nano_builder
                .append_option(data_point.time.map(system_time_to_nanos));
            self.start_time_unix_nano_builder
                .append_option(data_point.start_time.map(system_time_to_nanos));

            self.add_resource(resource);
            self.add_scope(instrument_scope);

            self.metric_type_builder
                .append_value(crate::schema::MetricType::Sum.to_u8());

            self.name_builder.append_value(&metric.name);
            self.description_builder.append_value(&metric.description);
            self.unit_builder.append_value(&metric.unit);

            self.aggregation_temporality_builder
                .append_value(temporality_to_i32(sum.temporality));
            self.is_monotonic_builder.append_value(sum.is_monotonic);

            self.flags_builder.append_null();

            Self::add_attributes_to_builder(&mut self.attributes_builder, &data_point.attributes);

            data_point.value.append(&mut self.data_number_builder);
            self.data_histogram_builder.append(false);
        }
    }

    fn process_gauge<T: AppendDataNumber>(
        &mut self,
        gauge: &Gauge<T>,
        metric: &Metric,
        resource: &Resource,
        instrument_scope: &InstrumentationScope,
    ) {
        for data_point in &gauge.data_points {
            self.time_unix_nano_builder
                .append_option(data_point.time.map(system_time_to_nanos));
            self.start_time_unix_nano_builder
                .append_option(data_point.start_time.map(system_time_to_nanos));

            self.add_resource(resource);
            self.add_scope(instrument_scope);

            self.metric_type_builder
                .append_value(crate::schema::MetricType::Gauge.to_u8());

            self.name_builder.append_value(&metric.name);
            self.description_builder.append_value(&metric.description);
            self.unit_builder.append_value(&metric.unit);

            self.aggregation_temporality_builder.append_null();
            self.is_monotonic_builder.append_null();

            self.flags_builder.append_null();

            Self::add_attributes_to_builder(&mut self.attributes_builder, &data_point.attributes);

            data_point.value.append(&mut self.data_number_builder);
            self.data_histogram_builder.append(false);
        }
    }

    fn process_histogram<T: AppendFloat64 + Clone>(
        &mut self,
        histogram: &Histogram<T>,
        metric: &Metric,
        resource: &Resource,
        instrument_scope: &InstrumentationScope,
    ) {
        for data_point in &histogram.data_points {
            self.time_unix_nano_builder
                .append_value(system_time_to_nanos(data_point.time));
            self.start_time_unix_nano_builder
                .append_value(system_time_to_nanos(data_point.start_time));

            self.add_resource(resource);
            self.add_scope(instrument_scope);

            self.metric_type_builder
                .append_value(crate::schema::MetricType::Histogram.to_u8());

            self.name_builder.append_value(&metric.name);
            self.description_builder.append_value(&metric.description);
            self.unit_builder.append_value(&metric.unit);

            self.aggregation_temporality_builder
                .append_value(temporality_to_i32(histogram.temporality));
            self.is_monotonic_builder.append_null();

            self.flags_builder.append_null();

            Self::add_attributes_to_builder(&mut self.attributes_builder, &data_point.attributes);

            self.data_number_builder.append(false);

            self.data_histogram_builder
                .count_builder
                .append_value(data_point.count);
            data_point
                .sum
                .append(&mut self.data_histogram_builder.sum_builder);
            AppendFloat64::append_option(
                data_point.min.clone(),
                &mut self.data_histogram_builder.min_builder,
            );
            AppendFloat64::append_option(
                data_point.max.clone(),
                &mut self.data_histogram_builder.max_builder,
            );

            self.data_histogram_builder
                .bucket_counts_builder
                .append_value(
                    data_point
                        .bucket_counts
                        .iter()
                        .map(|bc| Some(*bc))
                        .collect::<Vec<Option<u64>>>(),
                );

            self.data_histogram_builder
                .explicit_bounds_builder
                .append_value(
                    data_point
                        .bounds
                        .iter()
                        .map(|b| Some(*b))
                        .collect::<Vec<Option<f64>>>(),
                );

            self.data_histogram_builder.append(true);
        }
    }

    fn add_resource(&mut self, resource: &Resource) {
        self.resource_builder
            .schema_url_builder
            .append_option(resource.schema_url().as_ref());

        let attributes: Vec<opentelemetry::KeyValue> = resource
            .iter()
            .map(|(key, value)| opentelemetry::KeyValue::new(key.clone(), value.clone()))
            .collect();
        Self::add_attributes_to_builder(&mut self.resource_builder.attributes_builder, &attributes);

        self.resource_builder
            .dropped_attributes_count_builder
            .append_null();

        self.resource_builder.append(true);
    }

    fn add_scope(&mut self, scope: &InstrumentationScope) {
        self.scope_builder.name_builder.append_value(scope.name());

        self.scope_builder
            .version_builder
            .append_option(scope.version());

        Self::add_attributes_to_builder(
            &mut self.scope_builder.attributes_builder,
            scope.attributes().cloned().collect::<Vec<_>>().as_slice(),
        );

        self.scope_builder
            .dropped_attributes_count_builder
            .append_null();

        self.scope_builder.append(true);

        self.schema_url_builder.append_option(scope.schema_url());
    }

    fn add_attributes_to_builder(
        builder: &mut AttributesBuilder,
        attributes: &[opentelemetry::KeyValue],
    ) {
        for kv in attributes {
            builder.key_builder.append_value(kv.key.as_str());
            match &kv.value {
                opentelemetry::Value::String(s) => {
                    builder
                        .type_builder
                        .append_value(crate::schema::AttributeValueType::Str.to_u8());
                    builder.str_builder.append_value(s);
                    builder.int_builder.append_null();
                    builder.double_builder.append_null();
                    builder.bool_builder.append_null();
                    builder.bytes_builder.append_null();
                    builder.ser_builder.append_null();
                }
                opentelemetry::Value::I64(i) => {
                    builder
                        .type_builder
                        .append_value(crate::schema::AttributeValueType::Int.to_u8());
                    builder.str_builder.append_null();
                    builder.int_builder.append_value(*i);
                    builder.double_builder.append_null();
                    builder.bool_builder.append_null();
                    builder.bytes_builder.append_null();
                    builder.ser_builder.append_null();
                }
                opentelemetry::Value::F64(f) => {
                    builder
                        .type_builder
                        .append_value(crate::schema::AttributeValueType::Double.to_u8());
                    builder.str_builder.append_null();
                    builder.int_builder.append_null();
                    builder.double_builder.append_value(*f);
                    builder.bool_builder.append_null();
                    builder.bytes_builder.append_null();
                    builder.ser_builder.append_null();
                }
                opentelemetry::Value::Bool(b) => {
                    builder
                        .type_builder
                        .append_value(crate::schema::AttributeValueType::Bool.to_u8());
                    builder.str_builder.append_null();
                    builder.int_builder.append_null();
                    builder.double_builder.append_null();
                    builder.bool_builder.append_value(*b);
                    builder.bytes_builder.append_null();
                    builder.ser_builder.append_null();
                }
                opentelemetry::Value::Array(_arr) => {
                    builder
                        .type_builder
                        .append_value(crate::schema::AttributeValueType::Bytes.to_u8());
                    builder.str_builder.append_null();
                    builder.int_builder.append_null();
                    builder.double_builder.append_null();
                    builder.bool_builder.append_null();
                    // TODO: serialize the value to bytes
                    builder.bytes_builder.append_value([]);
                    builder.ser_builder.append_null();
                }
                _ => {
                    // We don't recognize this value type, so append nulls
                    builder
                        .type_builder
                        .append_value(crate::schema::AttributeValueType::Unknown.to_u8());
                    builder.str_builder.append_null();
                    builder.int_builder.append_null();
                    builder.double_builder.append_null();
                    builder.bool_builder.append_null();
                    builder.bytes_builder.append_null();
                    builder.ser_builder.append_null();
                }
            }
            builder.append_value(true);
        }

        builder.append(true);
    }
}

trait AppendFloat64 {
    fn append(&self, builder: &mut Float64Builder);

    fn append_option(value: Option<Self>, builder: &mut Float64Builder)
    where
        Self: Sized,
    {
        match value {
            Some(value) => value.append(builder),
            None => builder.append_null(),
        }
    }
}

impl AppendFloat64 for f64 {
    fn append(&self, builder: &mut Float64Builder) {
        builder.append_value(*self);
    }
}

impl AppendFloat64 for i64 {
    #[allow(clippy::cast_precision_loss)]
    fn append(&self, builder: &mut Float64Builder) {
        builder.append_value(*self as f64);
    }
}

impl AppendFloat64 for u64 {
    #[allow(clippy::cast_precision_loss)]
    fn append(&self, builder: &mut Float64Builder) {
        builder.append_value(*self as f64);
    }
}

trait AppendDataNumber {
    fn append(&self, builder: &mut DataNumberBuilder);
}

impl AppendDataNumber for u64 {
    #[allow(clippy::cast_possible_wrap)]
    fn append(&self, builder: &mut DataNumberBuilder) {
        builder.int_builder.append_value(*self as i64);
        builder.double_builder.append_null();

        builder.append(true);
    }
}

impl AppendDataNumber for i64 {
    fn append(&self, builder: &mut DataNumberBuilder) {
        builder.int_builder.append_value(*self);
        builder.double_builder.append_null();

        builder.append(true);
    }
}

impl AppendDataNumber for f64 {
    fn append(&self, builder: &mut DataNumberBuilder) {
        builder.int_builder.append_null();
        builder.double_builder.append_value(*self);

        builder.append(true);
    }
}

#[allow(clippy::cast_possible_truncation)]
fn system_time_to_nanos(time: SystemTime) -> i64 {
    let Ok(duration) = time.duration_since(UNIX_EPOCH) else {
        panic!("SystemTime before UNIX EPOCH!");
    };
    duration.as_nanos() as i64
}
