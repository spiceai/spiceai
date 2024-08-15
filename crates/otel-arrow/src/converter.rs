/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int32Array, Int64Array, Int64Builder, StringArray, StringBuilder, StructArray, StructBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt8Array, UInt8Builder
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use opentelemetry::metrics::MetricsError;
use opentelemetry::InstrumentationLibrary;
use opentelemetry_sdk::metrics::data::{
    Gauge, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics
};
use opentelemetry_sdk::Resource;
use std::sync::Arc;

pub struct OtelToArrowConverter {
    schema: Arc<Schema>,
    time_unix_nano_builder: TimestampNanosecondBuilder,
    start_time_unix_nano_builder: TimestampNanosecondBuilder,
    resource_builder: StructBuilder,
    scope_builder: StructBuilder,
    schema_url_builder: StringBuilder,
    metric_type_builder: UInt8Builder,
    name_builder: StringBuilder,
    description_builder: StringBuilder,
    unit_builder: StringBuilder,
    aggregation_temporality_builder: Int64Builder,
    is_monotonic_builder: BooleanBuilder,
    flags_builder: UInt32Builder,
    attributes_builder: StructBuilder,
    data_number_builder: StructBuilder,
    data_histogram_builder: StructBuilder,
};

impl OtelToArrowConverter {
    pub fn new(capacity: usize) -> Self {
        let schema = crate::schema::schema();
        
        let resource_type = schema.field_with_name("resource").unwrap().data_type().clone();
        let DataType::Struct(resource_schema_fields) = resource_type else {
            unreachable!("resource field is a struct");
        };
        let scope_type = schema.field_with_name("scope").unwrap().data_type().clone();
        let DataType::Struct(scope_fields) = scope_type else {
            unreachable!("scope field is a struct");
        };
        let attributes_type = schema.field_with_name("attributes").unwrap().data_type().clone();
        let DataType::Struct(attributes_fields) = attributes_type else {
            unreachable!("attributes field is a struct");
        };
        let data_number_type = schema.field_with_name("data_number").unwrap().data_type().clone();
        let DataType::Struct(data_number_fields) = data_number_type else {
            unreachable!("data_number field is a struct");
        };
        let data_histogram_type = schema.field_with_name("data_histogram").unwrap().data_type().clone();
        let DataType::Struct(data_histogram_fields) = data_histogram_type else {
            unreachable!("data_histogram field is a struct");
        };

        OtelToArrowConverter {
            schema: schema.clone(),
            time_unix_nano_builder: TimestampNanosecondBuilder::with_capacity(capacity),
            start_time_unix_nano_builder: TimestampNanosecondBuilder::with_capacity(capacity),
            resource_builder: StructBuilder::from_fields(resource_schema_fields, capacity),
            scope_builder: StructBuilder::from_fields(scope_fields, capacity),
            schema_url_builder: StringBuilder::with_capacity(capacity, capacity),
            metric_type_builder: UInt8Builder::with_capacity(capacity),
            name_builder: StringBuilder::with_capacity(capacity, capacity),
            description_builder: StringBuilder::with_capacity(capacity, capacity),
            unit_builder: StringBuilder::with_capacity(capacity, capacity),
            aggregation_temporality_builder: Int64Builder::with_capacity(capacity),
            is_monotonic_builder: BooleanBuilder::with_capacity(capacity),
            flags_builder: UInt32Builder::with_capacity(capacity),
            attributes_builder: StructBuilder::from_fields(attributes_fields, capacity),
            data_number_builder: StructBuilder::from_fields(data_number_fields, capacity),
            data_histogram_builder: StructBuilder::from_fields(data_histogram_fields, capacity),
        }
    }
    
    pub fn convert(&mut self, resource_metrics: &ResourceMetrics) -> Result<RecordBatch, MetricsError> {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                self.process_metric(
                    metric,
                    &resource_metrics.resource,
                    &scope_metrics.scope,
                )?;
            }
        }

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.time_unix_nano_builder.finish()),
            Arc::new(self.start_time_unix_nano_builder.finish()),
            Arc::new(self.resource_builder.finish()),
            Arc::new(self.scope_builder.finish()),
            Arc::new(self.schema_url_builder.finish()),
            Arc::new(self.metric_type_builder.finish()),
            Arc::new(self.name_builder.finish()),
            Arc::new(self.description_builder.finish()),
            Arc::new(self.unit_builder.finish()),
            Arc::new(self.aggregation_temporality_builder.finish()),
            Arc::new(self.is_monotonic_builder.finish()),
            Arc::new(self.flags_builder.finish()),
            Arc::new(self.attributes_builder.finish()),
            Arc::new(self.data_number_builder.finish()),
            Arc::new(self.data_histogram_builder.finish()),
        ];

        RecordBatch::try_new(self.schema.clone(), arrays).map_err(|e| MetricsError::Other(e.to_string()))
    }

    fn process_metric(
        &mut self,
        metric: &Metric,
        resource: &Resource,
        instrument_scope: &InstrumentationLibrary,
    ) -> Result<(), MetricsError> {
        self.name_builder.append_value(&metric.name);
        self.description_builder.append_value(&metric.description);
        self.unit_builder.append_value(&metric.unit);

        if let Some(gauge) = metric.data.as_any().downcast_ref::<Gauge<u64>>() {
            self.process_gauge(gauge, resource, instrument_scope)?;
        } else if let Some(gauge) = metric.data.as_any().downcast_ref::<Gauge<f64>>() {
            self.process_gauge(gauge, resource, instrument_scope)?;
        } else {
            return Err(MetricsError::Other("Unsupported metric type".into()));
        }

        Ok(())
    }

    fn process_gauge<T>(
        &mut self,
        gauge: &Gauge<T>,
        resource: &Resource,
        instrument_scope: &InstrumentationLibrary,
    ) -> Result<(), MetricsError> {
        for data_point in &gauge.data_points {
            self.time_unix_nano_builder.append_value(data_point.time_unix_nano);
            self.start_time_unix_nano_builder.append_option(data_point.start_time_unix_nano);
            
            self.add_resource(resource)?;
            self.add_scope(instrument_scope)?;
            
            self.schema_url_builder.append_null();
            self.metric_type_builder.append_value(crate::schema::MetricType::Gauge.to_u8());
            
            self.aggregation_temporality_builder.append_null();
            self.is_monotonic_builder.append_null();
            
            self.flags_builder.append_value(data_point.flags);
            
            self.add_attributes(&data_point.attributes)?;
            
            self.add_number_data(data_point)?;
            self.data_histogram_builder.append_null();
        }

        Ok(())
    }

    fn add_resource(&mut self, resource: &Resource) -> Result<(), MetricsError> {
        let schema_url = self.resource_builder.field_builder::<StringBuilder>(0)?;
        schema_url.append_option(resource.schema_url().map(|s| s.to_string()));

        let attributes = self.resource_builder.field_builder::<StructBuilder>(1)?;
        self.add_attributes_to_builder(attributes, &resource.attributes())?;

        let dropped_attributes_count = self.resource_builder.field_builder::<UInt32Builder>(2)?;
        dropped_attributes_count.append_value(resource.dropped_attributes_count() as u32);

        self.resource_builder.append(true)?;
        Ok(())
    }

    fn add_scope(&mut self, scope: &InstrumentationLibrary, dropped_attributes_count: u32) -> Result<(), MetricsError> {
        let Some(name) = self.scope_builder.field_builder::<StringBuilder>(0) else {
            unreachable!("name field is a string");
        };
        name.append_value(&scope.name);

        let Some(version) = self.scope_builder.field_builder::<StringBuilder>(1) else {
            unreachable!("version field is a string");
        };
        version.append_option(scope.version.as_deref());

        let Some(attributes) = self.scope_builder.field_builder::<StructBuilder>(2) else {
            unreachable!("attributes field is a struct");
        };
        Self::add_attributes_to_builder(attributes, &scope.attributes)?;

        let Some(dropped_attributes_count_builder) = self.scope_builder.field_builder::<UInt32Builder>(3) else {
            unreachable!("dropped_attributes_count field is a u32");
        };
        dropped_attributes_count_builder.append_value(dropped_attributes_count);

        self.scope_builder.append(true);
        Ok(())
    }

    fn add_attributes(&mut self, attributes: &[opentelemetry::KeyValue]) -> Result<(), MetricsError> {
        Self::add_attributes_to_builder(&mut self.attributes_builder, attributes)?;
        self.attributes_builder.append(true);
        Ok(())
    }

    fn add_attributes_to_builder(builder: &mut StructBuilder, attributes: &[opentelemetry::KeyValue]) -> Result<(), MetricsError> {
        
        
        
        let Some(int_builder) = builder.field_builder::<Int64Builder>(3) else {
            unreachable!("int field is an i64");
        };
        let Some(double_builder) = builder.field_builder::<Float64Builder>(4) else {
            unreachable!("double field is an f64");
        };
        let Some(bool_builder) = builder.field_builder::<BooleanBuilder>(5) else {
            unreachable!("bool field is a boolean");
        };
        let Some(bytes_builder) = builder.field_builder::<BinaryBuilder>(6) else {
            unreachable!("bytes field is a binary");
        };
        let Some(ser_builder) = builder.field_builder::<BinaryBuilder>(7) else {
            unreachable!("ser field is a binary");
        };

        for kv in attributes {
            let Some(key_builder) = builder.field_builder::<StringBuilder>(0) else {
                unreachable!("key field is a string");
            };
            key_builder.append_value(kv.key.as_str());
            match &kv.value {
                opentelemetry::Value::String(s) => {
                    let Some(type_builder) = builder.field_builder::<UInt8Builder>(1) else {
                        unreachable!("type field is a u8");
                    };
                    type_builder.append_value(crate::schema::AttributeValueType::Str.to_u8());
                    let Some(str_builder) = builder.field_builder::<StringBuilder>(2) else {
                        unreachable!("str field is a string");
                    };
                    str_builder.append_value(s);
                    int_builder.append_null();
                    double_builder.append_null();
                    bool_builder.append_null();
                    bytes_builder.append_null();
                    ser_builder.append_null();
                },
                opentelemetry::Value::I64(i) => {
                    let Some(type_builder) = builder.field_builder::<UInt8Builder>(1) else {
                        unreachable!("type field is a u8");
                    };
                    type_builder.append_value(crate::schema::AttributeValueType::Int.to_u8());
                    str_builder.append_null();
                    int_builder.append_value(*i);
                    double_builder.append_null();
                    bool_builder.append_null();
                    bytes_builder.append_null();
                    ser_builder.append_null();
                },
                opentelemetry::Value::F64(f) => {
                    type_builder.append_value(crate::schema::AttributeValueType::Double.to_u8());
                    str_builder.append_null();
                    int_builder.append_null();
                    double_builder.append_value(*f);
                    bool_builder.append_null();
                    bytes_builder.append_null();
                    ser_builder.append_null();
                },
                opentelemetry::Value::Bool(b) => {
                    type_builder.append_value(crate::schema::AttributeValueType::Bool.to_u8());
                    str_builder.append_null();
                    int_builder.append_null();
                    double_builder.append_null();
                    bool_builder.append_value(*b);
                    bytes_builder.append_null();
                    ser_builder.append_null();
                },
                // For complex types like arrays or maps, we'll serialize them to bytes
                _ => {
                    type_builder.append_value(crate::schema::AttributeValueType::Bytes.to_u8());
                    str_builder.append_null();
                    int_builder.append_null();
                    double_builder.append_null();
                    bool_builder.append_null();
                    // This is a placeholder. You'd need to implement actual serialization.
                    bytes_builder.append_value(&[]);
                    builder.field_builder::<BinaryBuilder>(7).append_null();
                },
            }
            builder.append(true);
        }

        Ok(())
    }

    fn add_number_data(&mut self, data_point: &NumberDataPoint) -> Result<(), MetricsError> {
        let int_builder = self.data_number_builder.field_builder::<Int64Builder>(0)?;
        let double_builder = self.data_number_builder.field_builder::<Float64Builder>(1)?;

        match data_point.value {
            opentelemetry_sdk::metrics::data::NumberDataPointValue::I64(v) => {
                int_builder.append_value(v);
                double_builder.append_null();
            },
            opentelemetry_sdk::metrics::data::NumberDataPointValue::F64(v) => {
                int_builder.append_null();
                double_builder.append_value(v);
            },
        }

        self.data_number_builder.append(true)?;
        Ok(())
    }
}
