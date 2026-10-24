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

//! Parquet float column statistics omit `NaN`, so they are not a bound on every
//! non-null value `DataFusion` would compare.
//!
//! Same rule as [`vortex_datafusion::bounds_account_for_nan`] (Cayenne,
//! spiceai/spiceai#14719 / #14792): a float min, max or sum that left `NaN` out
//! must not answer `MIN`/`MAX`/`SUM` or prune a `NaN` probe / `x > c`. Parquet
//! `ColumnIndex` carries `nan_counts` only in newer specs, so this path treats
//! every `Float16`/`Float32`/`Float64` bound as untrusted.

use std::sync::Arc;

use arrow_schema::{DataType, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{ColumnStatistics, Result, Statistics, stats::Precision};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::{FileFormat, FileMeta};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::physical_expr::{LexOrdering, LexRequirement};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::TableSchema;
use object_store::{ObjectMeta, ObjectStore};

/// Listing-table `Parquet` reader that will not trust float min/max.
#[derive(Debug)]
pub(crate) struct NanSafeParquetFormat {
    inner: ParquetFormat,
}

impl NanSafeParquetFormat {
    #[must_use]
    pub(crate) fn from_options(mut options: TableParquetOptions) -> Self {
        // Row-group, page and bloom pruning read Parquet min/max directly.
        // Those bounds omit `NaN`, and the scan predicate is attached after
        // `create_physical_plan` via `try_pushdown_filters`, so it cannot be
        // inspected here. `pushdown_filters` stays on: the predicate is still
        // applied as a row filter. Integer and decimal `MAX` still use
        // sanitized listing statistics.
        options.global.pruning = false;
        options.global.enable_page_index = false;
        options.global.bloom_filter_on_read = false;
        Self {
            inner: ParquetFormat::default().with_options(options),
        }
    }
}

/// The `FileFormat` every listing connector must use for `Parquet`.
#[must_use]
pub(crate) fn listing_parquet_format(options: TableParquetOptions) -> Arc<dyn FileFormat> {
    Arc::new(NanSafeParquetFormat::from_options(options))
}

/// Whether this Arrow type's Parquet (or Vortex) min/max leave `NaN` out.
///
/// Dictionary-encoded floats use the value type. Same float set as
/// [`vortex_datafusion::bounds_account_for_nan`].
#[must_use]
pub(crate) fn float_bounds_omit_nan(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        DataType::Dictionary(_, value) => float_bounds_omit_nan(value),
        _ => false,
    }
}

/// Drop min/max/sum on float columns. Null counts and integer/decimal bounds stay.
#[must_use]
pub(crate) fn drop_untrusted_float_bounds(stats: Statistics, schema: &Schema) -> Statistics {
    let column_statistics = stats
        .column_statistics
        .into_iter()
        .enumerate()
        .map(|(idx, column)| match schema.fields().get(idx) {
            Some(field) if float_bounds_omit_nan(field.data_type()) => ColumnStatistics {
                min_value: Precision::Absent,
                max_value: Precision::Absent,
                sum_value: Precision::Absent,
                ..column
            },
            _ => column,
        })
        .collect();
    Statistics {
        column_statistics,
        ..stats
    }
}

#[async_trait]
impl FileFormat for NanSafeParquetFormat {
    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let inferred = self
            .inner
            .infer_stats(state, store, Arc::clone(&table_schema), object)
            .await?;
        Ok(drop_untrusted_float_bounds(inferred, table_schema.as_ref()))
    }

    async fn infer_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Option<LexOrdering>> {
        self.inner
            .infer_ordering(state, store, table_schema, object)
            .await
    }

    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<FileMeta> {
        let meta = self
            .inner
            .infer_stats_and_ordering(state, store, Arc::clone(&table_schema), object)
            .await?;
        Ok(FileMeta::new(drop_untrusted_float_bounds(
            meta.statistics,
            table_schema.as_ref(),
        ))
        .with_ordering(meta.ordering))
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(state, conf).await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.inner.file_source(table_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::common::ScalarValue;

    fn exact_i64(value: i64) -> Precision<ScalarValue> {
        Precision::Exact(ScalarValue::Int64(Some(value)))
    }

    fn exact_f64(value: f64) -> Precision<ScalarValue> {
        Precision::Exact(ScalarValue::Float64(Some(value)))
    }

    fn exact_decimal(value: i128) -> Precision<ScalarValue> {
        Precision::Exact(ScalarValue::Decimal128(Some(value), 10, 2))
    }

    fn column(min: Precision<ScalarValue>, max: Precision<ScalarValue>) -> ColumnStatistics {
        ColumnStatistics {
            min_value: min,
            max_value: max,
            null_count: Precision::Exact(1),
            ..ColumnStatistics::new_unknown()
        }
    }

    #[test]
    fn float_bounds_omit_nan_covers_float16_32_64_and_dictionaries() {
        assert!(float_bounds_omit_nan(&DataType::Float16));
        assert!(float_bounds_omit_nan(&DataType::Float32));
        assert!(float_bounds_omit_nan(&DataType::Float64));
        assert!(float_bounds_omit_nan(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Float64),
        )));
        assert!(!float_bounds_omit_nan(&DataType::Int64));
        assert!(!float_bounds_omit_nan(&DataType::Decimal128(10, 2)));
        assert!(!float_bounds_omit_nan(&DataType::Utf8));
        assert!(!float_bounds_omit_nan(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Int64),
        )));
    }

    #[test]
    fn drop_untrusted_float_bounds_keeps_int_and_decimal_and_null_counts() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("g", DataType::Float32, true),
            Field::new("amt", DataType::Decimal128(10, 2), true),
        ]);
        let stats = Statistics {
            num_rows: Precision::Exact(8),
            total_byte_size: Precision::Exact(64),
            column_statistics: vec![
                column(exact_i64(0), exact_i64(9)),
                column(exact_f64(f64::NEG_INFINITY), exact_f64(f64::INFINITY)),
                column(
                    Precision::Exact(ScalarValue::Float32(Some(-1.0))),
                    Precision::Exact(ScalarValue::Float32(Some(1.0))),
                ),
                column(exact_decimal(100), exact_decimal(999)),
            ],
        };

        let sanitized = drop_untrusted_float_bounds(stats, &schema);

        assert_eq!(sanitized.column_statistics[0].min_value, exact_i64(0));
        assert_eq!(sanitized.column_statistics[0].max_value, exact_i64(9));
        assert_eq!(
            sanitized.column_statistics[0].null_count,
            Precision::Exact(1)
        );
        assert_eq!(sanitized.column_statistics[1].min_value, Precision::Absent);
        assert_eq!(sanitized.column_statistics[1].max_value, Precision::Absent);
        assert_eq!(sanitized.column_statistics[1].sum_value, Precision::Absent);
        assert_eq!(
            sanitized.column_statistics[1].null_count,
            Precision::Exact(1)
        );
        assert_eq!(sanitized.column_statistics[2].min_value, Precision::Absent);
        assert_eq!(sanitized.column_statistics[2].max_value, Precision::Absent);
        assert_eq!(sanitized.column_statistics[3].min_value, exact_decimal(100));
        assert_eq!(sanitized.column_statistics[3].max_value, exact_decimal(999));
        assert_eq!(sanitized.num_rows, Precision::Exact(8));
    }

    #[test]
    fn listing_parquet_format_disables_untrusted_float_pruning() {
        let mut options = TableParquetOptions::new();
        options.global.pruning = true;
        options.global.enable_page_index = true;
        options.global.bloom_filter_on_read = true;
        options.global.pushdown_filters = true;

        let format = NanSafeParquetFormat::from_options(options);
        let inner = format.inner.options();
        assert!(!inner.global.pruning);
        assert!(!inner.global.enable_page_index);
        assert!(!inner.global.bloom_filter_on_read);
        assert!(
            inner.global.pushdown_filters,
            "row filters must still apply after stats pruning is disabled"
        );
    }
}
