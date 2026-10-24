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

use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow_schema::{DataType, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{
    ColumnStatistics, Result, Statistics, internal_datafusion_err, stats::Precision,
};
use datafusion::config::{ConfigOptions, TableParquetOptions};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::{FileFormat, FileMeta};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, SortOrderPushdownResult};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::metadata::MetadataColumn;
use datafusion_datasource::morsel::Morselizer;
use datafusion_datasource::{PartitionedFile, TableSchema};
use object_store::{ObjectMeta, ObjectStore};

/// Listing-table `Parquet` reader that will not trust float min/max.
#[derive(Debug)]
pub(crate) struct NanSafeParquetFormat {
    inner: ParquetFormat,
}

impl NanSafeParquetFormat {
    #[must_use]
    pub(crate) fn from_options(mut options: TableParquetOptions) -> Self {
        // Static row-group, page and bloom pruning read Parquet min/max
        // directly. Those bounds omit `NaN`, and the scan predicate is
        // attached after `create_physical_plan` via `try_pushdown_filters`,
        // so it cannot be inspected here. `pushdown_filters` stays on: a
        // predicate that *is* attached is still applied as a row filter.
        // Integer and decimal `MAX` still use sanitized listing statistics.
        //
        // `pruning=false` does not stop `DataFusion`'s runtime dynamic-filter
        // `RowGroupPruner` (it never consults that flag). Float-column
        // filters are refused after plan creation so that pruner never
        // sees them.
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

/// Whether `expr` names a `Float16`/`Float32`/`Float64` (or dict-of-float)
/// column. Those columns' Parquet min/max omit `NaN`, so a predicate on them
/// must not reach the runtime row-group pruner.
fn expr_references_untrusted_float(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    let mut found = false;
    let walk = expr.apply(|node| {
        if let Some(column) = node.downcast_ref::<Column>()
            && column_bounds_omit_nan(column, schema)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found || walk.is_err()
}

fn column_bounds_omit_nan(column: &Column, schema: &Schema) -> bool {
    let data_type = schema
        .field_with_name(column.name())
        .ok()
        .map(arrow_schema::Field::data_type)
        .or_else(|| {
            schema
                .fields()
                .get(column.index())
                .map(|field| field.data_type())
        });
    match data_type {
        Some(data_type) => float_bounds_omit_nan(data_type),
        // Unknown column: do not attach. The pruner would have nothing sound
        // to evaluate, and refusing is the conservative answer.
        None => true,
    }
}

/// `ParquetFormat::create_physical_plan` always returns a `DataSourceExec`
/// over a `FileScanConfig`. Replace its `ParquetSource` with a wrapper that
/// will not attach float-column filters.
fn wrap_to_refuse_untrusted_float_filters(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let exec = plan.as_ref().downcast_ref::<DataSourceExec>().ok_or_else(|| {
        internal_datafusion_err!(
            "listing parquet scan must be a DataSourceExec so float filters are not attached for stats pruning"
        )
    })?;
    let config = exec.data_source().downcast_ref::<FileScanConfig>().ok_or_else(|| {
        internal_datafusion_err!(
            "listing parquet scan must be a FileScanConfig so float filters are not attached for stats pruning"
        )
    })?;
    if config.file_source().is::<NanSafeParquetSource>() {
        return Ok(plan);
    }
    let wrapped = Arc::new(NanSafeParquetSource::new(Arc::clone(config.file_source())));
    let config = FileScanConfigBuilder::from(config.clone())
        .with_source(wrapped)
        .build();
    Ok(DataSourceExec::from_data_source(config))
}

/// [`FileSource`] decorator that forwards everything except float-column
/// filter pushdown. `DataFusion`'s runtime `RowGroupPruner` ignores
/// `pruning=false` and evaluates the attached predicate against footer
/// min/max, which omit `NaN`.
///
/// [`FileSource`] methods with a default body must be forwarded:
/// inheriting a default compiles and then drops `inner`'s behavior.
/// `try_to_proto` stays the default `Ok(None)` so a serialized scan is
/// not a bare `ParquetSource` that would re-attach float filters.
struct NanSafeParquetSource {
    inner: Arc<dyn FileSource>,
}

impl NanSafeParquetSource {
    fn new(inner: Arc<dyn FileSource>) -> Self {
        Self { inner }
    }
}

impl FileSource for NanSafeParquetSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        self.inner
            .create_file_opener(object_store, base_config, partition)
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        // Parquet errors out of `create_file_opener`. The trait default
        // would call back into this source's opener, so forward.
        self.inner
            .create_morselizer(object_store, base_config, partition)
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }

    fn with_metadata_cols(
        &self,
        metadata_cols: Vec<MetadataColumn>,
    ) -> Option<Arc<dyn FileSource>> {
        let inner = self.inner.with_metadata_cols(metadata_cols)?;
        Some(Arc::new(Self::new(inner)))
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self::new(self.inner.with_batch_size(batch_size)))
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.inner.projection()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        self.inner.fmt_extra(t, f)
    }

    fn supports_repartitioning(&self) -> bool {
        self.inner.supports_repartitioning()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        self.inner.repartitioned(
            target_partitions,
            repartition_file_min_size,
            output_ordering,
            config,
        )
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let schema = self.inner.table_schema().table_schema();
        let mut parent = vec![PushedDown::No; filters.len()];
        let mut trusted = Vec::new();
        let mut trusted_at = Vec::new();
        for (idx, filter) in filters.iter().enumerate() {
            if !expr_references_untrusted_float(filter, schema.as_ref()) {
                trusted.push(Arc::clone(filter));
                trusted_at.push(idx);
            }
        }
        if trusted.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                parent,
            ));
        }
        let result = self.inner.try_pushdown_filters(trusted, config)?;
        for (offset, idx) in trusted_at.into_iter().enumerate() {
            parent[idx] = result.filters[offset];
        }
        let updated_node = result
            .updated_node
            .map(|inner| Arc::new(Self::new(inner)) as Arc<dyn FileSource>);
        Ok(FilterPushdownPropagation {
            filters: parent,
            updated_node,
        })
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        Ok(match self.inner.try_pushdown_sort(order, eq_properties)? {
            SortOrderPushdownResult::Exact { inner } => SortOrderPushdownResult::Exact {
                inner: Arc::new(Self::new(inner)),
            },
            SortOrderPushdownResult::Inexact { inner } => SortOrderPushdownResult::Inexact {
                inner: Arc::new(Self::new(inner)),
            },
            SortOrderPushdownResult::Unsupported => SortOrderPushdownResult::Unsupported,
        })
    }

    fn try_reverse_output(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        // Keep in lockstep with `try_pushdown_sort`: the trait default
        // would otherwise delegate here and drop `inner`'s sort pushdown.
        #[expect(
            deprecated,
            reason = "FileSource::try_reverse_output is deprecated; forward until it is removed"
        )]
        let result = self.inner.try_reverse_output(order, eq_properties)?;
        Ok(match result {
            SortOrderPushdownResult::Exact { inner } => SortOrderPushdownResult::Exact {
                inner: Arc::new(Self::new(inner)),
            },
            SortOrderPushdownResult::Inexact { inner } => SortOrderPushdownResult::Inexact {
                inner: Arc::new(Self::new(inner)),
            },
            SortOrderPushdownResult::Unsupported => SortOrderPushdownResult::Unsupported,
        })
    }

    fn reorder_files(&self, files: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
        self.inner.reorder_files(files)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let Some(inner) = self.inner.try_pushdown_projection(projection)? else {
            return Ok(None);
        };
        Ok(Some(Arc::new(Self::new(inner))))
    }

    #[expect(
        deprecated,
        reason = "SchemaAdapterFactory is deprecated upstream; forward to `inner` until it is removed"
    )]
    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn datafusion_datasource::schema_adapter::SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        let inner = self.inner.with_schema_adapter_factory(factory)?;
        Ok(Arc::new(Self::new(inner)))
    }

    #[expect(
        deprecated,
        reason = "SchemaAdapterFactory is deprecated upstream; forward to `inner` until it is removed"
    )]
    fn schema_adapter_factory(
        &self,
    ) -> Option<Arc<dyn datafusion_datasource::schema_adapter::SchemaAdapterFactory>> {
        self.inner.schema_adapter_factory()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
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
        // `ParquetFormat::create_physical_plan` downcasts `file_source` to
        // `ParquetSource` and replaces any wrapper, so the source that
        // refuses float filters must be installed on the finished plan.
        let plan = self.inner.create_physical_plan(state, conf).await?;
        wrap_to_refuse_untrusted_float_filters(plan)
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

    fn column_expr(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    #[test]
    fn expr_references_untrusted_float_detects_float_and_unknown_columns() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new(
                "g",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Float32)),
                true,
            ),
            Field::new("amt", DataType::Decimal128(10, 2), true),
        ]);
        assert!(!expr_references_untrusted_float(
            &column_expr("k", 0),
            &schema
        ));
        assert!(!expr_references_untrusted_float(
            &column_expr("amt", 3),
            &schema
        ));
        assert!(expr_references_untrusted_float(
            &column_expr("f", 1),
            &schema
        ));
        assert!(expr_references_untrusted_float(
            &column_expr("g", 2),
            &schema
        ));
        assert!(
            expr_references_untrusted_float(&column_expr("missing", 9), &schema),
            "unknown columns must not be attached for stats pruning"
        );
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
