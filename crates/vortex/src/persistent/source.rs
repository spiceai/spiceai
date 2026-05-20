// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::any::Any;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Weak;

use datafusion_common::Result as DFResult;
use datafusion_common::config::ConfigOptions;
use datafusion_common::exec_datafusion_err;
use datafusion_datasource::TableSchema;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_groups::FileGroupPartitioner;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::conjunction;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::PhysicalExpr;
use datafusion_physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion_physical_plan::filter_pushdown::PushedDown;
use datafusion_physical_plan::filter_pushdown::PushedDownPredicate;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;
use object_store::path::Path;
use vortex::file::VORTEX_FILE_EXTENSION;
use vortex::layout::LayoutReader;
use vortex::metrics::DefaultMetricsRegistry;
use vortex::metrics::MetricsRegistry;
use vortex::session::VortexSession;
use vortex_utils::aliases::dash_map::DashMap;

use super::opener::VortexOpener;
use super::segment_cache::SharedSegmentCache;
use crate::ProjectionPushdown;
use crate::ScanConcurrency;
use crate::VortexTableOptions;
use crate::convert::exprs::DefaultExpressionConvertor;
use crate::convert::exprs::ExpressionConvertor;
use crate::persistent::reader::DefaultVortexReaderFactory;
use crate::persistent::reader::VortexReaderFactory;

/// Execution plan for reading one or more Vortex files, intended to be consumed by [`DataSourceExec`].
///
/// [`DataSourceExec`]: datafusion_datasource::source::DataSourceExec
#[derive(Clone)]
pub struct VortexSource {
    pub(crate) session: VortexSession,
    pub(crate) table_schema: TableSchema,
    pub(crate) projection: ProjectionExprs,
    /// Combined predicate expression containing all filters from `DataFusion` query planning.
    /// Used with `FilePruner` to skip files based on statistics and partition values.
    pub(crate) full_predicate: Option<PhysicalExprRef>,
    /// Subset of predicates that can be pushed down into Vortex scan operations.
    /// These are expressions that Vortex can efficiently evaluate during scanning.
    pub(crate) vortex_predicate: Option<PhysicalExprRef>,
    pub(crate) batch_size: Option<usize>,
    df_metrics: ExecutionPlanMetricsSet,
    /// Shared layout readers, the source only lives as long as one scan.
    ///
    /// Sharing the readers allows us to only read every layout once from the file, even across partitions.
    layout_readers: Arc<DashMap<Path, Weak<dyn LayoutReader>>>,
    /// Shared full-file natural split ranges keyed by path.
    natural_split_ranges: Arc<DashMap<Path, Arc<[Range<u64>]>>>,
    expression_convertor: Arc<dyn ExpressionConvertor>,
    pub(crate) vortex_reader_factory: Option<Arc<dyn VortexReaderFactory>>,
    vx_metrics_registry: Arc<dyn MetricsRegistry>,
    file_metadata_cache: Option<Arc<dyn FileMetadataCache>>,
    segment_cache: Option<Arc<SharedSegmentCache>>,
    target_partitions: Option<usize>,
    /// Whether to enable expression pushdown into the underlying Vortex scan.
    options: VortexTableOptions,
}

impl VortexSource {
    /// Creates a new `VortexSource` with default configuration and a provided [`VortexSession`].
    /// Meant to be use with a [`FileScanConfig`] to scan a file with the provided schema.
    ///
    /// Can be configured using the provided methods.
    #[must_use]
    pub fn new(table_schema: TableSchema, session: VortexSession) -> Self {
        let full_schema = table_schema.table_schema();
        let indices = (0..full_schema.fields().len()).collect::<Vec<_>>();
        let projection = ProjectionExprs::from_indices(&indices, full_schema);

        Self {
            session,
            table_schema,
            projection,
            full_predicate: None,
            vortex_predicate: None,
            batch_size: None,
            df_metrics: ExecutionPlanMetricsSet::default(),
            layout_readers: Arc::new(DashMap::default()),
            natural_split_ranges: Arc::new(DashMap::default()),
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            vortex_reader_factory: None,
            vx_metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            file_metadata_cache: None,
            segment_cache: None,
            target_partitions: None,
            options: VortexTableOptions::default(),
        }
    }

    /// Set projection-expression pushdown behavior for the underlying Vortex scan.
    #[must_use]
    pub fn with_projection_pushdown(mut self, mode: ProjectionPushdown) -> Self {
        self.options.projection_pushdown = mode;
        self
    }

    /// Set a [`ExpressionConvertor`] to control how Datafusion expression should be converted and pushed down.
    #[must_use]
    pub fn with_expression_convertor(
        mut self,
        expr_convertor: Arc<dyn ExpressionConvertor>,
    ) -> Self {
        self.expression_convertor = expr_convertor;
        self
    }

    /// Set a user-defined factory to create the underlying [`VortexReadAt`]
    ///
    /// [`VortexReadAt`]: vortex::io::VortexReadAt
    #[must_use]
    pub fn with_vortex_reader_factory(
        mut self,
        vortex_reader_factory: Arc<dyn VortexReaderFactory>,
    ) -> Self {
        self.vortex_reader_factory = Some(vortex_reader_factory);
        self
    }

    /// Returns the [`MetricsRegistry`] attached to this source.
    #[must_use]
    pub fn metrics_registry(&self) -> &Arc<dyn MetricsRegistry> {
        &self.vx_metrics_registry
    }

    /// Override the file metadata cache
    #[must_use]
    pub fn with_file_metadata_cache(
        mut self,
        file_metadata_cache: Arc<dyn FileMetadataCache>,
    ) -> Self {
        self.file_metadata_cache = Some(file_metadata_cache);
        self
    }

    #[must_use]
    pub(crate) fn with_segment_cache(mut self, segment_cache: Arc<SharedSegmentCache>) -> Self {
        self.segment_cache = Some(segment_cache);
        self
    }

    /// Set the underlying scan concurrency mode. This limit is used per Vortex scan operation.
    #[must_use]
    pub fn with_scan_concurrency(mut self, scan_concurrency: ScanConcurrency) -> Self {
        self.options.scan_concurrency = scan_concurrency;
        self
    }

    /// Returns the table options for this source.
    #[must_use]
    pub fn options(&self) -> &VortexTableOptions {
        &self.options
    }

    /// Set the table options for this source.
    #[must_use]
    pub fn with_options(mut self, opts: VortexTableOptions) -> Self {
        self.options = opts;
        self
    }
}

impl FileSource for VortexSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> DFResult<Arc<dyn FileOpener>> {
        let batch_size = self
            .batch_size
            .ok_or_else(|| exec_datafusion_err!("batch_size must be supplied to VortexSource"))?;

        let expr_adapter_factory = base_config
            .expr_adapter_factory
            .as_ref()
            .map_or_else(|| Arc::new(DefaultPhysicalExprAdapterFactory), Arc::clone);

        let vortex_reader_factory = self.vortex_reader_factory.as_ref().map_or_else(
            || Arc::new(DefaultVortexReaderFactory::new(object_store)),
            Arc::clone,
        );

        let planned_file_count = planned_file_count(&base_config.file_groups);
        let target_partitions = self
            .target_partitions
            .unwrap_or_else(|| base_config.file_groups.len().max(1));
        let scan_concurrency = resolve_scan_concurrency(
            self.options.scan_concurrency,
            target_partitions,
            planned_file_count,
            base_config.limit.is_some() && self.vortex_predicate.is_none(),
        );

        tracing::debug!(
            scan_concurrency,
            mode = %self.options.scan_concurrency,
            target_partitions,
            planned_partitions = base_config.file_groups.len(),
            planned_file_count,
            limit = ?base_config.limit,
            has_filter = self.vortex_predicate.is_some(),
            "Resolved Vortex scan concurrency"
        );

        let opener = VortexOpener {
            partition,
            session: self.session.clone(),
            vortex_reader_factory,
            projection: self.projection.clone(),
            filter: self.vortex_predicate.as_ref().map(Arc::clone),
            file_pruning_predicate: self.full_predicate.as_ref().map(Arc::clone),
            expr_adapter_factory,
            table_schema: self.table_schema.clone(),
            batch_size,
            limit: base_config.limit.map(|l| l as u64),
            metrics_registry: Arc::clone(&self.vx_metrics_registry),
            layout_readers: Arc::clone(&self.layout_readers),
            natural_split_ranges: Arc::clone(&self.natural_split_ranges),
            has_output_ordering: !base_config.output_ordering.is_empty(),
            expression_convertor: Arc::clone(&self.expression_convertor),
            file_metadata_cache: self.file_metadata_cache.as_ref().map(Arc::clone),
            segment_cache: self.segment_cache.as_ref().map(Arc::clone),
            projection_pushdown: self.options.projection_pushdown.enabled(),
            scan_concurrency: Some(scan_concurrency),
        };

        Ok(Arc::new(opener))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = Some(batch_size);
        Arc::new(source)
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.vortex_predicate.as_ref().map(Arc::clone)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.df_metrics
    }

    fn file_type(&self) -> &str {
        VORTEX_FILE_EXTENSION
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                if let Some(ref predicate) = self.vortex_predicate {
                    write!(f, ", predicate: {predicate}")?;
                }
            }
            // Use TreeRender style key=value formatting to display the predicate
            DisplayFormatType::TreeRender => {
                if let Some(ref predicate) = self.vortex_predicate {
                    writeln!(f, "predicate={}", fmt_sql(predicate.as_ref()))?;
                }
            }
        }
        Ok(())
    }

    fn supports_repartitioning(&self) -> bool {
        true
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> DFResult<Option<FileScanConfig>> {
        let target_partitions = target_partitions.max(1);
        let mut source = self.clone();
        source.target_partitions = Some(target_partitions);

        let mut updated_config = config.clone();
        updated_config.file_source = Arc::new(source);

        if config.file_compression_type.is_compressed() || !self.supports_repartitioning() {
            return Ok(Some(updated_config));
        }

        if let Some(repartitioned_file_groups) = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(repartition_file_min_size)
            .with_preserve_order_within_groups(output_ordering.is_some())
            .repartition_file_groups(&config.file_groups)
        {
            updated_config.file_groups = repartitioned_file_groups;
        }

        Ok(Some(updated_config))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        if filters.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![],
            ));
        }

        let mut source = self.clone();
        source.target_partitions = Some(config.execution.target_partitions.max(1));

        // Combine new filters with existing predicate for file pruning.
        // This full predicate is used by FilePruner to eliminate files.
        source.full_predicate = match source.full_predicate {
            Some(predicate) => Some(conjunction(
                std::iter::once(predicate).chain(filters.clone()),
            )),
            None => Some(conjunction(filters.clone())),
        };

        let supported_filters = filters
            .into_iter()
            .map(|expr| {
                if self
                    .expression_convertor
                    .can_be_pushed_down(&expr, self.table_schema.file_schema())
                {
                    PushedDownPredicate::supported(expr)
                } else {
                    PushedDownPredicate::unsupported(expr)
                }
            })
            .collect::<Vec<_>>();

        if supported_filters
            .iter()
            .all(|p| matches!(p.discriminant, PushedDown::No))
        {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::No; supported_filters.len()],
            )
            .with_updated_node(Arc::new(source) as _));
        }

        let supported = supported_filters
            .iter()
            .filter_map(|p| match p.discriminant {
                PushedDown::Yes => Some(&p.predicate),
                PushedDown::No => None,
            })
            .cloned();

        let predicate = match source.vortex_predicate {
            Some(predicate) => conjunction(std::iter::once(predicate).chain(supported)),
            None => conjunction(supported),
        };

        tracing::debug!(%predicate, "Saving predicate");

        source.vortex_predicate = Some(predicate);

        Ok(FilterPushdownPropagation::with_parent_pushdown_result(
            supported_filters.iter().map(|f| f.discriminant).collect(),
        )
        .with_updated_node(Arc::new(source) as _))
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> DFResult<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        source.projection = self.projection.try_merge(projection)?;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }
}

fn planned_file_count(file_groups: &[FileGroup]) -> usize {
    file_groups.iter().map(FileGroup::len).sum::<usize>().max(1)
}

fn resolve_scan_concurrency(
    mode: ScanConcurrency,
    target_partitions: usize,
    planned_file_count: usize,
    has_limit_without_filter: bool,
) -> usize {
    match mode {
        ScanConcurrency::Auto if has_limit_without_filter => 1,
        ScanConcurrency::Auto => target_partitions
            .max(1)
            .div_ceil(planned_file_count.max(1))
            .max(1),
        ScanConcurrency::Off => 1,
        ScanConcurrency::Explicit(value) => value.max(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_scan_concurrency_uses_file_count() {
        assert_eq!(
            resolve_scan_concurrency(ScanConcurrency::Auto, 16, 1, false),
            16
        );
        assert_eq!(
            resolve_scan_concurrency(ScanConcurrency::Auto, 16, 4, false),
            4
        );
        assert_eq!(
            resolve_scan_concurrency(ScanConcurrency::Auto, 16, 32, false),
            1
        );
    }

    #[test]
    fn auto_scan_concurrency_clamps_limit_without_filter_to_serial() {
        assert_eq!(
            resolve_scan_concurrency(ScanConcurrency::Auto, 16, 1, true),
            1
        );
    }

    #[test]
    fn explicit_and_off_scan_concurrency_override_auto() {
        assert_eq!(
            resolve_scan_concurrency(ScanConcurrency::Explicit(3), 16, 1, true),
            3
        );
        assert_eq!(
            resolve_scan_concurrency(ScanConcurrency::Off, 16, 1, false),
            1
        );
    }
}
