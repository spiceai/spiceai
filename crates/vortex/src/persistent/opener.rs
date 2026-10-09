// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;
use std::sync::Weak;

use arrow_schema::Field;
use arrow_schema::Schema;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::AsArray;
use datafusion_common::arrow::array::RecordBatch;
use datafusion_common::exec_datafusion_err;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::TableSchema;
use datafusion_datasource::file_stream::FileOpenFuture;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use datafusion_expr::Operator;
use datafusion_physical_expr::DynamicFilterTracking;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr::split_conjunction;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use datafusion_physical_plan::expressions as df_expr;
use datafusion_physical_plan::metrics::Count;
use datafusion_pruning::FilePruner;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use futures::stream::BoxStream;
use itertools::Itertools;
use object_store::path::Path;
use tracing::Instrument;
use vortex::array::MaskFuture;
use vortex::array::VortexSessionExecute;
use vortex::array::expr::Expression;
use vortex::array::expr::forms::conjuncts;
use vortex::array::expr::root;
use vortex::array::expr::transform::replace_root_fields;
use vortex::arrow::ArrowSessionExt;
use vortex::dtype::DType;
use vortex::dtype::FieldMask;
use vortex::error::VortexError;
use vortex::error::VortexExpect;
use vortex::error::VortexResult;
use vortex::error::vortex_err;
use vortex::expr::BoundExpression;
use vortex::file::OpenOptionsSessionExt;
use vortex::io::InstrumentedReadAt;
use vortex::layout::LayoutReader;
use vortex::layout::scan::scan_builder::ScanBuilder;
use vortex::layout::scan::split_by::SplitBy;
use vortex::mask::Mask;
use vortex::metrics::Label;
use vortex::metrics::MetricsRegistry;
use vortex::session::VortexSession;
use vortex_utils::aliases::dash_map::DashMap;
use vortex_utils::aliases::dash_map::Entry;

use crate::VortexAccessPlan;
use crate::VortexRuntimeAccessPlanProvider;
use crate::convert::exprs::ExpressionConvertor;
use crate::convert::exprs::ProcessedProjection;
use crate::convert::exprs::make_vortex_predicate;
use crate::convert::schema::calculate_physical_schema;
use crate::metrics::PARTITION_LABEL;
use crate::metrics::PATH_LABEL;
use crate::persistent::cache::CachedVortexMetadata;
use crate::persistent::deferred_projection::DeferredProjectionReader;
use crate::persistent::key_blocks;
use crate::persistent::reader::VortexReaderFactory;
use crate::persistent::segment_cache::SharedSegmentCache;
use crate::persistent::stream::PrunableStream;

#[derive(Clone)]
pub(crate) struct VortexOpener {
    /// The partition this opener is assigned to. Only used for labeling metrics.
    pub partition: usize,
    pub session: VortexSession,
    pub vortex_reader_factory: Arc<dyn VortexReaderFactory>,
    /// Optional table schema projection. The indices are w.r.t. the `table_schema`, which is
    /// all fields in the final scan result not including the partition columns.
    pub projection: ProjectionExprs,
    /// Filter expression optimized for pushdown into Vortex scan operations.
    /// This may be a subset of `file_pruning_predicate` containing only expressions
    /// that Vortex can efficiently evaluate.
    pub filter: Option<PhysicalExprRef>,
    /// Filter expression used by `DataFusion`'s `FilePruner` to eliminate files based on
    /// statistics and partition values without opening them.
    pub file_pruning_predicate: Option<PhysicalExprRef>,
    pub expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    /// This is the table's schema without partition columns. It may contain fields which do
    /// not exist in the file. Missing columns are null-filled during batch remapping.
    pub table_schema: TableSchema,
    /// A hint for the desired row count of record batches returned from the scan.
    pub batch_size: usize,
    /// If provided, the scan will not return more than this many rows.
    pub limit: Option<u64>,
    /// A metrics object for tracking performance of the scan.
    pub metrics_registry: Arc<dyn MetricsRegistry>,
    /// A shared cache of file readers.
    ///
    /// To save on the overhead of reparsing `FlatBuffers` and rebuilding the layout tree, we cache
    /// a file reader the first time we read a file.
    pub layout_readers: Arc<DashMap<Path, Weak<dyn LayoutReader>>>,
    /// Shared full-file natural split ranges keyed by file path.
    pub natural_split_ranges: Arc<DashMap<Path, Arc<[Range<u64>]>>>,
    /// Whether the query has output ordering specified
    pub has_output_ordering: bool,

    pub expression_convertor: Arc<dyn ExpressionConvertor>,
    pub file_metadata_cache: Option<Arc<FileMetadataCache>>,
    pub segment_cache: Option<Arc<SharedSegmentCache>>,
    /// URL of the object store this scan reads from. Part of every segment-cache
    /// key: `ObjectMeta::location` is store-relative, and the cache is shared by
    /// every table, so two stores could otherwise collide on the same path.
    pub object_store_url: Arc<str>,
    /// Whether to enable expression pushdown into the underlying Vortex scan.
    pub projection_pushdown: bool,
    pub scan_concurrency: Option<usize>,
    /// Provider consulted after runtime dynamic filters have been populated.
    pub runtime_access_plan_provider: Option<Arc<dyn VortexRuntimeAccessPlanProvider>>,
    /// Column whose equality predicates a whole-file scan answers from the file's
    /// key blocks (see [`key_blocks`]).
    pub key_column: Option<Arc<str>>,
}

/// Most candidate ranges a point read serves; more fall back to a scan.
const POINT_READ_MAX_RANGES: usize = 4;

/// Most candidate rows a point read serves. A point read evaluates each range in
/// one piece rather than streaming it in splits, so this bounds what it decodes.
const POINT_READ_MAX_ROWS: u64 = 16 * key_blocks::BLOCK_ROWS;

impl FileOpener for VortexOpener {
    fn open(&self, file: PartitionedFile) -> DFResult<FileOpenFuture> {
        let session = self.session.clone();
        let metrics_registry = Arc::clone(&self.metrics_registry);
        let labels = vec![
            Label::new(PATH_LABEL, file.path().to_string()),
            Label::new(PARTITION_LABEL, self.partition.to_string()),
        ];

        let mut projection = self.projection.clone();
        let mut filter = self.filter.clone();

        let reader = self
            .vortex_reader_factory
            .create_reader(file.path().as_ref(), &session)?;

        let reader =
            InstrumentedReadAt::new_with_labels(reader, metrics_registry.as_ref(), labels.clone());

        let mut file_pruning_predicate = self.file_pruning_predicate.as_ref().map(Arc::clone);
        let expr_adapter_factory = Arc::clone(&self.expr_adapter_factory);
        let file_metadata_cache = self.file_metadata_cache.as_ref().map(Arc::clone);
        let segment_cache = self.segment_cache.as_ref().map(Arc::clone);
        let object_store_url = Arc::clone(&self.object_store_url);

        let unified_file_schema = Arc::clone(self.table_schema.file_schema());
        let batch_size = self.batch_size;
        let limit = self.limit;
        let layout_reader = Arc::clone(&self.layout_readers);
        let natural_split_ranges = Arc::clone(&self.natural_split_ranges);
        let has_output_ordering = self.has_output_ordering;
        let scan_concurrency = self.scan_concurrency;

        let expr_convertor = Arc::clone(&self.expression_convertor);
        let projection_pushdown = self.projection_pushdown;
        let runtime_access_plan_provider =
            self.runtime_access_plan_provider.as_ref().map(Arc::clone);
        let runtime_predicate = self.filter.as_ref().map(Arc::clone);
        let key_column = self.key_column.as_ref().map(Arc::clone);

        // Replace column access for partition columns with literals
        let literal_value_cols: std::collections::HashMap<String, ScalarValue> = self
            .table_schema
            .table_partition_cols()
            .iter()
            .map(|f| f.name())
            .cloned()
            .zip(file.partition_values.clone())
            .collect();

        if !literal_value_cols.is_empty() {
            projection = projection.try_map_exprs(|expr| {
                replace_columns_with_literals(Arc::clone(&expr), &literal_value_cols)
            })?;
            filter = filter
                .map(|p| replace_columns_with_literals(p, &literal_value_cols))
                .transpose()?;
            // `FilePruner` evaluates its predicate against the file schema, which
            // has no partition columns, so it expects them already folded to this
            // file's values.
            file_pruning_predicate = file_pruning_predicate
                .map(|p| replace_columns_with_literals(p, &literal_value_cols))
                .transpose()?;
        }

        Ok(async move {
            let runtime_access_plan = match runtime_access_plan_provider.as_ref() {
                Some(provider) => {
                    provider
                        .runtime_access_plan_for_file(&file, runtime_predicate.as_ref())
                        .await
                }
                None => None,
            };

            // A runtime index may prove that this file has no candidate rows. Return
            // before opening the Vortex footer or constructing its layout reader.
            if runtime_access_plan
                .as_deref()
                .is_some_and(VortexAccessPlan::is_empty)
            {
                return Ok(stream::empty().boxed());
            }

            // A key lookup on a file whose key blocks are already cached is decided
            // here, before the file is opened. A file with no block that can hold the
            // key is skipped. Otherwise the file pruner is not built: its statistics
            // cannot rule out a key the blocks hold, and a predicate that is not
            // dynamic gives it nothing to re-check while the file is read.
            let early_key_ranges = match (key_column.as_ref(), filter.as_ref(), &file.range) {
                (Some(column), Some(predicate), None) if !contains_dynamic_filter(predicate) => {
                    match key_equality(predicate, column, &unified_file_schema) {
                        Some(key) => key_blocks::cached_key_blocks(
                            &object_store_url,
                            &file.object_meta,
                            column,
                        )
                        .await
                        .map(|blocks| blocks.candidate_ranges(key)),
                        None => None,
                    }
                }
                _ => None,
            };
            if early_key_ranges.as_ref().is_some_and(Vec::is_empty) {
                return Ok(stream::empty().boxed());
            }

            // Create FilePruner when we have a predicate and either dynamic expressions
            // or file statistics available. The pruner can eliminate files without
            // opening them based on:
            // - Partition column values (e.g., date=2024-01-01)
            // - File-level statistics (min/max values per column)
            let mut file_pruner = file_pruning_predicate
                .filter(|_| early_key_ranges.is_none())
                .filter(|p| {
                    // Only create pruner if we have dynamic expressions or file statistics
                    // to work with. Static predicates without stats won't benefit from pruning.
                    contains_dynamic_filter(p) || file.has_statistics()
                })
                .and_then(|predicate| {
                    FilePruner::try_new(
                        Arc::clone(&predicate),
                        &unified_file_schema,
                        &file,
                        Count::default(),
                    )
                });

            // Check if this file should be pruned based on statistics/partition values.
            // Returns empty stream if file can be skipped entirely.
            if let Some(file_pruner) = file_pruner.as_mut()
                && file_pruner.should_prune()?
            {
                return Ok(stream::empty().boxed());
            }

            let mut open_opts = session
                .open_options()
                .with_file_size(file.object_meta.size)
                .with_metrics_registry(Arc::clone(&metrics_registry))
                .with_labels(labels);

            if let Some(segment_cache) = segment_cache {
                open_opts = open_opts.with_segment_cache(segment_cache.for_path(
                    Arc::clone(&object_store_url),
                    file.object_meta.location.clone(),
                ));
            }

            if let Some(file_metadata_cache) = file_metadata_cache
                && let Some(entry) = file_metadata_cache.get(file.path())
                && entry.is_valid_for(&file.object_meta)
                && let Some(vortex_metadata) = entry
                    .file_metadata
                    .as_any()
                    .downcast_ref::<CachedVortexMetadata>()
            {
                open_opts = open_opts.with_footer(vortex_metadata.footer().clone());
            }

            let vxf = open_opts
                .open_read(reader)
                .await
                .map_err(|e| exec_datafusion_err!("Failed to open Vortex file {e}"))?;

            // This is the expected arrow types of the actual columns in the file, which might have different types
            // from the unified logical schema or miss
            let this_file_schema = Arc::new(calculate_physical_schema(
                vxf.dtype(),
                &unified_file_schema,
                &session.arrow(),
            )?);

            let projected_physical_schema = projection.project_schema(&unified_file_schema)?;

            let expr_adapter = expr_adapter_factory.create(
                Arc::clone(&unified_file_schema),
                Arc::clone(&this_file_schema),
            )?;

            let simplifier = PhysicalExprSimplifier::new(&this_file_schema);

            // The adapter rewrites the expressions to the local file schema, allowing
            // for schema evolution and divergence between the table's schema and individual files.
            let filter = filter
                .map(|filter| {
                    // Expression might now reference columns that don't exist in the file, so we can give it
                    // another simplification pass.
                    simplifier.simplify(expr_adapter.rewrite(filter)?)
                })
                .transpose()?;
            let projection =
                projection.try_map_exprs(|p| simplifier.simplify(expr_adapter.rewrite(p)?))?;

            let ProcessedProjection {
                scan_projection,
                leftover_projection,
            } = if projection_pushdown {
                expr_convertor.split_projection(
                    projection.clone(),
                    &this_file_schema,
                    &projected_physical_schema,
                )?
            } else {
                // When projection pushdown is disabled, read only the required columns
                // and apply the full projection after the scan.
                expr_convertor.no_pushdown_projection(projection.clone(), &this_file_schema)?
            };

            // The schema of the stream returned from the vortex scan.
            // We use a reference schema for types that don't roundtrip (Dictionary, Utf8, etc.).
            // The scan takes the projection bound to the file's type; a point read
            // optimizes the unbound projection against that type before binding it.
            let bound_scan_projection = scan_projection.bind(vxf.dtype()).map_err(|e| {
                exec_datafusion_err!("Couldn't get the dtype for the underlying Vortex scan: {e}")
            })?;
            let scan_dtype = bound_scan_projection.dtype().clone();

            // When projection pushdown is enabled, the scan outputs the projected columns.
            // When disabled, the scan outputs raw columns and the projection is applied after.
            let scan_reference_schema = if projection_pushdown {
                projected_physical_schema
            } else {
                // Build schema from the raw columns being read
                let column_indices = projection.column_indices();
                let fields: Vec<_> = column_indices
                    .into_iter()
                    .map(|idx| this_file_schema.field(idx).clone())
                    .collect();
                Schema::new_with_metadata(fields, this_file_schema.metadata().clone())
            };
            let stream_schema =
                calculate_physical_schema(&scan_dtype, &scan_reference_schema, &session.arrow())?;

            let leftover_projection = leftover_projection
                .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;
            let projector = leftover_projection.make_projector(&stream_schema)?;

            // We share our layout readers with others partitions in the scan, so we can only need to read each layout in each file once.
            let layout_reader = match layout_reader.entry(file.object_meta.location.clone()) {
                Entry::Occupied(mut occupied_entry) => {
                    if let Some(reader) = occupied_entry.get().upgrade() {
                        tracing::trace!("reusing layout reader for {}", occupied_entry.key());
                        reader
                    } else {
                        tracing::trace!("creating layout reader for {}", occupied_entry.key());
                        let reader = vxf.layout_reader().map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to create layout reader: {e}"
                            ))
                        })?;
                        occupied_entry.insert(Arc::downgrade(&reader));
                        reader
                    }
                }
                Entry::Vacant(vacant_entry) => {
                    tracing::trace!("creating layout reader for {}", vacant_entry.key());
                    let reader = vxf.layout_reader().map_err(|e| {
                        DataFusionError::Execution(format!("Failed to create layout reader: {e}"))
                    })?;
                    vacant_entry.insert(Arc::downgrade(&reader));

                    reader
                }
            };

            // Resolved before the filter below so that a split whose byte range covers no
            // whole row group still short-circuits to an empty stream, rather than
            // reporting a pushdown failure it will never act on.
            let row_range = match file.range {
                Some(file_range) => {
                    let natural_split_ranges = natural_split_ranges_for_file(
                        natural_split_ranges.as_ref(),
                        &file.object_meta.location,
                        &layout_reader,
                    )?;
                    let byte_range = Range {
                        start: u64::try_from(file_range.start).map_err(|_| {
                            exec_datafusion_err!("Vortex file range start is negative")
                        })?,
                        end: u64::try_from(file_range.end).map_err(|_| {
                            exec_datafusion_err!("Vortex file range end is negative")
                        })?,
                    };

                    let Some(row_range) = split_aligned_row_range(
                        byte_range,
                        file.object_meta.size,
                        natural_split_ranges.as_ref(),
                    ) else {
                        return Ok(stream::empty().boxed());
                    };

                    Some(row_range)
                }
                None => None,
            };

            // An equality on the key column of a whole-file scan can only match inside
            // the file's key blocks that hold the key: none means the file has no match,
            // and a few are read directly below instead of scanned. A dynamic filter
            // changes after the file opens, so it keeps the scan.
            let key_ranges = match (key_column.as_ref(), filter.as_ref(), &row_range) {
                _ if early_key_ranges.is_some() => early_key_ranges,
                (Some(column), Some(predicate), None) if !contains_dynamic_filter(predicate) => {
                    match key_equality(predicate, column, &this_file_schema) {
                        Some(key) => key_blocks::key_blocks(
                            &layout_reader,
                            &session,
                            &object_store_url,
                            &file.object_meta,
                            column,
                        )
                        .await
                        .map(|blocks| blocks.candidate_ranges(key)),
                        None => None,
                    }
                }
                _ => None,
            };
            if key_ranges.as_ref().is_some_and(Vec::is_empty) {
                return Ok(stream::empty().boxed());
            }

            // Stats-layout pruning chain (Vortex 0.74 `FileStatsLayoutReader` / zoned
            // `StatFn`). The conjuncts collected here are translated to a Vortex
            // `Expression` and handed to `ScanBuilder::with_some_filter` below. Inside
            // Vortex the zoned layout reader rewrites that expression into a stats
            // predicate (`Expression::falsify`) and prunes whole zones whose min/max
            // can't satisfy it (`ZoneMap::prune`). Dynamic hash-join filters (the
            // InList fragments produced by the native dynamic-filter pass) flow through
            // the same path: `collect_vortex_pushdown_conjunct` unwraps
            // `DynamicFilterPhysicalExpr` via `.current()` at file-open time (not plan
            // build time), and Vortex's `PruningResult::mask()` re-derives the zone mask
            // whenever the dynamic expression's version advances — so a build side that
            // populates after the scan starts still prunes zones. `VortexAccessPlan` only
            // adds a row `Selection`; it does not bypass this filter, so stats pruning
            // still engages under position-delete scans. Filters Vortex can't translate
            // (`skipped_dynamic`) are dropped here but still feed the coarser per-file
            // `FilePruner`/`PrunableStream` above.
            let filter = filter
                .and_then(|f| {
                    // Verify that all filters we've accepted from DataFusion get pushed down.
                    // This will only fail if the user has not configured a suitable
                    // PhysicalExprAdapterFactory on the file source to handle rewriting the
                    // expression to handle missing/reordered columns in the Vortex file.

                    let PushdownConjuncts {
                        pushed,
                        unpushed,
                        skipped_dynamic,
                    } = match split_vortex_pushdown_conjuncts(
                        expr_convertor.as_ref(),
                        &f,
                        &this_file_schema,
                    ) {
                        Ok(conjuncts) => conjuncts,
                        Err(err) => return Some(Err(err)),
                    };

                    if !unpushed.is_empty() {
                        tracing::debug!(filters = ?unpushed, "VortexSource accepted filters that could not be pushed down");
                        return Some(Err(exec_datafusion_err!("VortexSource accepted but failed to push {} filters; configure a PhysicalExprAdapterFactory that can rewrite missing or reordered columns before pushdown", unpushed.len())));
                    }

                    if !skipped_dynamic.is_empty() {
                        tracing::debug!(filters = ?skipped_dynamic, "Skipping dynamic filter fragments that Vortex can't push down");
                    }

                    match make_vortex_predicate(expr_convertor.as_ref(), &pushed) {
                        Ok(predicate) => predicate.map(Ok),
                        Err(err) => Some(Err(err)),
                    }
                })
                .transpose()?;
            // The scan takes the filter bound to the file's type; a point read splits
            // and optimizes the unbound filter before binding each conjunct.
            let bound_filter = filter
                .as_ref()
                .map(|predicate| predicate.bind(vxf.dtype()))
                .transpose()
                .map_err(|e| exec_datafusion_err!("Failed to bind the Vortex filter to the file's type: {e}"))?;

            // A point read needs the whole filter in Vortex form and no row selection: a
            // planning-time or runtime access plan (deleted rows, say) is applied by the
            // scan builder, which a point read does not use.
            let point_read = key_ranges.filter(|ranges| {
                filter.is_some()
                    && ranges.len() <= POINT_READ_MAX_RANGES
                    && ranges.iter().map(|range| range.end - range.start).sum::<u64>()
                        <= POINT_READ_MAX_ROWS
                    && file.extensions.get::<VortexAccessPlan>().is_none()
                    && runtime_access_plan.is_none()
            });

            let stream_target_field = Field::new_struct("", stream_schema.fields().clone(), false);
            let batches = if let Some(ranges) = point_read
                && let Some(predicate) = filter.as_ref()
            {
                #[cfg(test)]
                tests::record_point_read();
                point_read_stream(
                    layout_reader,
                    session,
                    ranges,
                    predicate,
                    &scan_projection,
                    stream_target_field,
                )
                .map_err(|e| exec_datafusion_err!("Failed to create Vortex point read: {e}"))?
            } else {
                let filter = bound_filter;
                // Drop a split whose zones cannot satisfy the filter before the scan is
                // built. Vortex prunes these same zones inside the scan, but only after
                // `ScanBuilder::build` has optimized the projection and the filter
                // against the file's dtype — work a split that will read nothing should
                // not pay for, and which is repeated for every split the file is divided
                // into. A file is split by byte range for parallelism and each split
                // inherits the whole file's statistics, so `FilePruner` above cannot
                // separate them; only the zone map can.
                //
                // `pruning_evaluation` returns a mask whose false lanes are *proven*
                // false for the expression, so an all-false mask is a sound skip. The
                // zone map it reads is memoized on the layout reader, which
                // `layout_readers` shares with every other split of this file, so the
                // read happens once per file rather than once per split.
                if let Some(predicate) = filter.as_ref() {
                    let prune_range = row_range
                        .clone()
                        .unwrap_or_else(|| 0..layout_reader.row_count());
                    let prune_len = usize::try_from(prune_range.end - prune_range.start)
                        .map_err(|_| exec_datafusion_err!("Vortex split row range exceeds usize"))?;
                    let pruned = layout_reader
                        .pruning_evaluation(&prune_range, predicate, Mask::new_true(prune_len))
                        .map_err(|e| {
                            exec_datafusion_err!("Failed to build Vortex zone pruning: {e}")
                        })?
                        .await
                        .map_err(|e| {
                            exec_datafusion_err!("Failed to evaluate Vortex zone pruning: {e}")
                        })?;
                    if pruned.all_false() {
                        return Ok(stream::empty().boxed());
                    }
                }

                // Built after the filter so we know whether there is one: a filtered scan
                // discards splits whose mask comes back empty, and deferring projection setup
                // keeps those splits from registering reads for the output columns. An
                // unfiltered scan has nothing to wait on, and its eager registration is what
                // lets the read driver coalesce adjacent splits, so it keeps the plain reader.
                let layout_reader: Arc<dyn LayoutReader> = if filter.is_some() {
                    Arc::new(DeferredProjectionReader::new(layout_reader))
                } else {
                    layout_reader
                };
                #[cfg(test)]
                tests::record_scan_built();

                let mut scan_builder = ScanBuilder::new(session.clone(), layout_reader);

                // A runtime plan narrows the planning-time plan rather than replacing
                // it, so rows the planning-time plan excludes (deleted rows, say) stay
                // excluded whatever the runtime provider returns.
                match (
                    file.extensions.get::<VortexAccessPlan>(),
                    runtime_access_plan.as_deref(),
                ) {
                    (Some(planned), Some(runtime)) => {
                        scan_builder = planned.intersect(runtime).apply_to_builder(scan_builder);
                    }
                    (Some(plan), None) | (None, Some(plan)) => {
                        scan_builder = plan.apply_to_builder(scan_builder);
                    }
                    (None, None) => {}
                }

                if let Some(row_range) = row_range {
                    scan_builder = scan_builder.with_row_range(row_range);
                }

                if let Some(limit) = limit
                    && filter.is_none()
                {
                    scan_builder = scan_builder.with_limit(limit);
                }

                if let Some(concurrency) = scan_concurrency {
                    // Absolute, not per-worker: this count is charged to the query
                    // memory pool one decoded batch at a time, and it is capped
                    // against the process's CPU entitlement. The per-worker form
                    // multiplies by `available_parallelism`, which reports the
                    // machine's cores rather than the share a cgroup granted, so
                    // neither the charge nor the cap would mean what it says.
                    scan_builder = scan_builder.with_absolute_concurrency(concurrency);
                }

                scan_builder
                    .with_metrics_registry(metrics_registry)
                    .with_projection(bound_scan_projection)
                    .with_some_filter(filter)
                    .with_ordered(has_output_ordering)
                    .map(move |chunk| {
                        let mut ctx = session.create_execution_ctx();
                        let arrow_session = ctx.session().clone();
                        let arrow = arrow_session.arrow().execute_arrow(
                            chunk,
                            Some(&stream_target_field),
                            &mut ctx,
                        )?;
                        Ok(RecordBatch::from(arrow.as_struct().clone()))
                    })
                    .into_stream()
                    .map_err(|e| exec_datafusion_err!("Failed to create Vortex stream: {e}"))?
                    .boxed()
            };
            let stream = batches
                .map_ok(move |rb| {
                    // We try and slice the stream into respecting datafusion's configured batch size.
                    stream::iter(
                        (0..rb.num_rows().div_ceil(batch_size * 2))
                            .flat_map(move |block_idx| {
                                let offset = block_idx * batch_size * 2;

                                // If we have less than two batches worth of rows left, we keep them together as a single batch.
                                if rb.num_rows() - offset < 2 * batch_size {
                                    let length = rb.num_rows() - offset;
                                    [Some(rb.slice(offset, length)), None].into_iter()
                                } else {
                                    let first = rb.slice(offset, batch_size);
                                    let second = rb.slice(offset + batch_size, batch_size);
                                    [Some(first), Some(second)].into_iter()
                                }
                            })
                            .flatten()
                            .map(Ok),
                    )
                })
                .map_err(move |e: VortexError| {
                    DataFusionError::External(Box::new(e.with_context(format!(
                        "Failed to read Vortex file: {}",
                        file.object_meta.location
                    ))))
                })
                .try_flatten()
                .map(move |batch| {
                    if projector.projection().as_ref().is_empty() {
                        batch
                    } else {
                        batch.and_then(|b| projector.project_batch(&b))
                    }
                })
                .boxed();

            if let Some(file_pruner) = file_pruner {
                Ok(PrunableStream::new(file_pruner, stream).boxed())
            } else {
                Ok(stream)
            }
        }
        .in_current_span()
        .boxed())
    }
}

/// The integer `column` must equal for `predicate` to hold: the literal of the first
/// conjunct comparing `column` for equality with an integer literal, on a column
/// whose type in `schema` has key blocks. `None` for any other predicate.
fn key_equality(predicate: &PhysicalExprRef, column: &str, schema: &Schema) -> Option<i64> {
    let data_type = schema.field_with_name(column).ok()?.data_type();
    if !key_blocks::is_indexable(data_type) {
        return None;
    }
    split_conjunction(predicate)
        .into_iter()
        .find_map(|conjunct| {
            let binary = conjunct.downcast_ref::<df_expr::BinaryExpr>()?;
            if *binary.op() != Operator::Eq {
                return None;
            }
            let (left, right) = (binary.left(), binary.right());
            let (key, literal) = match (
                left.downcast_ref::<df_expr::Column>(),
                right.downcast_ref::<df_expr::Literal>(),
            ) {
                (Some(key), Some(literal)) => (key, literal),
                _ => (
                    right.downcast_ref::<df_expr::Column>()?,
                    left.downcast_ref::<df_expr::Literal>()?,
                ),
            };
            if key.name() != column {
                return None;
            }
            key_blocks::integer_literal(literal.value())
        })
}

/// Reads the rows of `ranges` that satisfy `filter` straight from `reader`, one
/// range at a time and in row order.
///
/// Each range is evaluated the way a scan split is — every filter conjunct over
/// the range, then the projection of the rows that pass — without the work a
/// `ScanBuilder` does first: zone-map pruning across the whole file, split
/// planning, and preparing the scan on the blocking pool. That work is sized to
/// the file, and a key lookup has only a few blocks left to read.
fn point_read_stream(
    reader: Arc<dyn LayoutReader>,
    session: VortexSession,
    ranges: Vec<Range<u64>>,
    filter: &Expression,
    projection: &Expression,
    target: Field,
) -> VortexResult<BoxStream<'static, VortexResult<RecordBatch>>> {
    let dtype = reader.dtype();
    let conjuncts: Arc<[BoundExpression]> = conjuncts(&filter.optimize_recursive(dtype)?)
        .iter()
        .map(|conjunct| conjunct.bind(dtype))
        .collect::<VortexResult<Vec<_>>>()?
        .into();
    // A projection of every field in file order is the root itself. The struct
    // reader rewrites a projection against its own expansion of the root before
    // partitioning it by field; handed `root()`, it has nothing to rewrite.
    let projection = if is_identity_projection(projection, dtype) {
        root()
    } else {
        projection.optimize_recursive(dtype)?
    }
    .bind(dtype)?;
    let reads = ranges.into_iter().map(move |range| {
        read_range(
            Arc::clone(&reader),
            session.clone(),
            range,
            Arc::clone(&conjuncts),
            projection.clone(),
            target.clone(),
        )
    });
    Ok(stream::iter(reads)
        .buffered(POINT_READ_MAX_RANGES)
        .try_filter_map(|batch| async move { Ok(batch) })
        .boxed())
}

/// Whether `projection` selects every field of the non-nullable struct `dtype`,
/// in order and under its own name — the expansion of `root()` a struct reader
/// builds for `dtype`.
fn is_identity_projection(projection: &Expression, dtype: &DType) -> bool {
    !dtype.is_nullable()
        && dtype
            .as_struct_fields_opt()
            .is_some_and(|fields| *projection == replace_root_fields(root(), fields))
}

async fn read_range(
    reader: Arc<dyn LayoutReader>,
    session: VortexSession,
    range: Range<u64>,
    conjuncts: Arc<[BoundExpression]>,
    projection: BoundExpression,
    target: Field,
) -> VortexResult<Option<RecordBatch>> {
    let rows = usize::try_from(range.end - range.start)
        .map_err(|_| vortex_err!("Vortex point read range exceeds usize"))?;
    // Each evaluation returns its input mask narrowed to the rows that pass.
    let mut mask = Mask::new_true(rows);
    for conjunct in conjuncts.iter() {
        mask = reader
            .filter_evaluation(&range, conjunct, MaskFuture::ready(mask))?
            .await?;
        if mask.all_false() {
            return Ok(None);
        }
    }
    let array = reader
        .projection_evaluation(&range, &projection, MaskFuture::ready(mask))?
        .await?;
    let mut ctx = session.create_execution_ctx();
    let arrow_session = ctx.session().clone();
    let arrow = arrow_session
        .arrow()
        .execute_arrow(array, Some(&target), &mut ctx)?;
    let batch = arrow
        .as_struct_opt()
        .ok_or_else(|| vortex_err!("Vortex point read did not produce a struct array"))?;
    Ok(Some(RecordBatch::from(batch.clone())))
}

struct PushdownConjuncts {
    pushed: Vec<PhysicalExprRef>,
    unpushed: Vec<PhysicalExprRef>,
    skipped_dynamic: Vec<PhysicalExprRef>,
}

fn split_vortex_pushdown_conjuncts(
    expr_convertor: &dyn ExpressionConvertor,
    expr: &PhysicalExprRef,
    schema: &Schema,
) -> DFResult<PushdownConjuncts> {
    let mut conjuncts = PushdownConjuncts {
        pushed: Vec::new(),
        unpushed: Vec::new(),
        skipped_dynamic: Vec::new(),
    };

    for conjunct in split_conjunction(expr).into_iter().cloned() {
        collect_vortex_pushdown_conjunct(expr_convertor, conjunct, schema, false, &mut conjuncts)?;
    }

    Ok(conjuncts)
}

fn collect_vortex_pushdown_conjunct(
    expr_convertor: &dyn ExpressionConvertor,
    expr: PhysicalExprRef,
    schema: &Schema,
    from_dynamic_filter: bool,
    conjuncts: &mut PushdownConjuncts,
) -> DFResult<()> {
    if let Some(dynamic_filter) = expr.downcast_ref::<df_expr::DynamicFilterPhysicalExpr>() {
        let current = match dynamic_filter.current() {
            Ok(current) => current,
            Err(err) => {
                tracing::debug!(error = %err, filter = ?expr, "Skipping dynamic filter that is not ready for Vortex pushdown");
                conjuncts.skipped_dynamic.push(expr);
                return Ok(());
            }
        };
        for conjunct in split_conjunction(&current).into_iter().cloned() {
            collect_vortex_pushdown_conjunct(expr_convertor, conjunct, schema, true, conjuncts)?;
        }
        return Ok(());
    }

    // Decline the *membership* (`InList`) conjunct of a hash-join dynamic filter.
    // Vortex evaluates an `InList` with the O(N×M) `list_contains` kernel per row, which
    // dominates scan time for large build-side lists.
    if from_dynamic_filter && expr.is::<df_expr::InListExpr>() {
        conjuncts.skipped_dynamic.push(expr);
        return Ok(());
    }

    if expr_convertor.can_be_pushed_down(&expr, schema) {
        conjuncts.pushed.push(expr);
    } else if from_dynamic_filter || contains_dynamic_filter(&expr) {
        conjuncts.skipped_dynamic.push(expr);
    } else {
        conjuncts.unpushed.push(expr);
    }

    Ok(())
}

fn natural_split_ranges_for_file(
    natural_split_ranges: &DashMap<Path, Arc<[Range<u64>]>>,
    path: &Path,
    layout_reader: &Arc<dyn LayoutReader>,
) -> DFResult<Arc<[Range<u64>]>> {
    if let Some(split_ranges) = natural_split_ranges.get(path) {
        return Ok(Arc::clone(split_ranges.value()));
    }

    let split_ranges = compute_natural_split_ranges(layout_reader.as_ref())?;

    match natural_split_ranges.entry(path.clone()) {
        Entry::Occupied(entry) => Ok(Arc::clone(entry.get())),
        Entry::Vacant(entry) => {
            entry.insert(Arc::clone(&split_ranges));
            Ok(split_ranges)
        }
    }
}

/// Whether `expr` holds a dynamic filter (for example a hash-join or `TopK` bound), whose
/// value can change after planning.
fn contains_dynamic_filter(expr: &PhysicalExprRef) -> bool {
    DynamicFilterTracking::classify(expr).contains_dynamic_filter()
}

fn compute_natural_split_ranges(layout_reader: &dyn LayoutReader) -> DFResult<Arc<[Range<u64>]>> {
    let row_count = layout_reader.row_count();
    let row_range = 0..row_count;
    let split_points: Vec<_> = SplitBy::Layout
        .splits(layout_reader, &row_range, &[FieldMask::All])
        .map_err(|e| exec_datafusion_err!("Failed to compute Vortex natural splits: {e}"))?
        .into_iter()
        .tuple_windows()
        .map(|(s, e)| s..e)
        .collect::<Vec<_>>();

    Ok(split_points.into())
}

/// Translate a `DataFusion` byte range to the contiguous natural split ranges it owns.
fn split_aligned_row_range(
    byte_range: Range<u64>,
    total_size: u64,
    split_ranges: &[Range<u64>],
) -> Option<Range<u64>> {
    if byte_range.start >= byte_range.end {
        return None;
    }

    let row_count = split_ranges.last().map(|split| split.end)?;
    if row_count == 0 {
        return None;
    }

    let mut owned_splits = split_ranges.iter().filter(|split_range| {
        let midpoint_byte = split_midpoint_to_byte(split_range, row_count, total_size);
        byte_range.contains(&midpoint_byte)
    });

    let first_split = owned_splits.next()?;
    let mut row_range = first_split.start..first_split.end;
    for split_range in owned_splits {
        row_range.end = split_range.end;
    }

    Some(row_range)
}

fn split_midpoint_to_byte(split_range: &Range<u64>, row_count: u64, total_size: u64) -> u64 {
    let midpoint_row = split_range.start + (split_range.end - split_range.start) / 2;
    let midpoint_byte = (u128::from(midpoint_row) * u128::from(total_size)) / u128::from(row_count);

    u64::try_from(midpoint_byte).vortex_expect("midpoint byte projection should fit into u64")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::LazyLock;

    use arrow_schema::Field;
    use arrow_schema::Fields;
    use arrow_schema::SchemaRef;
    use datafusion::arrow::array::DictionaryArray;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::array::StructArray;
    use datafusion::arrow::array::record_batch;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::datatypes::UInt32Type;
    use datafusion::arrow::util::display::FormatOptions;
    use datafusion::arrow::util::pretty::pretty_format_batches_with_options;
    use datafusion::logical_expr::col;
    use datafusion::logical_expr::lit;
    use datafusion::physical_expr::planner::logical2physical;
    use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions as df_expr;
    use datafusion_physical_expr::projection::ProjectionExpr;
    use insta::assert_snapshot;
    use itertools::Itertools;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use rstest::rstest;
    use std::cell::Cell;
    use vortex::VortexSessionDefault;
    use vortex::array::IntoArray;
    use vortex::array::arrays::ChunkedArray;
    use vortex::array::arrays::StructArray as VortexStructArray;
    use vortex::array::arrays::VarBinArray;
    use vortex::array::validity::Validity;
    use vortex::buffer::Buffer;
    use vortex::file::WriteOptionsSessionExt;
    use vortex::io::VortexWrite;
    use vortex::io::object_store::ObjectStoreWrite;
    use vortex::metrics::DefaultMetricsRegistry;
    use vortex::session::VortexSession;

    use super::*;
    use crate::VortexAccessPlan;
    use crate::convert::exprs::DefaultExpressionConvertor;
    use crate::persistent::reader::DefaultVortexReaderFactory;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::default);

    #[rstest]
    #[case(0..3, 10, vec![0..2, 2..5, 5..10], Some(0..2))]
    #[case(3..7, 10, vec![0..2, 2..5, 5..10], Some(2..5))]
    #[case(1..8, 10, vec![0..1, 1..9, 9..10], Some(1..9))]
    #[case(1..4, 16, vec![0..1, 1..2, 2..3, 3..4], None)]
    fn test_split_aligned_row_range(
        #[case] byte_range: Range<u64>,
        #[case] total_size: u64,
        #[case] split_ranges: Vec<Range<u64>>,
        #[case] expected: Option<Range<u64>>,
    ) {
        assert_eq!(
            split_aligned_row_range(byte_range, total_size, &split_ranges),
            expected
        );
    }

    #[test]
    fn test_split_aligned_ranges_cover_splits_exactly_once() {
        let split_ranges = vec![0..1, 1..4, 4..10, 10..13];
        let byte_ranges = [0..4, 4..8, 8..12, 12..16];

        let assigned = byte_ranges
            .into_iter()
            .filter_map(|byte_range| split_aligned_row_range(byte_range, 16, &split_ranges))
            .collect::<Vec<_>>();

        assert_eq!(assigned, vec![0..4, 4..10, 10..13]);
        assert_eq!(
            assigned
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>(),
            13
        );

        let split_starts = split_ranges
            .iter()
            .map(|range| range.start)
            .collect::<Vec<_>>();
        let split_ends = split_ranges
            .iter()
            .map(|range| range.end)
            .collect::<Vec<_>>();

        for range in &assigned {
            assert!(split_starts.contains(&range.start));
            assert!(split_ends.contains(&range.end));
        }

        for (left, right) in assigned.iter().tuple_windows() {
            assert_eq!(left.end, right.start);
        }
    }

    async fn write_arrow_to_vortex(
        object_store: Arc<dyn ObjectStore>,
        path: &str,
        rb: RecordBatch,
    ) -> anyhow::Result<u64> {
        let schema = rb.schema();
        let array = SESSION.arrow().from_arrow_record_batch(rb, &schema)?;
        let path = Path::parse(path)?;

        let mut write = ObjectStoreWrite::new(object_store, &path).await?;
        let summary = SESSION
            .write_options()
            .write(&mut write, array.to_array_stream())
            .await?;
        write.shutdown().await?;

        Ok(summary.size())
    }

    /// Writes an ascending `i32` filter column plus a payload column, chunked.
    ///
    /// Chunk boundaries are what `SplitBy::Layout` reports as natural splits,
    /// and a byte range owns whole natural splits. The payload is what keeps
    /// those chunks apart: an ascending `i32` column alone compresses to a
    /// couple of KiB, and the layout writer then emits the whole file as a
    /// single split that no byte range can tile.
    async fn write_chunked_ascending(
        object_store: Arc<dyn ObjectStore>,
        path: &str,
        chunks: u32,
        rows_per_chunk: u32,
    ) -> anyhow::Result<u64> {
        let ascending = (0..chunks)
            .map(|chunk| {
                (0..rows_per_chunk)
                    .map(|row| i32::try_from(chunk * rows_per_chunk + row).expect("row fits i32"))
                    .collect::<Buffer<_>>()
                    .into_array()
            })
            .collect::<ChunkedArray>()
            .into_array();
        let payload = (0..chunks)
            .map(|chunk| {
                VarBinArray::from(
                    (0..rows_per_chunk)
                        .map(|row| format!("{chunk}-{row}-{}", "x".repeat(48)))
                        .collect::<Vec<_>>(),
                )
                .into_array()
            })
            .collect::<ChunkedArray>()
            .into_array();
        let table = VortexStructArray::try_new(
            ["a", "p"].into(),
            vec![ascending, payload],
            (chunks * rows_per_chunk) as usize,
            Validity::NonNullable,
        )?;

        let path = Path::parse(path)?;
        let mut write = ObjectStoreWrite::new(object_store, &path).await?;
        let summary = SESSION
            .write_options()
            .write(&mut write, table.into_array().to_array_stream())
            .await?;
        write.shutdown().await?;
        Ok(summary.size())
    }

    thread_local! {
        /// Splits that reached scan construction on this thread.
        ///
        /// A split the zone map rejects returns before `ScanBuilder::new`, so
        /// this is what separates "the split was skipped" from "the scan ran
        /// and matched nothing". Row counts cannot: the scan prunes the same
        /// zones itself and returns the same rows either way, so a test with
        /// only that oracle stays green if the skip is deleted. Thread-local
        /// rather than global because tests run in parallel, and each
        /// `#[tokio::test]` polls its opener on its own thread.
        static SCANS_BUILT: Cell<usize> = const { Cell::new(0) };
    }

    /// Called from `VortexOpener::open` immediately before a scan is built.
    pub(super) fn record_scan_built() {
        SCANS_BUILT.with(|built| built.set(built.get() + 1));
    }

    thread_local! {
        /// Files this thread read with a point read instead of a scan.
        static POINT_READS: Cell<usize> = const { Cell::new(0) };
    }

    /// Called from `VortexOpener::open` when a file is read with a point read.
    pub(super) fn record_point_read() {
        POINT_READS.with(|reads| reads.set(reads.get() + 1));
    }

    /// Point reads since the last call, resetting the count.
    fn take_point_reads() -> usize {
        POINT_READS.with(|reads| reads.replace(0))
    }

    /// Scans built since the last call, resetting the count.
    fn take_scans_built() -> usize {
        SCANS_BUILT.with(|built| built.replace(0))
    }

    fn make_opener(
        object_store: Arc<dyn ObjectStore>,
        table_schema: TableSchema,
        filter: Option<PhysicalExprRef>,
    ) -> VortexOpener {
        VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(object_store)),
            projection: ProjectionExprs::from_indices(&[0], table_schema.file_schema()),
            filter,
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema,
            batch_size: 100,
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            layout_readers: Default::default(),
            natural_split_ranges: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            segment_cache: None,
            object_store_url: Arc::from("memory:///"),
            projection_pushdown: false,
            scan_concurrency: None,
            runtime_access_plan_provider: None,
            key_column: None,
        }
    }

    #[tokio::test]
    async fn test_open() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "part=1/file.vortex";
        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)]))
            .expect("test record batch should build");
        let data_size =
            write_arrow_to_vortex(object_store.clone(), file_path, batch.clone()).await?;

        let file_schema = batch.schema();
        let mut file = PartitionedFile::new(file_path.to_string(), data_size);
        file.partition_values = vec![ScalarValue::Int32(Some(1))];

        let table_schema = TableSchema::builder(file_schema.clone())
            .with_table_partition_cols(vec![Arc::new(Field::new("part", DataType::Int32, false))])
            .build();

        // filter matches partition value
        let filter = col("part").eq(lit(1));
        let filter = logical2physical(&filter, table_schema.table_schema());

        let opener = make_opener(object_store.clone(), table_schema.clone(), Some(filter));
        let stream = opener
            .open(file.clone())
            .expect("opener should open file with matching filter")
            .await
            .expect("opening matching-filter file should produce a stream");

        let data = stream.try_collect::<Vec<_>>().await?;
        let num_batches = data.len();
        let num_rows = data.iter().map(|rb| rb.num_rows()).sum::<usize>();

        assert_eq!((num_batches, num_rows), (1, 3));

        // filter doesn't matches partition value
        let filter = col("part").eq(lit(2));
        let filter = logical2physical(&filter, table_schema.table_schema());

        let opener = make_opener(object_store.clone(), table_schema.clone(), Some(filter));
        let stream = opener
            .open(file.clone())
            .expect("opener should open file with non-matching filter")
            .await
            .expect("opening non-matching-filter file should produce a stream");

        let data = stream.try_collect::<Vec<_>>().await?;
        let num_batches = data.len();
        let num_rows = data.iter().map(|rb| rb.num_rows()).sum::<usize>();
        assert_eq!((num_batches, num_rows), (0, 0));

        Ok(())
    }

    /// Zone pruning must never drop a row the filter matches, split by split.
    ///
    /// Pruning is applied to the split's own row range, so it is sound only
    /// while that range is the one the scan would have read. A whole-file open
    /// cannot catch a mismatch between the two — both are then the whole file —
    /// so this tiles the file with byte-range splits the way `FileScanConfig`
    /// does, opens each, and aggregates.
    ///
    /// 20,000 ascending rows close several zones (the writer ends one every
    /// 8192 rows), which is what gives zones disjoint ranges and so actually
    /// reaches the skip path. Byte thirds land one zone midpoint each, so every
    /// needle is owned by exactly one split and provably absent from the other
    /// two: the run asserts both halves, or a split silently returning nothing
    /// would read as success.
    #[tokio::test]
    async fn zone_pruning_keeps_every_matching_row_across_byte_splits() -> anyhow::Result<()> {
        const CHUNKS: u32 = 16;
        const ROWS_PER_CHUNK: u32 = 4_096;
        const ROWS: i32 = (CHUNKS * ROWS_PER_CHUNK) as i32;
        const SPLITS: usize = 4;

        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "zones.vortex";
        let data_size =
            write_chunked_ascending(object_store.clone(), file_path, CHUNKS, ROWS_PER_CHUNK)
                .await?;

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("p", DataType::Utf8, false),
        ]));
        let table_schema = TableSchema::from(file_schema);
        let splits = u64::try_from(SPLITS).expect("split count fits u64");
        let byte_splits: Vec<PartitionedFile> = (0..splits)
            .map(|i| {
                let start = data_size * i / splits;
                let end = data_size * (i + 1) / splits;
                PartitionedFile::new_with_range(
                    file_path.to_string(),
                    data_size,
                    i64::try_from(start).expect("split start fits i64"),
                    i64::try_from(end).expect("split end fits i64"),
                )
            })
            .collect();

        // Rows each split returns for `a = needle`, in split order.
        async fn rows_per_split(
            object_store: &Arc<dyn ObjectStore>,
            table_schema: &TableSchema,
            byte_splits: &[PartitionedFile],
            predicate: PhysicalExprRef,
        ) -> anyhow::Result<(Vec<usize>, usize)> {
            take_scans_built();
            // One opener over every split, as a scan partition does: the layout
            // reader and its zone map are shared between them.
            let opener = make_opener(
                Arc::clone(object_store),
                table_schema.clone(),
                Some(predicate),
            );
            let mut per_split = Vec::with_capacity(byte_splits.len());
            for split in byte_splits {
                let rows: usize = opener
                    .open(split.clone())
                    .expect("opener should open the split")
                    .await
                    .expect("opening should produce a stream")
                    .try_collect::<Vec<_>>()
                    .await?
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum();
                per_split.push(rows);
            }
            Ok((per_split, take_scans_built()))
        }

        // First row, both sides of a zone boundary, an interior value, last row.
        for needle in [0, 4_095, 4_096, 16_384, 32_768, ROWS - 1] {
            let filter = logical2physical(&col("a").eq(lit(needle)), table_schema.table_schema());
            let (per_split, scans_built) =
                rows_per_split(&object_store, &table_schema, &byte_splits, filter).await?;

            assert_eq!(
                per_split.iter().sum::<usize>(),
                1,
                "value {needle} is present once and must survive pruning; per split: {per_split:?}"
            );
            assert_eq!(
                per_split.iter().filter(|rows| **rows > 0).count(),
                1,
                "exactly one split owns {needle}; per split: {per_split:?}"
            );
            assert_eq!(
                scans_built,
                1,
                "only the split owning {needle} may reach scan construction; the other \
                 {} built a scan the zone map could have skipped",
                SPLITS - 1
            );
        }

        // Outside every zone's range: every split is skipped, nothing is returned.
        let filter = logical2physical(&col("a").eq(lit(ROWS + 1)), table_schema.table_schema());
        let (per_split, scans_built) =
            rows_per_split(&object_store, &table_schema, &byte_splits, filter).await?;
        assert!(
            per_split.iter().all(|rows| *rows == 0),
            "a value the file cannot hold must return no rows; per split: {per_split:?}"
        );
        assert_eq!(
            scans_built, 0,
            "no split may reach scan construction for a value the file cannot hold"
        );

        // Nothing prunable: the splits must still tile the file exactly, which is
        // what says the pruned range and the scanned range are the same range.
        let filter = logical2physical(&col("a").gt_eq(lit(0)), table_schema.table_schema());
        let (per_split, scans_built) =
            rows_per_split(&object_store, &table_schema, &byte_splits, filter).await?;
        assert_eq!(
            per_split.iter().sum::<usize>(),
            ROWS as usize,
            "byte splits must cover every row exactly once; per split: {per_split:?}"
        );
        assert_eq!(
            scans_built, SPLITS,
            "a filter that prunes nothing must leave every split to the scan"
        );

        Ok(())
    }
    /// Writes `keys` as a nullable `Int64` column `a`, plus a column `p` naming
    /// each row by its position, and returns the file's schema and size.
    async fn write_keyed(
        object_store: Arc<dyn ObjectStore>,
        path: &str,
        keys: Vec<Option<i64>>,
    ) -> anyhow::Result<(SchemaRef, u64)> {
        use datafusion::arrow::array::Int64Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("p", DataType::Utf8, false),
        ]));
        let positions: Vec<String> = (0..keys.len()).map(|row| format!("row-{row}")).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringArray::from(positions)),
            ],
        )?;
        let size = write_arrow_to_vortex(object_store, path, batch).await?;
        Ok((schema, size))
    }

    /// Reads `file` with `predicate`, projecting both columns, and returns the rows
    /// as sorted `(a, p)` pairs: an unordered scan may return its splits in any
    /// order.
    async fn read_keyed(
        object_store: &Arc<dyn ObjectStore>,
        schema: &SchemaRef,
        file: &PartitionedFile,
        predicate: &datafusion::logical_expr::Expr,
        key_column: Option<&str>,
    ) -> anyhow::Result<Vec<(Option<i64>, String)>> {
        use datafusion::arrow::array::Array;
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Int64Type;

        let table_schema = TableSchema::from(Arc::clone(schema));
        let filter = logical2physical(predicate, table_schema.table_schema());
        let mut opener = make_opener(Arc::clone(object_store), table_schema, Some(filter));
        opener.projection = ProjectionExprs::from_indices(&[0, 1], schema);
        opener.key_column = key_column.map(Arc::from);
        let batches = opener
            .open(file.clone())?
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut rows = Vec::new();
        for batch in &batches {
            let keys = batch.column(0).as_primitive::<Int64Type>();
            let positions = batch.column(1).as_string_view_opt().map_or_else(
                || {
                    batch
                        .column(1)
                        .as_string::<i32>()
                        .iter()
                        .map(|p| p.expect("p is not null").to_string())
                        .collect::<Vec<_>>()
                },
                |view| {
                    view.iter()
                        .map(|p| p.expect("p is not null").to_string())
                        .collect()
                },
            );
            for (row, position) in positions.into_iter().enumerate() {
                let key = (!keys.is_null(row)).then(|| keys.value(row));
                rows.push((key, position));
            }
        }
        rows.sort();
        Ok(rows)
    }

    /// Reads `file` with `predicate`, projecting only `p`, and returns its values.
    async fn read_positions(
        object_store: &Arc<dyn ObjectStore>,
        schema: &SchemaRef,
        file: &PartitionedFile,
        predicate: &datafusion::logical_expr::Expr,
        key_column: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        use datafusion::arrow::util::display::ArrayFormatter;

        let table_schema = TableSchema::from(Arc::clone(schema));
        let filter = logical2physical(predicate, table_schema.table_schema());
        let mut opener = make_opener(Arc::clone(object_store), table_schema, Some(filter));
        opener.projection = ProjectionExprs::from_indices(&[1], schema);
        opener.key_column = key_column.map(Arc::from);
        let batches = opener
            .open(file.clone())?
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut values = Vec::new();
        for batch in &batches {
            assert_eq!(batch.num_columns(), 1, "only `p` is projected");
            let formatter = ArrayFormatter::try_new(batch.column(0), &FormatOptions::default())?;
            values.extend((0..batch.num_rows()).map(|row| formatter.value(row).to_string()));
        }
        values.sort();
        Ok(values)
    }

    /// A key lookup over a whole file reads only the blocks whose bounds hold the
    /// key, and must return exactly what the scan returns — for keys at block
    /// edges, a key stored twice, keys inside an all-null block, and keys the
    /// file cannot hold. The path counters are what show the point read ran:
    /// the rows alone would match even if every lookup fell back to the scan.
    #[tokio::test]
    async fn key_lookups_read_the_rows_the_scan_reads() -> anyhow::Result<()> {
        const BLOCK: i64 = 8_192;
        const ROWS: i64 = 5 * BLOCK + 100;

        // `a = row`, except: every 1000th key is null, block 1 is entirely null,
        // and row 30,000 (block 3) repeats key 24,000 (block 2).
        let keys: Vec<Option<i64>> = (0..ROWS)
            .map(|row| match row {
                30_000 => Some(24_000),
                _ if (BLOCK..2 * BLOCK).contains(&row) || row % 1_000 == 999 => None,
                _ => Some(row),
            })
            .collect();
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let path = "key_lookups_read_the_rows_the_scan_reads.vortex";
        let (schema, size) = write_keyed(Arc::clone(&object_store), path, keys).await?;
        let file = PartitionedFile::new(path.to_string(), size);

        // (key, rows expected, whether any block can hold it)
        let cases = [
            (0, 1, true),
            (24_000, 2, true),
            (999, 0, true),
            (BLOCK - 1, 1, true),
            // Only the all-null block spans 10,000.
            (10_000, 0, false),
            (2 * BLOCK, 1, true),
            // Row 30,000 holds 24,000, but block 3's bounds still hold 30,000.
            (30_000, 0, true),
            (ROWS - 1, 1, true),
            (ROWS, 0, false),
            (-1, 0, false),
        ];
        for (key, expected, candidate) in cases {
            let predicate = col("a").eq(lit(key));
            let scanned = read_keyed(&object_store, &schema, &file, &predicate, None).await?;
            take_scans_built();
            take_point_reads();
            let looked_up =
                read_keyed(&object_store, &schema, &file, &predicate, Some("a")).await?;
            assert_eq!(
                looked_up, scanned,
                "key {key}: point read and scan disagree"
            );
            assert_eq!(looked_up.len(), expected, "key {key}: {looked_up:?}");
            assert_eq!(
                (take_point_reads(), take_scans_built()),
                (usize::from(candidate), 0),
                "key {key}: a key some block can hold is read with a point read, and one \
                 no block can hold skips the file"
            );
        }

        // Every conjunct still applies: key 24,000 is stored twice, one row passes.
        let predicate = col("a")
            .eq(lit(24_000_i64))
            .and(col("p").eq(lit("row-30000")));
        let scanned = read_keyed(&object_store, &schema, &file, &predicate, None).await?;
        take_point_reads();
        let looked_up = read_keyed(&object_store, &schema, &file, &predicate, Some("a")).await?;
        assert_eq!(looked_up, scanned);
        assert_eq!(looked_up, vec![(Some(24_000), "row-30000".to_string())]);
        assert_eq!(take_point_reads(), 1);

        // A projection of some of the columns reads the same rows.
        for key in [0, ROWS - 1] {
            let predicate = col("a").eq(lit(key));
            let scanned = read_positions(&object_store, &schema, &file, &predicate, None).await?;
            take_point_reads();
            let looked_up =
                read_positions(&object_store, &schema, &file, &predicate, Some("a")).await?;
            assert_eq!(looked_up, scanned);
            assert_eq!(looked_up, vec![format!("row-{key}")]);
            assert_eq!(take_point_reads(), 1);
        }

        // Anything but an equality on the key keeps the scan.
        let predicate = col("a").gt_eq(lit(ROWS - 3));
        take_scans_built();
        take_point_reads();
        let looked_up = read_keyed(&object_store, &schema, &file, &predicate, Some("a")).await?;
        assert_eq!(looked_up.len(), 3, "{looked_up:?}");
        assert_eq!((take_point_reads(), take_scans_built()), (0, 1));

        Ok(())
    }

    /// Once a file's key blocks are cached, a key no block can hold skips the file
    /// before it is opened. The object is deleted after the blocks are built: a key
    /// outside every block is still answered, and one a block holds has to open the
    /// file and fails.
    #[tokio::test]
    async fn cached_key_blocks_skip_a_file_without_opening_it() -> anyhow::Result<()> {
        use object_store::ObjectStoreExt;

        let keys = (0..3 * 8_192).map(Some).collect();
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let path = "cached_key_blocks_skip_a_file_without_opening_it.vortex";
        let (schema, size) = write_keyed(Arc::clone(&object_store), path, keys).await?;
        let file = PartitionedFile::new(path.to_string(), size);

        let warm = col("a").eq(lit(5_i64));
        let rows = read_keyed(&object_store, &schema, &file, &warm, Some("a")).await?;
        assert_eq!(rows, vec![(Some(5), "row-5".to_string())]);

        object_store.delete(&Path::from(path)).await?;

        let absent = col("a").eq(lit(-1_i64));
        let rows = read_keyed(&object_store, &schema, &file, &absent, Some("a")).await?;
        assert!(rows.is_empty(), "{rows:?}");

        let present = col("a").eq(lit(6_i64));
        assert!(
            read_keyed(&object_store, &schema, &file, &present, Some("a"))
                .await
                .is_err(),
            "a key a block holds must open the deleted file"
        );
        Ok(())
    }

    /// A point read does not apply row selections, so a file carrying one (deleted
    /// rows, say) must keep the scan; so must a key more blocks can hold than a
    /// point read serves.
    #[tokio::test]
    async fn key_lookups_leave_selections_and_wide_keys_to_the_scan() -> anyhow::Result<()> {
        use vortex::buffer::Buffer;

        const BLOCK: i64 = 8_192;

        // Even blocks hold keys 0..BLOCK and odd blocks BLOCK..2*BLOCK, so a key
        // below BLOCK is in five separate blocks.
        let keys: Vec<Option<i64>> = (0..9 * BLOCK)
            .map(|row| Some(row % BLOCK + (row / BLOCK % 2) * BLOCK))
            .collect();
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let path = "key_lookups_leave_selections_and_wide_keys_to_the_scan.vortex";
        let (schema, size) = write_keyed(Arc::clone(&object_store), path, keys).await?;
        let file = PartitionedFile::new(path.to_string(), size);

        let predicate = col("a").eq(lit(5_i64));
        let scanned = read_keyed(&object_store, &schema, &file, &predicate, None).await?;
        take_scans_built();
        take_point_reads();
        let looked_up = read_keyed(&object_store, &schema, &file, &predicate, Some("a")).await?;
        assert_eq!(looked_up, scanned);
        assert_eq!(looked_up.len(), 5, "{looked_up:?}");
        assert_eq!(
            (take_point_reads(), take_scans_built()),
            (0, 1),
            "five candidate ranges exceed what a point read serves"
        );

        // Key BLOCK + 5 is in the four odd blocks, which a point read serves —
        // unless the file carries a selection, here dropping the first match.
        let predicate = col("a").eq(lit(BLOCK + 5));
        let mut selected = file.clone();
        let keep: Vec<u64> = (0..9 * 8_192_u64).filter(|row| *row != 8_197).collect();
        selected.extensions.insert(
            VortexAccessPlan::default()
                .with_selection(crate::include_by_index(&Buffer::from_iter(keep))),
        );
        let scanned = read_keyed(&object_store, &schema, &selected, &predicate, None).await?;
        take_scans_built();
        take_point_reads();
        let looked_up =
            read_keyed(&object_store, &schema, &selected, &predicate, Some("a")).await?;
        assert_eq!(looked_up, scanned);
        assert_eq!(
            looked_up.len(),
            3,
            "the selection drops row 8197: {looked_up:?}"
        );
        assert_eq!((take_point_reads(), take_scans_built()), (0, 1));

        take_point_reads();
        let looked_up = read_keyed(&object_store, &schema, &file, &predicate, Some("a")).await?;
        assert_eq!(looked_up.len(), 4, "{looked_up:?}");
        assert_eq!(take_point_reads(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_open_empty_file() -> anyhow::Result<()> {
        use futures::TryStreamExt;

        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let data_batch = record_batch!(("a", Int32, Vec::<i32>::new()))
            .expect("empty record batch should build");
        let file_path = "part=1/empty.vortex";
        let file_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, data_batch.clone()).await?;

        let file_schema = data_batch.schema();
        // Parallel scans may attach a byte range even for empty files; the
        // opener must return early before attempting split-aligned translation.
        let file =
            PartitionedFile::new_with_range(file_path.to_string(), file_size, 0, file_size as i64);

        let table_schema = TableSchema::from(Arc::clone(&file_schema));

        let opener = make_opener(object_store, table_schema, None);
        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;

        assert_eq!(data.len(), 0);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_open_files_different_table_schema() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let file1 = {
            let file1_path = "/path/file1.vortex";
            let batch1 = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)]))
                .expect("Int32 record batch should build");
            let data_size1 =
                write_arrow_to_vortex(object_store.clone(), file1_path, batch1).await?;
            PartitionedFile::new(file1_path.to_string(), data_size1)
        };

        let file2 = {
            let file2_path = "/path/file2.vortex";
            let batch2 = record_batch!(("a", Int16, vec![Some(-1), Some(-2), Some(-3)]))
                .expect("Int16 record batch should build");
            let data_size2 =
                write_arrow_to_vortex(object_store.clone(), file2_path, batch2).await?;
            PartitionedFile::new(file2_path.to_string(), data_size2)
        };

        // Table schema has can accommodate both files
        let table_schema = TableSchema::from(Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            true,
        )])));

        let make_opener = |filter| VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(object_store.clone())),
            projection: ProjectionExprs::from_indices(&[0], table_schema.file_schema()),
            filter: Some(filter),
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema: table_schema.clone(),
            batch_size: 100,
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            layout_readers: Default::default(),
            natural_split_ranges: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            segment_cache: None,
            object_store_url: Arc::from("memory:///"),
            projection_pushdown: false,
            scan_concurrency: None,
            runtime_access_plan_provider: None,
            key_column: None,
        };

        let filter = col("a").lt(lit(100_i32));
        let filter = logical2physical(&filter, table_schema.table_schema());

        let opener1 = make_opener(filter.clone());
        let stream = opener1.open(file1)?.await?;

        let format_opts = FormatOptions::new().with_types_info(true);

        let data = stream.try_collect::<Vec<_>>().await?;
        assert_snapshot!(
            "open_files_different_table_schema_int32_file",
            pretty_format_batches_with_options(&data, &format_opts)?.to_string()
        );

        let opener2 = make_opener(filter.clone());
        let stream = opener2.open(file2)?.await?;

        let data = stream.try_collect::<Vec<_>>().await?;
        assert_snapshot!(
            "open_files_different_table_schema_int16_file_widened",
            pretty_format_batches_with_options(&data, &format_opts)?.to_string()
        );

        Ok(())
    }

    #[tokio::test]
    // This test verifies that files with different column order than the
    // table schema can be opened without errors. The fix ensures that the
    // schema mapper is only used for type casting, not for reordering,
    // since the vortex projection already handles reordering.
    async fn test_schema_different_column_order() -> anyhow::Result<()> {
        use datafusion::arrow::util::pretty::pretty_format_batches_with_options;

        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        // File has columns in order: c, b, a
        let batch = record_batch!(
            ("c", Int32, vec![Some(300), Some(301), Some(302)]),
            ("b", Int32, vec![Some(200), Some(201), Some(202)]),
            ("a", Int32, vec![Some(100), Some(101), Some(102)])
        )
        .expect("column-order test record batch should build");
        let data_size = write_arrow_to_vortex(object_store.clone(), file_path, batch).await?;
        let file = PartitionedFile::new(file_path.to_string(), data_size);

        // Table schema has columns in different order: a, b, c
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]));

        let opener = VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(object_store)),
            projection: ProjectionExprs::from_indices(&[0, 1, 2], &table_schema),
            filter: None,
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema: TableSchema::from(table_schema.clone()),
            batch_size: 100,
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            layout_readers: Default::default(),
            natural_split_ranges: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            segment_cache: None,
            object_store_url: Arc::from("memory:///"),
            projection_pushdown: false,
            scan_concurrency: None,
            runtime_access_plan_provider: None,
            key_column: None,
        };

        let stream = opener.open(file)?.await?;

        let format_opts = FormatOptions::new().with_types_info(true);
        let data = stream.try_collect::<Vec<_>>().await?;

        // Verify the output has columns in table schema order (a, b, c)
        // not file order (c, b, a)
        assert_snapshot!(
            "schema_different_column_order_table_order",
            pretty_format_batches_with_options(&data, &format_opts)?.to_string()
        );

        Ok(())
    }

    #[tokio::test]
    // This test verifies that expression rewriting doesn't fail when there is
    // a nested schema mismatch between the physical file schema and logical
    // table schema.
    async fn test_adapter_logical_physical_struct_mismatch() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";
        let file_struct_fields = Fields::from(vec![
            Field::new("field1", DataType::Utf8, true),
            Field::new("field2", DataType::Utf8, true),
        ]);
        let struct_array = StructArray::new(
            file_struct_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["value1", "value2", "value3"])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "my_struct",
                DataType::Struct(file_struct_fields),
                true,
            )])),
            vec![Arc::new(struct_array)],
        )?;
        let data_size = write_arrow_to_vortex(object_store.clone(), file_path, batch).await?;

        // Table schema has an extra utf8 field.
        let table_schema = TableSchema::from(Arc::new(Schema::new(vec![Field::new(
            "my_struct",
            DataType::Struct(Fields::from(vec![
                Field::new(
                    "field1",
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                    true,
                ),
                Field::new(
                    "field2",
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                    true,
                ),
                Field::new("field3", DataType::Utf8, true),
            ])),
            true,
        )])));

        let opener = make_opener(
            object_store.clone(),
            table_schema.clone(),
            // expression references my_struct column which has different fields in each
            // field.
            Some(logical2physical(
                &col("my_struct").is_not_null(),
                table_schema.table_schema(),
            )),
        );

        // The opener should be able to open the file with a filter on the
        // struct column.
        let data = opener
            .open(PartitionedFile::new(file_path.to_string(), data_size))?
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 3);

        Ok(())
    }

    #[tokio::test]
    // Minimal reproducing test for the schema projection bug.
    // Before the fix, this would fail with a cast error when the file schema
    // and table schema have different field orders and we project a subset of columns.
    async fn test_projection_bug_minimal_repro() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        // File has columns in order: a, b, c with simple types
        let batch = record_batch!(
            ("a", Int32, vec![Some(1)]),
            ("b", Utf8, vec![Some("test")]),
            ("c", Int32, vec![Some(2)])
        )
        .expect("projection-repro record batch should build");
        let data_size = write_arrow_to_vortex(object_store.clone(), file_path, batch).await?;

        // Table schema has columns in DIFFERENT order: c, a, b
        // and different types that require casting (Utf8 -> Dictionary)
        let table_schema = TableSchema::from(Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
            Field::new(
                "b",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                true,
            ),
        ])));

        // Project columns [0, 2] from table schema, which should give us: c, b
        // Before the fix, the schema adapter would get confused about which fields
        // to select from the file, causing incorrect type mappings.
        let projection = vec![0, 2];

        let opener = VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(object_store.clone())),
            projection: ProjectionExprs::from_indices(
                projection.as_ref(),
                table_schema.file_schema(),
            ),
            filter: None,
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema: table_schema.clone(),
            batch_size: 100,
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            layout_readers: Default::default(),
            natural_split_ranges: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            segment_cache: None,
            object_store_url: Arc::from("memory:///"),
            projection_pushdown: false,
            scan_concurrency: None,
            runtime_access_plan_provider: None,
            key_column: None,
        };

        // This should succeed and return the correctly projected and cast data
        let data = opener
            .open(PartitionedFile::new(file_path.to_string(), data_size))?
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        // Verify the columns are in the right order and have the right values
        use datafusion::arrow::util::pretty::pretty_format_batches_with_options;
        let format_opts = FormatOptions::new().with_types_info(true);
        assert_snapshot!(
            "projection_bug_minimal_repro_projected_and_cast",
            pretty_format_batches_with_options(&data, &format_opts)?.to_string()
        );

        Ok(())
    }

    fn make_test_batch_with_10_rows() -> RecordBatch {
        record_batch!(
            ("a", Int32, (0..=9).map(Some).collect::<Vec<_>>()),
            (
                "b",
                Utf8,
                (0..=9).map(|i| Some(format!("r{}", i))).collect::<Vec<_>>()
            )
        )
        .expect("10-row test record batch should build")
    }

    fn make_test_opener(
        object_store: Arc<dyn ObjectStore>,
        schema: SchemaRef,
        projection: ProjectionExprs,
    ) -> VortexOpener {
        VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(object_store)),
            projection,
            filter: None,
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema: TableSchema::from(schema),
            batch_size: 100,
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            layout_readers: Default::default(),
            natural_split_ranges: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            segment_cache: None,
            object_store_url: Arc::from("memory:///"),
            projection_pushdown: false,
            scan_concurrency: None,
            runtime_access_plan_provider: None,
            key_column: None,
        }
    }

    #[derive(Debug)]
    struct EmptyRuntimeAccessPlanProvider;

    #[async_trait::async_trait]
    impl VortexRuntimeAccessPlanProvider for EmptyRuntimeAccessPlanProvider {
        async fn runtime_access_plan_for_file(
            &self,
            _file: &PartitionedFile,
            _predicate: Option<&PhysicalExprRef>,
        ) -> Option<Arc<VortexAccessPlan>> {
            Some(Arc::new(
                VortexAccessPlan::default()
                    .with_selection(crate::include_by_index(&Buffer::empty())),
            ))
        }
    }

    #[tokio::test]
    async fn empty_runtime_selection_skips_file_open() -> anyhow::Result<()> {
        let _ = take_scans_built();
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let schema = make_test_batch_with_10_rows().schema();
        let file = PartitionedFile::new("/path/does-not-exist.vortex".to_string(), 100);
        let mut opener = make_test_opener(
            object_store,
            Arc::clone(&schema),
            ProjectionExprs::from_indices(&[0], &schema),
        );
        opener.runtime_access_plan_provider = Some(Arc::new(EmptyRuntimeAccessPlanProvider));

        let data = opener.open(file)?.await?.try_collect::<Vec<_>>().await?;

        assert!(data.is_empty());
        assert_eq!(take_scans_built(), 0);
        Ok(())
    }

    #[tokio::test]
    // Test that Selection::IncludeByIndex filters to specific row indices.
    async fn test_selection_include_by_index() -> anyhow::Result<()> {
        use datafusion::arrow::util::pretty::pretty_format_batches_with_options;
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        let batch = make_test_batch_with_10_rows();
        let data_size =
            write_arrow_to_vortex(object_store.clone(), file_path, batch.clone()).await?;

        let schema = batch.schema();
        let mut file = PartitionedFile::new(file_path.to_string(), data_size);
        file.extensions
            .insert(
                VortexAccessPlan::default().with_selection(crate::include_by_index(
                    &Buffer::from_iter(vec![1, 3, 5, 7]),
                )),
            );

        let opener = make_test_opener(
            object_store.clone(),
            schema.clone(),
            ProjectionExprs::from_indices(&[0, 1], &schema),
        );

        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;
        let format_opts = FormatOptions::new().with_types_info(true);

        assert_snapshot!(
            "selection_include_by_index_rows",
            pretty_format_batches_with_options(&data, &format_opts)?.to_string()
        );

        Ok(())
    }

    #[tokio::test]
    // Test that Selection::ExcludeByIndex excludes specific row indices.
    async fn test_selection_exclude_by_index() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        let batch = make_test_batch_with_10_rows();
        let data_size =
            write_arrow_to_vortex(object_store.clone(), file_path, batch.clone()).await?;

        let schema = batch.schema();
        let mut file = PartitionedFile::new(file_path.to_string(), data_size);
        file.extensions
            .insert(
                VortexAccessPlan::default().with_selection(crate::exclude_by_index(
                    &Buffer::from_iter(vec![0, 2, 4, 6, 8]),
                )),
            );

        let opener = make_test_opener(
            object_store.clone(),
            schema.clone(),
            ProjectionExprs::from_indices(&[0, 1], &schema),
        );

        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;
        let format_opts = FormatOptions::new().with_types_info(true);

        assert_snapshot!(
            "selection_exclude_by_index_rows",
            pretty_format_batches_with_options(&data, &format_opts)?.to_string()
        );

        Ok(())
    }

    #[tokio::test]
    // Test that Selection::All returns all rows.
    async fn test_selection_all() -> anyhow::Result<()> {
        use vortex::scan::selection::Selection;

        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        let batch = make_test_batch_with_10_rows();
        let data_size =
            write_arrow_to_vortex(object_store.clone(), file_path, batch.clone()).await?;

        let schema = batch.schema();
        let mut file = PartitionedFile::new(file_path.to_string(), data_size);
        file.extensions
            .insert(VortexAccessPlan::default().with_selection(Selection::All));

        let opener = make_test_opener(
            object_store.clone(),
            schema.clone(),
            ProjectionExprs::from_indices(&[0], &schema),
        );

        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;

        let total_rows: usize = data.iter().map(|rb| rb.num_rows()).sum();
        assert_eq!(total_rows, 10);

        Ok(())
    }

    #[tokio::test]
    // Test that when no extensions are provided, all rows are returned (backward compatibility).
    async fn test_selection_no_extensions() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        let batch = make_test_batch_with_10_rows();
        let data_size =
            write_arrow_to_vortex(object_store.clone(), file_path, batch.clone()).await?;

        let schema = batch.schema();
        let file = PartitionedFile::new(file_path.to_string(), data_size);
        // file.extensions is None by default

        let opener = make_test_opener(
            object_store.clone(),
            schema.clone(),
            ProjectionExprs::from_indices(&[0], &schema),
        );

        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;

        let total_rows: usize = data.iter().map(|rb| rb.num_rows()).sum();
        assert_eq!(total_rows, 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_projection_expr_pushdown() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        let batch = record_batch!(
            ("a", Int32, vec![Some(1), Some(2), Some(3)]),
            ("b", Int32, vec![Some(10), Some(20), Some(30)])
        )
        .expect("projection-pushdown record batch should build");
        let data_size =
            write_arrow_to_vortex(object_store.clone(), file_path, batch.clone()).await?;

        let file_schema = batch.schema();
        let table_schema = TableSchema::from(file_schema.clone());

        // Create a projection that includes an arithmetic expression: a + b * 2
        let col_a = df_expr::col("a", &file_schema)?;
        let col_b = df_expr::col("b", &file_schema)?;
        let two = df_expr::lit(ScalarValue::Int32(Some(2)));

        // b * 2
        let b_times_2 = df_expr::binary(col_b, Operator::Multiply, two, &file_schema)?;
        // a + (b * 2)
        let a_plus_b_times_2 = df_expr::binary(col_a, Operator::Plus, b_times_2, &file_schema)?;

        let projection = ProjectionExprs::new(vec![ProjectionExpr::new(
            a_plus_b_times_2,
            "result".to_string(),
        )]);

        let opener = VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(object_store.clone())),
            projection,
            filter: None,
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema,
            batch_size: 100,
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            layout_readers: Default::default(),
            natural_split_ranges: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            segment_cache: None,
            object_store_url: Arc::from("memory:///"),
            projection_pushdown: false,
            scan_concurrency: None,
            runtime_access_plan_provider: None,
            key_column: None,
        };

        let file = PartitionedFile::new(file_path.to_string(), data_size);
        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;

        // Expected: a + b * 2
        // row 0: 1 + 10 * 2 = 21
        // row 1: 2 + 20 * 2 = 42
        // row 2: 3 + 30 * 2 = 63
        assert_snapshot!(
            "projection_expr_pushdown_result",
            pretty_format_batches_with_options(&data, &FormatOptions::new().with_types_info(true))?
                .to_string()
        );

        Ok(())
    }

    /// When a Struct contains Dictionary fields, writing to vortex and reading back
    /// should preserve the Dictionary type.
    #[tokio::test]
    async fn test_struct_with_dictionary_roundtrip() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let struct_fields = Fields::from(vec![
            Field::new_dictionary("a", DataType::UInt32, DataType::Utf8, true),
            Field::new_dictionary("b", DataType::UInt32, DataType::Utf8, true),
        ]);
        let struct_array = StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(DictionaryArray::<UInt32Type>::from_iter(["x", "y", "x"])),
                Arc::new(DictionaryArray::<UInt32Type>::from_iter(["p", "p", "q"])),
            ],
            None,
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "labels",
            DataType::Struct(struct_fields.clone()),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)])?;

        let file_path = "/test.vortex";
        let data_size = write_arrow_to_vortex(object_store.clone(), file_path, batch).await?;

        let opener = make_test_opener(
            object_store.clone(),
            schema.clone(),
            ProjectionExprs::from_indices(&[0], &schema),
        );
        let data: Vec<_> = opener
            .open(PartitionedFile::new(file_path.to_string(), data_size))?
            .await?
            .try_collect()
            .await?;

        assert_eq!(
            data[0].schema().field(0).data_type(),
            &DataType::Struct(struct_fields),
            "Struct(Dictionary) type should be preserved"
        );
        Ok(())
    }

    /// Builds a hash-join style dynamic filter `(id >= 3 AND id <= 7) AND id IN (3, 7)`
    /// — the min/max bounds conjuncts AND the `InList` membership.
    fn bounds_and_inlist_dynamic_filter() -> PhysicalExprRef {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let column = Arc::new(df_expr::Column::new("id", 0)) as PhysicalExprRef;

        let ge = Arc::new(df_expr::BinaryExpr::new(
            Arc::clone(&column),
            Operator::GtEq,
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(3)))),
        )) as PhysicalExprRef;
        let le = Arc::new(df_expr::BinaryExpr::new(
            Arc::clone(&column),
            Operator::LtEq,
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(7)))),
        )) as PhysicalExprRef;
        let bounds = Arc::new(df_expr::BinaryExpr::new(ge, Operator::And, le)) as PhysicalExprRef;

        let in_list = Arc::new(
            df_expr::InListExpr::try_new(
                Arc::clone(&column),
                vec![
                    Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(3)))) as PhysicalExprRef,
                    Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(7)))) as PhysicalExprRef,
                ],
                false,
                &schema,
            )
            .expect("IN-list expression should be valid"),
        ) as PhysicalExprRef;

        let combined =
            Arc::new(df_expr::BinaryExpr::new(bounds, Operator::And, in_list)) as PhysicalExprRef;

        let dynamic_filter = Arc::new(df_expr::DynamicFilterPhysicalExpr::new(
            vec![column],
            Arc::new(df_expr::Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        dynamic_filter
            .update(combined)
            .expect("dynamic filter update should succeed");
        dynamic_filter as PhysicalExprRef
    }

    #[test]
    fn dynamic_filter_inlist_membership_is_declined() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let convertor = DefaultExpressionConvertor::default();
        let filter = bounds_and_inlist_dynamic_filter();

        // The cheap min/max bounds conjuncts enter the scan (driving zone pruning) while
        // the expensive `InList` membership is declined and left to the join hash-probe.
        let conjuncts = split_vortex_pushdown_conjuncts(&convertor, &filter, &schema)
            .expect("split should succeed");
        assert_eq!(
            conjuncts.pushed.len(),
            2,
            "both min/max bounds conjuncts are pushed into the scan"
        );
        assert!(conjuncts.unpushed.is_empty());
        assert_eq!(
            conjuncts.skipped_dynamic.len(),
            1,
            "the InList membership conjunct is declined"
        );
        assert!(conjuncts.skipped_dynamic[0].is::<df_expr::InListExpr>());
    }

    /// A dynamic filter over the columns of `bounds`, each `(name, index, lo,
    /// hi)` naming a column at `index` of the scanned table. Its current value
    /// is `lo <= name AND name <= hi` for every column, as a hash join builds
    /// it for its keys.
    fn bounds_dynamic_filter(bounds: &[(&str, usize, i32, i32)]) -> PhysicalExprRef {
        let columns: Vec<PhysicalExprRef> = bounds
            .iter()
            .map(|&(name, index, _, _)| {
                Arc::new(df_expr::Column::new(name, index)) as PhysicalExprRef
            })
            .collect();
        let current = bounds
            .iter()
            .zip(&columns)
            .flat_map(|(&(_, _, lo, hi), column)| {
                [(Operator::GtEq, lo), (Operator::LtEq, hi)].map(|(op, value)| {
                    Arc::new(df_expr::BinaryExpr::new(
                        Arc::clone(column),
                        op,
                        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(value)))),
                    )) as PhysicalExprRef
                })
            })
            .reduce(|left, right| {
                Arc::new(df_expr::BinaryExpr::new(left, Operator::And, right)) as PhysicalExprRef
            })
            .expect("a dynamic filter needs at least one bound");
        let dynamic_filter = Arc::new(df_expr::DynamicFilterPhysicalExpr::new(
            columns,
            Arc::new(df_expr::Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        dynamic_filter
            .update(current)
            .expect("dynamic filter update should succeed");
        dynamic_filter as PhysicalExprRef
    }

    /// A hash-join dynamic filter on a partition column, scanned through the
    /// source `try_pushdown_filters` plans for it. `FilePruner` evaluates its
    /// predicate against the file schema, which has no partition columns, so
    /// the opener folds each file's partition value into it first. With file
    /// statistics the pruner runs: it skips a file whose partition value the
    /// bound excludes, and keeps a NULL partition, whose bound is unknown.
    #[tokio::test]
    async fn dynamic_filter_on_a_partition_column_prunes_by_partition_value() -> anyhow::Result<()>
    {
        use datafusion_common::config::ConfigOptions;
        use datafusion_datasource::file::FileSource;
        use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
        use datafusion_execution::object_store::ObjectStoreUrl;

        use crate::VortexSource;

        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)]))
            .expect("partition test batch should build");
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/partitioned.vortex";
        let data_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch.clone()).await?;
        let table_schema = TableSchema::builder(batch.schema())
            .with_table_partition_cols(vec![Arc::new(Field::new("part", DataType::Int32, true))])
            .build();

        // `part` follows the file's `a` in the table schema.
        let planned = VortexSource::new(table_schema, SESSION.clone()).try_pushdown_filters(
            vec![bounds_dynamic_filter(&[("part", 1, 3, 7)])],
            &ConfigOptions::default(),
        )?;
        let source = planned
            .updated_node
            .expect("pushing a filter should update the source")
            .with_batch_size(100);

        let mut scanned = Vec::new();
        for partition_value in [None, Some(5), Some(100)] {
            for with_statistics in [false, true] {
                let mut file = PartitionedFile::new(file_path.to_string(), data_size);
                file.partition_values = vec![ScalarValue::Int32(partition_value)];
                file.statistics = with_statistics
                    .then(|| Arc::new(datafusion_common::Statistics::new_unknown(&batch.schema())));
                let config = FileScanConfigBuilder::new(
                    ObjectStoreUrl::parse("memory:///")?,
                    Arc::clone(&source),
                )
                .with_file(file.clone())
                .build();
                let rows: usize = source
                    .create_file_opener(Arc::clone(&object_store), &config, 0)?
                    .open(file)?
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum();
                scanned.push((partition_value, with_statistics, rows));
            }
        }

        // Without statistics no pruner runs and the row filter leaves the file
        // whole; with them only the partition outside `3..=7` is skipped.
        assert_eq!(
            scanned,
            vec![
                (None, false, 3),
                (None, true, 3),
                (Some(5), false, 3),
                (Some(5), true, 3),
                (Some(100), false, 3),
                (Some(100), true, 0),
            ]
        );
        Ok(())
    }
}
