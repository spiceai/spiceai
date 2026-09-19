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

//! [`FirstRecordProbeSource`] — a [`FileSource`] decorator that reads at most the
//! first record of each file.
//!
//! It answers a partition-only `GROUP BY`/`DISTINCT` for formats that carry no
//! exact per-file row count (JSON, CSV, and any file without collected
//! statistics), where [`super::partition_only_scan`]'s statistics fast path
//! cannot apply. Those formats have no footer to read a row count from, so the
//! only way to learn whether a partition's file holds any rows is to decode a
//! record from it — but decoding *one* record is enough:
//!
//! - a partition value is constant across every row of its file, so one row
//!   carries the same partition tuple as the whole file; and
//! - a `DISTINCT`/`GROUP BY` over partition columns needs only the *set* of
//!   tuples, which the downstream aggregate recovers by collapsing the one row
//!   per file this source emits.
//!
//! Correctness rests on reusing the wrapped source's own opener, so "does this
//! file yield a row?" is decided by the identical decode path the full scan
//! would use:
//!
//! - an empty file (or a whitespace-only / header-only file) yields no record,
//!   so the source emits no row and the partition drops out of the result —
//!   exactly as a full scan would exclude it; and
//! - a non-empty file yields exactly one row carrying its partition values.
//!
//! Setting the scan's batch size to one and stopping after the first row bounds
//! the read to a single record's worth of bytes per file (a small object-store
//! prefix for compressed formats), instead of decoding every row of every file.
//!
//! [`FileSource`] carries several methods with a default body (`create_morselizer`,
//! `with_metadata_cols`, `repartitioned`, `try_pushdown_filters`, `try_pushdown_sort`,
//! `try_reverse_output`, `reorder_files`, `try_pushdown_projection`,
//! `with_schema_adapter_factory`, `schema_adapter_factory`). A decorator that
//! silently inherits one of those defaults instead of forwarding to `inner`
//! compiles but drops `inner`'s behavior for that method — for
//! `create_morselizer` in particular, the default calls back into
//! `self.create_file_opener`, which a morsel-only source (Parquet) errors out
//! of. `#[deny(clippy::missing_trait_methods)]` below turns a forgotten
//! override into a compile error instead of a runtime regression.

use std::fmt::{self, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::SortOrderPushdownResult;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::metadata::MetadataColumn;
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
#[expect(
    deprecated,
    reason = "SchemaAdapterFactory is deprecated upstream; FileSource::with_schema_adapter_factory/schema_adapter_factory still require it by name to forward to `inner`"
)]
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::table_schema::TableSchema;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use object_store::ObjectStore;

/// A [`FileSource`] decorator that emits at most one row per file — the first
/// record the wrapped source decodes. See the module documentation for the
/// correctness argument.
pub struct FirstRecordProbeSource {
    inner: Arc<dyn FileSource>,
}

impl FirstRecordProbeSource {
    /// Wrap `inner` so each file it scans yields at most its first record.
    #[must_use]
    pub fn new(inner: Arc<dyn FileSource>) -> Self {
        Self { inner }
    }
}

#[deny(clippy::missing_trait_methods)]
impl FileSource for FirstRecordProbeSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // A batch size of one makes the first decoded batch carry a single row,
        // so stopping after it reads only that record's bytes rather than a full
        // 8192-row batch.
        let inner = self.inner.with_batch_size(1);
        let inner_opener = inner.create_file_opener(object_store, base_config, partition)?;
        Ok(Arc::new(FirstRecordOpener {
            inner: inner_opener,
        }))
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        // A source with a native morsel implementation (e.g. Parquet) errors out
        // of `create_file_opener`, so the probe must reuse `inner`'s own
        // `create_morselizer` rather than the trait's default, which would call
        // back into this source's `create_file_opener`.
        let inner = self.inner.with_batch_size(1);
        let inner_morselizer = inner.create_morselizer(object_store, base_config, partition)?;
        Ok(Box::new(FirstRecordMorselizer {
            inner: inner_morselizer,
        }))
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
        // The probe forces a batch size of one when it opens a file; carry the
        // requested size on the wrapped source so the rest of its configuration
        // is preserved.
        Arc::new(Self {
            inner: self.inner.with_batch_size(batch_size),
        })
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        // A filter pushed into the wrapped scan must still run: it decides which
        // rows count, so it decides whether a file yields a probe row at all.
        self.inner.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        // The projected schema (partition columns only) must be preserved, or
        // the rewritten plan's output schema would not match the original scan.
        self.inner.projection()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        self.inner.fmt_extra(t, f)?;
        // Mark the probe in `EXPLAIN` output so a partition-only scan answered by
        // reading one record per file is distinguishable from a full file scan.
        if matches!(t, DisplayFormatType::Default | DisplayFormatType::Verbose) {
            write!(f, ", first_record_probe=true")?;
        }
        Ok(())
    }

    fn supports_repartitioning(&self) -> bool {
        // The probe collapses each file to one row, so splitting a file into
        // byte-range partitions would read the same file's first record more
        // than once. One file, one probe.
        false
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
        _config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        // `supports_repartitioning` is always `false`: one file must stay one
        // probe, so there is nothing to repartition.
        Ok(None)
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let result = self.inner.try_pushdown_filters(filters, config)?;
        let updated_node = result
            .updated_node
            .map(|new_inner| Arc::new(Self::new(new_inner)) as Arc<dyn FileSource>);
        Ok(FilterPushdownPropagation {
            filters: result.filters,
            updated_node,
        })
    }

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
        _eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        // `build_first_record_probe_scan` drops the original scan's ordering
        // claim, so there is no order left for a sort to be satisfied by.
        Ok(SortOrderPushdownResult::Unsupported)
    }

    /// The deprecated predecessor of `try_pushdown_sort`; kept in lockstep with
    /// it since the default `try_pushdown_sort` would otherwise delegate here.
    fn try_reverse_output(
        &self,
        _order: &[PhysicalSortExpr],
        _eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        Ok(SortOrderPushdownResult::Unsupported)
    }

    fn reorder_files(&self, files: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
        // Each file contributes at most one row regardless of read order, so
        // reordering for e.g. TopK statistics buys nothing here.
        files
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let Some(new_inner) = self.inner.try_pushdown_projection(projection)? else {
            return Ok(None);
        };
        Ok(Some(Arc::new(Self::new(new_inner))))
    }

    #[expect(
        deprecated,
        reason = "SchemaAdapterFactory is deprecated upstream; forward to `inner` until it is removed"
    )]
    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        let new_inner = self.inner.with_schema_adapter_factory(factory)?;
        Ok(Arc::new(Self::new(new_inner)))
    }

    #[expect(
        deprecated,
        reason = "SchemaAdapterFactory is deprecated upstream; forward to `inner` until it is removed"
    )]
    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.inner.schema_adapter_factory()
    }
}

/// A [`FileOpener`] that truncates the wrapped opener's per-file stream to its
/// first row.
struct FirstRecordOpener {
    inner: Arc<dyn FileOpener>,
}

impl FileOpener for FirstRecordOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let inner = self.inner.open(partitioned_file)?;
        Ok(async move {
            let stream = inner.await?;
            let first_row = stream
                .filter_map(|batch| async move {
                    match batch {
                        // Skip empty batches: an empty file must contribute no
                        // row so its partition drops out of a `DISTINCT`.
                        Ok(batch) if batch.num_rows() == 0 => None,
                        // Keep one row; a partition value is constant across the
                        // file, so any row carries the same partition tuple.
                        Ok(batch) => Some(Ok(batch.slice(0, 1))),
                        Err(err) => Some(Err(err)),
                    }
                })
                .take(1)
                .boxed();
            Ok(first_row)
        }
        .boxed())
    }
}

/// A [`Morselizer`] decorator for a source whose native morsel implementation
/// must be used instead of the legacy [`FileOpener`] API (Parquet errors out of
/// `create_file_opener` for exactly this reason). Truncates the wrapped
/// morselizer's per-file output to its first non-empty row, the morsel-API
/// counterpart of [`FirstRecordOpener`].
#[derive(Debug)]
struct FirstRecordMorselizer {
    inner: Box<dyn Morselizer>,
}

impl Morselizer for FirstRecordMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        let inner = self.inner.plan_file(file)?;
        Ok(Box::new(FirstRecordMorselPlanner {
            inner,
            // Shared across every morsel/planner this file's plan produces, so
            // a row surfaced by one morsel suppresses the rest of the file's
            // morsels rather than each independently emitting its own first row.
            emitted: Arc::new(AtomicBool::new(false)),
        }))
    }
}

/// A [`MorselPlanner`] decorator that stops planning once this file's first row
/// has been emitted, and wraps every morsel/child planner it does produce with
/// the same first-row truncation. See [`FirstRecordMorselizer`].
#[derive(Debug)]
struct FirstRecordMorselPlanner {
    inner: Box<dyn MorselPlanner>,
    emitted: Arc<AtomicBool>,
}

impl MorselPlanner for FirstRecordMorselPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        if self.emitted.load(Ordering::Acquire) {
            return Ok(None);
        }

        let Self { inner, emitted } = *self;
        let Some(mut plan) = inner.plan()? else {
            return Ok(None);
        };

        let morsels = plan
            .take_morsels()
            .into_iter()
            .map(|inner| {
                Box::new(FirstRecordMorsel {
                    inner,
                    emitted: Arc::clone(&emitted),
                }) as Box<dyn Morsel>
            })
            .collect();
        let planners = plan
            .take_ready_planners()
            .into_iter()
            .map(|inner| {
                Box::new(FirstRecordMorselPlanner {
                    inner,
                    emitted: Arc::clone(&emitted),
                }) as Box<dyn MorselPlanner>
            })
            .collect();

        let mut new_plan = MorselPlan::new()
            .with_morsels(morsels)
            .with_planners(planners);

        if let Some(pending) = plan.take_pending_planner() {
            let emitted = Arc::clone(&emitted);
            new_plan.set_pending_planner(async move {
                let inner = pending.into_future().await?;
                Ok(Box::new(FirstRecordMorselPlanner { inner, emitted }) as Box<dyn MorselPlanner>)
            });
        }

        Ok(Some(new_plan))
    }
}

/// A [`Morsel`] decorator that truncates its inner stream to the first
/// non-empty batch (sliced to one row) and marks `emitted` so sibling morsels
/// of the same file stop contributing once a row has been found.
#[derive(Debug)]
struct FirstRecordMorsel {
    inner: Box<dyn Morsel>,
    emitted: Arc<AtomicBool>,
}

impl Morsel for FirstRecordMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        let emitted = self.emitted;
        self.inner
            .into_stream()
            .filter_map(move |batch| {
                let emitted = Arc::clone(&emitted);
                async move {
                    // Another morsel of this file already produced the row;
                    // skip decoding further batches from this one.
                    if emitted.load(Ordering::Acquire) {
                        return None;
                    }
                    match batch {
                        Ok(batch) if batch.num_rows() == 0 => None,
                        Ok(batch) => {
                            emitted.store(true, Ordering::Release);
                            Some(Ok(batch.slice(0, 1)))
                        }
                        Err(err) => Some(Err(err)),
                    }
                }
            })
            .take(1)
            .boxed()
    }
}
