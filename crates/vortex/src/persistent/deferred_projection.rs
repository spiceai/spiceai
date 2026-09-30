// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;

use vortex::array::MaskFuture;
use vortex::dtype::DType;
use vortex::dtype::FieldMask;
use vortex::error::VortexResult;
use vortex::expr::Expression;
use vortex::layout::ArrayFuture;
use vortex::layout::LayoutReader;
use vortex::layout::RowSplits;
use vortex::layout::SplitRange;
use vortex::mask::Mask;

/// Defers projection setup, and the segment reads it registers, until the scan's filter
/// has resolved.
///
/// Vortex builds each split's task by calling `projection_evaluation` up front and only
/// then awaiting the filter mask, discarding the projection future when the mask comes
/// back all-false. Building the projection is not free: it walks the layout to construct
/// the output columns' readers and calls `SegmentSource::request` for each of their
/// segments. A registered-but-never-polled request is never dispatched on its own, but it
/// stays in the read driver's spatial index, where the coalescer will pull it into a
/// neighbouring physical read — so the output columns of a split that matches nothing can
/// still be fetched from storage.
///
/// Wrapping the root reader moves that work behind the mask: the inner
/// `projection_evaluation` runs only once a caller polls the returned future, which the
/// scan does only for splits that survive the filter.
///
/// Only applied to filtered scans. Without a filter every split's mask is already
/// resolved, so there is nothing to defer, and the eager registration is what lets the
/// coalescer merge the projection reads of adjacent splits.
pub(crate) struct DeferredProjectionReader {
    inner: Arc<dyn LayoutReader>,
}

impl DeferredProjectionReader {
    pub(crate) fn new(inner: Arc<dyn LayoutReader>) -> Self {
        Self { inner }
    }
}

impl LayoutReader for DeferredProjectionReader {
    fn name(&self) -> &Arc<str> {
        self.inner.name()
    }

    /// Reports the wrapper, not the reader it wraps: a caller that downcasts to a concrete
    /// layout reader must not be handed one whose projection is no longer deferred.
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn dtype(&self) -> &DType {
        self.inner.dtype()
    }

    fn row_count(&self) -> u64 {
        self.inner.row_count()
    }

    fn register_splits(
        &self,
        field_mask: &[FieldMask],
        split_range: &SplitRange,
        splits: &mut RowSplits,
    ) -> VortexResult<()> {
        self.inner.register_splits(field_mask, split_range, splits)
    }

    fn pruning_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &Expression,
        mask: Mask,
    ) -> VortexResult<MaskFuture> {
        self.inner.pruning_evaluation(row_range, expr, mask)
    }

    fn filter_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &Expression,
        mask: MaskFuture,
    ) -> VortexResult<MaskFuture> {
        self.inner.filter_evaluation(row_range, expr, mask)
    }

    fn projection_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &Expression,
        mask: MaskFuture,
    ) -> VortexResult<ArrayFuture> {
        let inner = Arc::clone(&self.inner);
        let row_range = row_range.clone();
        let expr = expr.clone();
        Ok(Box::pin(async move {
            // `MaskFuture` is a shared future, so awaiting this clone resolves against the
            // same filter evaluation the scan awaits before it polls us.
            let mask = mask.await?;
            inner
                .projection_evaluation(&row_range, &expr, MaskFuture::ready(mask))?
                .await
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion_execution::TaskContext;
    use datafusion_physical_plan::ExecutionPlan;
    use datafusion_physical_plan::collect;
    use datafusion_physical_plan::metrics::MetricValue;
    use datafusion_physical_plan::metrics::MetricsSet;
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;
    use vortex::VortexSessionDefault;
    use vortex::array::IntoArray;
    use vortex::array::VortexSessionExecute;
    use vortex::array::arrays::ChunkedArray;
    use vortex::array::arrays::StructArray;
    use vortex::array::arrays::VarBinArray;
    use vortex::array::validity::Validity;
    use vortex::buffer::Buffer;
    use vortex::buffer::buffer;
    use vortex::error::vortex_err;
    use vortex::expr::gt;
    use vortex::expr::lit;
    use vortex::expr::root;
    use vortex::file::WriteOptionsSessionExt;
    use vortex::io::VortexWrite;
    use vortex::io::object_store::ObjectStoreWrite;
    use vortex::layout::scan::scan_builder::ScanBuilder;
    use vortex::scalar::Scalar;
    use vortex::session::VortexSession;

    use super::*;
    use crate::common_tests::TestSessionContext;
    use crate::persistent::metrics::VortexMetricsFinder;

    struct CountingReader {
        name: Arc<str>,
        dtype: DType,
        projection_calls: Arc<AtomicUsize>,
        reject: bool,
    }

    impl LayoutReader for CountingReader {
        fn name(&self) -> &Arc<str> {
            &self.name
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn dtype(&self) -> &DType {
            &self.dtype
        }

        fn row_count(&self) -> u64 {
            4
        }

        fn register_splits(
            &self,
            _field_mask: &[FieldMask],
            split_range: &SplitRange,
            splits: &mut RowSplits,
        ) -> VortexResult<()> {
            splits.push(split_range.root_row_range().end);
            Ok(())
        }

        fn pruning_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &Expression,
            mask: Mask,
        ) -> VortexResult<MaskFuture> {
            Ok(MaskFuture::ready(if self.reject {
                Mask::new_false(mask.len())
            } else {
                mask
            }))
        }

        fn filter_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &Expression,
            mask: MaskFuture,
        ) -> VortexResult<MaskFuture> {
            Ok(mask)
        }

        fn projection_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &Expression,
            mask: MaskFuture,
        ) -> VortexResult<ArrayFuture> {
            self.projection_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Box::pin(async move {
                buffer![1i32, 2, 3, 4].into_array().filter(mask.await?)
            }))
        }
    }

    fn reader(reject: bool) -> (Arc<dyn LayoutReader>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(CountingReader {
            name: Arc::from("counting"),
            dtype: buffer![1i32, 2, 3, 4].into_array().dtype().clone(),
            projection_calls: Arc::clone(&calls),
            reject,
        });
        (Arc::new(DeferredProjectionReader::new(inner)), calls)
    }

    #[tokio::test]
    async fn rejected_split_does_not_construct_a_projection() -> VortexResult<()> {
        let (reader, calls) = reader(true);
        let arrays = ScanBuilder::new(VortexSession::default(), reader)
            .with_filter(gt(root(), lit(0i32)))
            .into_stream()?
            .try_collect::<Vec<_>>()
            .await?;
        assert!(arrays.is_empty());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn matching_split_preserves_all_projected_values() -> VortexResult<()> {
        let (reader, calls) = reader(false);
        let arrays = ScanBuilder::new(VortexSession::default(), reader)
            .with_filter(gt(root(), lit(0i32)))
            .into_stream()?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(arrays.len(), 1);
        assert_eq!(arrays[0].len(), 4);
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        for (index, value) in [1i32, 2, 3, 4].into_iter().enumerate() {
            assert_eq!(
                arrays[0].execute_scalar(index, &mut ctx)?,
                Scalar::from(value)
            );
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[tokio::test]
    async fn failed_filter_does_not_construct_a_projection() -> VortexResult<()> {
        let (reader, calls) = reader(false);
        let mask = MaskFuture::new(4, async { Err(vortex_err!("filter failed")) });
        let result = reader.projection_evaluation(&(0..4), &root(), mask)?.await;
        assert!(
            result.is_err(),
            "a failed filter must surface as a failed projection"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        Ok(())
    }

    /// The scan wiring, not just the wrapper: bytes read must fall with selectivity.
    ///
    /// The four tests above construct `DeferredProjectionReader` directly and all still
    /// pass if `VortexOpener` never wraps anything, so none of them guards the seam. This
    /// one goes through the real opener and measures `vortex.io.read.total_size` off the
    /// executed plan.
    ///
    /// Measured on the file it writes (13,998,704 bytes, 64 chunks, `id` plus four payload
    /// columns), eager setup versus deferred:
    ///
    /// | rows matched | before    | after     |
    /// |--------------|-----------|-----------|
    /// | 1 of 262,144 | 3,496,560 |   208,424 |
    /// | half         | 3,923,936 | 1,734,364 |
    /// | all          | 5,229,296 | 3,937,280 |
    ///
    /// Without deferral the point lookup reads the whole `p0` column: all 17 of its
    /// segments are registered up front, and the read driver's coalescer merges them into
    /// one read spanning the column even though 63 of the 64 chunks match nothing.
    ///
    /// Only the two selective cases carry a byte budget. The all-rows gap is 1.33x, too
    /// narrow to assert without turning a compression or layout change in a Vortex bump
    /// into a spurious failure; it is covered for row count and values instead, which is
    /// what says the deferral did not break unselective scans.
    #[tokio::test(flavor = "multi_thread")]
    async fn filtered_scan_reads_fewer_bytes_as_selectivity_rises() -> anyhow::Result<()> {
        let ctx = fixture().await?;

        // Budgets sit roughly midway between the measured arms, on a log scale.
        let point = ctx
            .run("SELECT p0 FROM '/deferred_projection.vortex' WHERE id = 5")
            .await?;
        assert_eq!(point.rows, 1, "the predicate matches exactly one row");
        assert_eq!(
            point.first_value,
            Some(format!("0-0-5-{}", "x".repeat(48))),
            "the deferred projection must return the value the eager one did"
        );
        point.assert_bytes_under(700_000, "point lookup");

        let half = ctx
            .run("SELECT p0 FROM '/deferred_projection.vortex' WHERE id < 131072")
            .await?;
        assert_eq!(half.rows, 131_072);
        half.assert_bytes_under(2_600_000, "half the rows");

        // Aggregated rather than row-by-row: the scan is concurrent and unordered, so which
        // row arrives first is not defined, but the projection still runs over every row.
        let all = ctx
            .run(
                "SELECT count(p0) AS n, min(p0) AS lo, max(p0) AS hi \
                 FROM '/deferred_projection.vortex' WHERE id >= 0",
            )
            .await?;
        assert_eq!(all.rows, 1);
        assert_eq!(
            all.row_values()?,
            vec![
                "262144".to_string(),
                format!("0-0-0-{}", "x".repeat(48)),
                format!("0-9-999-{}", "x".repeat(48)),
            ],
            "an unselective filtered scan must project every row unchanged"
        );

        Ok(())
    }

    /// A Vortex file plus a session that can query it, shared by the cases above.
    struct Fixture {
        ctx: TestSessionContext,
        file_bytes: u64,
    }

    /// What one query read, and what it returned.
    struct Scan {
        rows: usize,
        first_value: Option<String>,
        first_row: Option<RecordBatch>,
        bytes_read: u64,
        reads: u64,
        file_bytes: u64,
    }

    impl Scan {
        /// Every column of the first row, as display strings. Only meaningful for a query
        /// whose result is a single row.
        fn row_values(&self) -> anyhow::Result<Vec<String>> {
            let batch = self
                .first_row
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("query returned no rows"))?;
            Ok((0..batch.num_columns())
                .map(|column| array_value_to_string(batch.column(column), 0))
                .collect::<Result<Vec<_>, _>>()?)
        }

        fn assert_bytes_under(&self, budget: u64, what: &str) {
            assert!(
                self.bytes_read > 0,
                "{what} reported zero bytes read, so the {budget}-byte budget below would \
                 pass without the scan having done any I/O"
            );
            assert!(
                self.bytes_read < budget,
                "{what} read {} bytes of a {}-byte file in {} reads, over the {budget}-byte \
                 budget: projection setup is running before the filter resolves",
                self.bytes_read,
                self.file_bytes,
                self.reads,
            );
        }
    }

    /// Writes a 64-chunk file: an `id` column plus four payload columns, so the projected
    /// column is not adjacent to the filter column. That is the layout in which an eager
    /// projection read spans the file.
    async fn fixture() -> anyhow::Result<Fixture> {
        const CHUNKS: u32 = 64;
        const ROWS_PER_CHUNK: u32 = 4096;

        let ctx = TestSessionContext::default();
        let session = VortexSession::default();

        let ids = (0..CHUNKS)
            .map(|chunk| {
                (0..ROWS_PER_CHUNK)
                    .map(|row| chunk * ROWS_PER_CHUNK + row)
                    .collect::<Buffer<_>>()
                    .into_array()
            })
            .collect::<ChunkedArray>()
            .into_array();

        let payload = |salt: u32| {
            (0..CHUNKS)
                .map(|chunk| {
                    VarBinArray::from(
                        (0..ROWS_PER_CHUNK)
                            .map(|row| format!("{salt}-{chunk}-{row}-{}", "x".repeat(48)))
                            .collect::<Vec<_>>(),
                    )
                    .into_array()
                })
                .collect::<ChunkedArray>()
                .into_array()
        };

        let table = StructArray::try_new(
            ["id", "p0", "p1", "p2", "p3"].into(),
            vec![ids, payload(0), payload(1), payload(2), payload(3)],
            (CHUNKS * ROWS_PER_CHUNK) as usize,
            Validity::NonNullable,
        )?;

        let path = "deferred_projection.vortex".into();
        let mut writer = ObjectStoreWrite::new(ctx.store.clone(), &path).await?;
        session
            .write_options()
            .write(&mut writer, table.into_array().to_array_stream())
            .await?;
        writer.shutdown().await?;
        let file_bytes = ctx.store.head(&path).await?.size;

        Ok(Fixture { ctx, file_bytes })
    }

    impl Fixture {
        async fn run(&self, sql: &str) -> anyhow::Result<Scan> {
            let df = self.ctx.session.sql(sql).await?;
            let plan = self
                .ctx
                .session
                .state()
                .create_physical_plan(df.logical_plan())
                .await?;
            let task_ctx = Arc::new(TaskContext::from(&self.ctx.session.state()));
            let batches = collect(Arc::clone(&plan), task_ctx).await?;

            let first_row = batches.iter().find(|batch| batch.num_rows() > 0).cloned();
            let first_value = first_row
                .as_ref()
                .map(|batch| array_value_to_string(batch.column(0), 0))
                .transpose()?;

            Ok(Scan {
                rows: batches.iter().map(RecordBatch::num_rows).sum(),
                first_value,
                first_row,
                bytes_read: sum_metric(plan.as_ref(), "vortex.io.read.total_size")?,
                reads: sum_metric(plan.as_ref(), "vortex.io.read.size_count")?,
                file_bytes: self.file_bytes,
            })
        }
    }

    /// Sums one Vortex counter across every Vortex scan in `plan`.
    ///
    /// Errors when no scan reported the counter at all, rather than summing to zero: a
    /// budget compared against a metric that stopped being collected — renamed upstream, or
    /// no longer reachable from the plan — would pass no matter how much the scan read.
    fn sum_metric(plan: &dyn ExecutionPlan, metric_name: &str) -> anyhow::Result<u64> {
        let sets = VortexMetricsFinder::find_all(plan);
        let mut total = 0u64;
        let mut found = false;
        for metric in sets.iter().flat_map(MetricsSet::iter) {
            if let MetricValue::Count { name, count } = metric.value()
                && name == metric_name
            {
                total += count.value() as u64;
                found = true;
            }
        }
        anyhow::ensure!(
            found,
            "no Vortex scan reported `{metric_name}`, so this measurement is not measuring \
             anything; {} metric set(s) were found on the plan",
            sets.len()
        );
        Ok(total)
    }

    #[tokio::test]
    async fn sparse_mask_preserves_selected_values() -> VortexResult<()> {
        let (reader, calls) = reader(false);
        let mask = MaskFuture::ready(Mask::from_iter([false, true, false, true]));
        let projected = reader.projection_evaluation(&(0..4), &root(), mask)?;
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        let array = projected.await?;
        assert_eq!(array.len(), 2);
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        assert_eq!(array.execute_scalar(0, &mut ctx)?, Scalar::from(2i32));
        assert_eq!(array.execute_scalar(1, &mut ctx)?, Scalar::from(4i32));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        Ok(())
    }
}
