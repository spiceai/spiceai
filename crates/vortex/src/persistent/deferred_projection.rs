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

    /// The scan wiring, not just the wrapper: a point lookup through the real
    /// `VortexOpener` must not read the whole projected column.
    ///
    /// The four tests above construct `DeferredProjectionReader` directly and all still
    /// pass if `VortexOpener` never wraps anything, so none of them guards the seam. This
    /// one measures the bytes Vortex actually pulled from the object store.
    ///
    /// Measured on the file this test writes (13,998,704 bytes, 64 chunks, `id` plus four
    /// payload columns): the point lookup reads 3,496,560 bytes without the wrapper and
    /// 208,424 bytes with it. Without deferral every chunk's `p0` segment is registered up
    /// front and the read driver's coalescer merges all 17 of them into one read spanning
    /// the entire column, even though 63 of the 64 chunks match nothing. The 5%-of-file
    /// budget below sits between the two by more than 3x on either side.
    #[tokio::test(flavor = "multi_thread")]
    async fn point_lookup_does_not_read_the_whole_projected_column() -> anyhow::Result<()> {
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

        // Four payload columns so the projected column is not adjacent to the filter
        // column, which is the layout that lets an eager projection read span the file.
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

        let df = ctx
            .session
            .sql("SELECT p0 FROM '/deferred_projection.vortex' WHERE id = 5")
            .await?;
        let plan = ctx
            .session
            .state()
            .create_physical_plan(df.logical_plan())
            .await?;
        let task_ctx = Arc::new(TaskContext::from(&ctx.session.state()));
        let batches = collect(Arc::clone(&plan), task_ctx).await?;

        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "the predicate matches exactly one row");
        assert_eq!(
            array_value_to_string(batches[0].column(0), 0)?,
            format!("0-0-5-{}", "x".repeat(48)),
            "the deferred projection must return the same value the eager one did"
        );

        let bytes_read = read_total_size(plan.as_ref());
        let budget = file_bytes / 20;
        assert!(
            bytes_read < budget,
            "a point lookup read {bytes_read} bytes of a {file_bytes}-byte file, over the \
             {budget}-byte budget: the projection is being set up before the filter resolves"
        );

        Ok(())
    }

    /// Sums `vortex.io.read.total_size` across every Vortex scan in `plan`.
    fn read_total_size(plan: &dyn ExecutionPlan) -> u64 {
        VortexMetricsFinder::find_all(plan)
            .iter()
            .flat_map(MetricsSet::iter)
            .filter_map(|metric| match metric.value() {
                MetricValue::Count { name, count } if name == "vortex.io.read.total_size" => {
                    Some(count.value() as u64)
                }
                _ => None,
            })
            .sum()
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
