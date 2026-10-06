// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2024-2026 The Spice.ai OSS Authors

//! Per-file key blocks: the exact minimum and maximum of a key column over each
//! [`BLOCK_ROWS`]-row block of a file.
//!
//! An equality on the key can only match inside the blocks whose bounds hold the
//! key, so the blocks turn a lookup into a few candidate row ranges, and a file
//! with no candidate block is not read at all. The alternative is the zone-map
//! pruning a scan runs on every query, which rewrites the predicate against every
//! zone of the file and builds a row mask as long as the file — the largest single
//! cost of a point lookup.
//!
//! The bounds are built from the column's data the first time a lookup reaches a
//! file and cost 17 bytes per block (about 5 KiB for 2.5 million rows). Because
//! they are exact, a block that holds the key is never left out, and the read
//! still evaluates the whole predicate over the candidate rows.

use std::hash::BuildHasherDefault;
use std::ops::Range;
use std::sync::Arc;
use std::sync::LazyLock;

use arrow_schema::DataType;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::AsArray;
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::Int64Type;
use datafusion_common::exec_datafusion_err;
use moka::future::Cache;
use object_store::ObjectMeta;
use object_store::path::Path;
use twox_hash::XxHash3_64;
use vortex::array::MaskFuture;
use vortex::array::VortexSessionExecute;
use vortex::array::expr::col;
use vortex::arrow::ArrowSessionExt;
use vortex::layout::LayoutReader;
use vortex::session::VortexSession;

/// Rows summarized by one block.
pub(crate) const BLOCK_ROWS: u64 = 8192;

/// Blocks decoded per step of a build. A step decodes at most 1 MiB of keys, so a
/// large file never holds its whole key column decoded at once, and the build
/// yields to the scheduler between steps.
const BUILD_STEP_BLOCKS: u64 = 16;

/// Files with more rows than this are not indexed, which bounds the one-time
/// decode a lookup can trigger.
const MAX_INDEXED_ROWS: u64 = 256 * 1024 * 1024;

/// Capacity of the process-wide cache of bounds, in bytes.
const CACHE_CAPACITY_BYTES: u64 = 16 * 1024 * 1024;

/// Bytes charged per cached file on top of its bounds: the key and the cache's
/// own bookkeeping. It also bounds the number of files the cache holds, including
/// the ones that could not be indexed.
const ENTRY_OVERHEAD_BYTES: u64 = 256;

/// Exact per-block bounds of one integer key column of one file.
#[derive(Debug)]
pub(crate) struct KeyBlocks {
    row_count: u64,
    mins: Vec<i64>,
    maxs: Vec<i64>,
    /// Whether the block holds any non-null key; an all-null block never matches
    /// an equality.
    any: Vec<bool>,
}

impl KeyBlocks {
    fn size_bytes(&self) -> u64 {
        let per_block = 2 * size_of::<i64>() + size_of::<bool>();
        u64::try_from(self.mins.len() * per_block).unwrap_or(u64::MAX)
    }

    /// Row ranges of the blocks whose bounds hold `key`, in row order, with
    /// adjacent blocks merged. Empty when no row of the file can equal `key`.
    pub(crate) fn candidate_ranges(&self, key: i64) -> Vec<Range<u64>> {
        let mut ranges: Vec<Range<u64>> = Vec::new();
        let mut start = 0;
        for ((&min, &max), &any) in self.mins.iter().zip(&self.maxs).zip(&self.any) {
            let end = (start + BLOCK_ROWS).min(self.row_count);
            if any && min <= key && key <= max {
                match ranges.last_mut() {
                    Some(last) if last.end == start => last.end = end,
                    _ => ranges.push(start..end),
                }
            }
            start = end;
        }
        ranges
    }
}

/// Identifies a file's content and the indexed column. Paths are relative to their
/// object store, so the store is part of the key; size and modification time are
/// what `DataFusion` checks before serving a cached footer.
#[derive(Clone, PartialEq, Eq, Hash)]
struct BlocksKey {
    store: Arc<str>,
    path: Path,
    size: u64,
    modified_micros: i64,
    column: Arc<str>,
}

impl BlocksKey {
    fn new(store: &Arc<str>, meta: &ObjectMeta, column: &Arc<str>) -> Self {
        Self {
            store: Arc::clone(store),
            path: meta.location.clone(),
            size: meta.size,
            modified_micros: meta.last_modified.timestamp_micros(),
            column: Arc::clone(column),
        }
    }
}

type BlocksCache = Cache<BlocksKey, Option<Arc<KeyBlocks>>, BuildHasherDefault<XxHash3_64>>;

static KEY_BLOCKS: LazyLock<BlocksCache> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(CACHE_CAPACITY_BYTES)
        .weigher(|_, blocks: &Option<Arc<KeyBlocks>>| {
            let bytes = ENTRY_OVERHEAD_BYTES + blocks.as_ref().map_or(0, |b| b.size_bytes());
            u32::try_from(bytes).unwrap_or(u32::MAX)
        })
        .build_with_hasher(BuildHasherDefault::default())
});

/// Whether a column of this type can be indexed: an integer type whose every value
/// fits in an `i64`.
pub(crate) fn is_indexable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
    )
}

/// The value of an integer literal as an `i64`, or `None` for a null or any other
/// type.
pub(crate) fn integer_literal(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt8(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i64::from(*v)),
        _ => None,
    }
}

/// The key blocks of `column` in the file `reader` reads, built on first use and
/// shared by every later scan of the file.
///
/// `None` when the file is too large to index or its column cannot be read as
/// integers; the caller then scans as it would without them. Only sound for files
/// that are never rewritten in place.
pub(crate) async fn key_blocks(
    reader: &Arc<dyn LayoutReader>,
    session: &VortexSession,
    store: &Arc<str>,
    meta: &ObjectMeta,
    column: &Arc<str>,
) -> Option<Arc<KeyBlocks>> {
    if reader.row_count() > MAX_INDEXED_ROWS {
        return None;
    }
    KEY_BLOCKS
        .get_with(BlocksKey::new(store, meta, column), async {
            match build(reader.as_ref(), session, column).await {
                Ok(blocks) => Some(Arc::new(blocks)),
                Err(error) => {
                    tracing::debug!(
                        path = %meta.location,
                        column = %column,
                        %error,
                        "Vortex key blocks unavailable; the file is scanned without them"
                    );
                    None
                }
            }
        })
        .await
}

/// The key blocks of `column` already cached for the file `meta` describes,
/// without reading the file.
pub(crate) async fn cached_key_blocks(
    store: &Arc<str>,
    meta: &ObjectMeta,
    column: &Arc<str>,
) -> Option<Arc<KeyBlocks>> {
    KEY_BLOCKS
        .get(&BlocksKey::new(store, meta, column))
        .await
        .flatten()
}

async fn build(
    reader: &dyn LayoutReader,
    session: &VortexSession,
    column: &str,
) -> DFResult<KeyBlocks> {
    let row_count = reader.row_count();
    let blocks = usize::try_from(row_count.div_ceil(BLOCK_ROWS))
        .map_err(|_| exec_datafusion_err!("Vortex file row count exceeds usize"))?;
    let block_rows = usize::try_from(BLOCK_ROWS)
        .map_err(|_| exec_datafusion_err!("Vortex key block size exceeds usize"))?;
    let mut bounds = KeyBlocks {
        row_count,
        mins: Vec::with_capacity(blocks),
        maxs: Vec::with_capacity(blocks),
        any: Vec::with_capacity(blocks),
    };
    let key = col(column).bind(reader.dtype()).map_err(vortex_error)?;

    let mut start = 0;
    while start < row_count {
        let end = (start + BUILD_STEP_BLOCKS * BLOCK_ROWS).min(row_count);
        let rows = usize::try_from(end - start)
            .map_err(|_| exec_datafusion_err!("Vortex key block step exceeds usize"))?;
        let values = reader
            .projection_evaluation(&(start..end), &key, MaskFuture::new_true(rows))
            .map_err(vortex_error)?
            .await
            .map_err(vortex_error)?;
        let mut ctx = session.create_execution_ctx();
        let arrow_session = ctx.session().clone();
        let values = arrow_session
            .arrow()
            .execute_arrow(values, None, &mut ctx)
            .map_err(vortex_error)?;
        let values = compute::cast(&values, &DataType::Int64)?;
        let values = values
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| exec_datafusion_err!("Vortex key column did not cast to Int64"))?;

        let mut offset = 0;
        while offset < rows {
            let block = values.slice(offset, block_rows.min(rows - offset));
            // Both are `None` exactly when every key in the block is null.
            let (min, max) = (compute::min(&block), compute::max(&block));
            bounds.any.push(min.is_some());
            bounds.mins.push(min.unwrap_or_default());
            bounds.maxs.push(max.unwrap_or_default());
            offset += block_rows;
        }

        start = end;
        tokio::task::consume_budget().await;
    }
    Ok(bounds)
}

fn vortex_error(error: vortex::error::VortexError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::BLOCK_ROWS;
    use super::KeyBlocks;

    fn blocks(bounds: &[Option<(i64, i64)>], row_count: u64) -> KeyBlocks {
        KeyBlocks {
            row_count,
            mins: bounds.iter().map(|b| b.map_or(0, |(lo, _)| lo)).collect(),
            maxs: bounds.iter().map(|b| b.map_or(0, |(_, hi)| hi)).collect(),
            any: bounds.iter().map(Option::is_some).collect(),
        }
    }

    #[test]
    fn candidates_are_the_blocks_whose_bounds_hold_the_key() {
        let b = blocks(
            &[
                Some((0, 9)),
                Some((10, 19)),
                Some((5, 25)),
                None,
                Some((20, 29)),
            ],
            4 * BLOCK_ROWS + 100,
        );
        let none: Vec<Range<u64>> = Vec::new();
        assert_eq!(b.candidate_ranges(-1), none);
        assert_eq!(b.candidate_ranges(0), vec![0..BLOCK_ROWS]);
        // Blocks 0 and 2 hold 7; block 1 does not, so the ranges stay apart.
        assert_eq!(
            b.candidate_ranges(7),
            vec![0..BLOCK_ROWS, 2 * BLOCK_ROWS..3 * BLOCK_ROWS]
        );
        // Adjacent candidate blocks merge into one range.
        assert_eq!(b.candidate_ranges(15), vec![BLOCK_ROWS..3 * BLOCK_ROWS]);
        // The all-null block 3 never matches, and the last block is short.
        assert_eq!(
            b.candidate_ranges(22),
            vec![
                2 * BLOCK_ROWS..3 * BLOCK_ROWS,
                4 * BLOCK_ROWS..4 * BLOCK_ROWS + 100
            ]
        );
        assert_eq!(
            b.candidate_ranges(29),
            vec![4 * BLOCK_ROWS..4 * BLOCK_ROWS + 100]
        );
        assert_eq!(b.candidate_ranges(30), none);
    }

    #[test]
    fn extreme_keys_match_only_blocks_that_hold_them() {
        let b = blocks(&[Some((i64::MIN, -1)), Some((0, i64::MAX))], 2 * BLOCK_ROWS);
        assert_eq!(b.candidate_ranges(i64::MIN), vec![0..BLOCK_ROWS]);
        assert_eq!(
            b.candidate_ranges(i64::MAX),
            vec![BLOCK_ROWS..2 * BLOCK_ROWS]
        );
    }
}
