// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Spice.ai OSS Authors

//! Reports where each written batch lands, so a caller can build a row-address
//! index during the write instead of reading the finished files back.

use std::fmt::Debug;

use datafusion_common::arrow::array::RecordBatch;
use object_store::path::Path;

/// Observes the placement of every batch a Vortex write emits.
///
/// The writer knows which file a batch is appended to and how many rows precede
/// it there; without this hook that pairing is discarded and a caller that needs
/// row addresses has to re-derive it by scanning the finished files.
///
/// # Position contract
///
/// `first_row_position` is the file-local physical position of the batch's first
/// row, counted from zero over the rows written to `file_path`. It is exact for
/// the whole batch because a batch is appended whole and never straddles two
/// files: the writer rolls to a new file only after the current batch has been
/// handed to the active writer.
///
/// These are the same positions a full unfiltered scan of the finished file
/// yields in order, which is what makes them usable as
/// `Selection::IncludeByIndex` inputs. That equivalence rests on the writer
/// appending batches in arrival order, so a caller that treats the positions as
/// authoritative should verify a sample against the written file rather than
/// trust the invariant silently.
///
/// Called from the shard writer task, so implementations must be cheap and must
/// tolerate concurrent calls from every shard.
pub trait VortexWriteObserver: Debug + Send + Sync + 'static {
    /// A batch of `batch.num_rows()` rows is being appended to `file_path`,
    /// occupying positions `first_row_position .. first_row_position + num_rows`.
    fn batch_written(&self, file_path: &Path, first_row_position: u64, batch: &RecordBatch);
}
