/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Write-back execution path for [`WriteMode::WriteBack`].
//!
//! Writes are streamed directly into the local accelerator. The synchronous
//! response returns once the accelerator commit completes. This path does
//! not forward writes to the federated source itself; an external
//! mechanism must keep the source in sync. Write-back is gated by
//! validation that requires `replication.enabled: true` as the user's
//! attestation that source synchronization is handled
//! (`acceleration.on_conflict` is rejected separately because it declares
//! accelerator-only semantics; `refresh_mode: changes` alone is not
//! sufficient because it is a source-to-accelerator stream).
//!
//! No batches are buffered in memory in this path: the caller's input
//! [`ExecutionPlan`] is handed directly to
//! [`TableProvider::insert_into`](datafusion::datasource::TableProvider::insert_into)
//! on the accelerator, so `DataFusion`'s streaming execution is preserved.
//!
//! [`WriteMode::WriteBack`]: super::WriteMode::WriteBack

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::dml::InsertOp;

/// Returns an error for `InsertOp::Overwrite` / `InsertOp::Replace` because
/// the federated source is updated out-of-band by the refresh mechanism, so
/// destructive writes against the accelerator can leave the source diverged
/// (or in an inconsistent state under partial replication). Append is the
/// only mode whose semantics are well-defined for write-back today.
pub(crate) fn validate_insert_op(overwrite: InsertOp) -> DataFusionResult<()> {
    match overwrite {
        InsertOp::Append => Ok(()),
        InsertOp::Overwrite | InsertOp::Replace => Err(DataFusionError::Plan(
            "Write-back accelerated tables currently support append writes only".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_insert_op_allows_append() {
        validate_insert_op(InsertOp::Append).expect("Append must be accepted by write-back");
    }

    #[test]
    fn validate_insert_op_rejects_overwrite() {
        let err = validate_insert_op(InsertOp::Overwrite)
            .expect_err("Overwrite must be rejected by write-back validation");
        assert!(
            err.to_string().contains("append writes only"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn validate_insert_op_rejects_replace() {
        let err = validate_insert_op(InsertOp::Replace)
            .expect_err("Replace must be rejected by write-back validation");
        assert!(
            err.to_string().contains("append writes only"),
            "unexpected error message: {err}"
        );
    }
}
