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

//! Durable federated write-back delivery worker (#11838).
//!
//! One worker per durable-write-back Cayenne dataset, spawned onto the
//! [`AcceleratedTable`](super::AcceleratedTable)'s `handlers` (aborted on drop).
//! It reconciles the dirty-key markers a committed write leaves in
//! `cayenne_pending_write_back` to the federated source, in strict order:
//!
//! 1. **List** a batch of the oldest markers (`list_dirty_keys`) — a plain
//!    read, NOT an atomic reservation; concurrency safety comes from the
//!    compare-and-clear in step 4, not from claiming these markers here.
//! 2. **Read** those keys' *current* committed values from the accelerator
//!    (a fenced point scan), AFTER the list.
//! 3. **Deliver** to the source idempotently: delete those keys, then
//!    insert the rows that are still present (delete-all-then-insert-present is
//!    the same regardless of how many times it runs, so a re-delivery after a
//!    crash converges).
//! 4. **Compare-and-clear** the markers whose stored sequence is still at or
//!    below the sequence listed in step 1 — a newer commit that bumped a marker
//!    during delivery leaves it in place, so the stale delivery never clears a
//!    fresh mark.
//!
//! Delivery failure never blocks accelerator commits; the dirty set simply
//! grows until the next successful pass. Marking happens only in the
//! commit-publish transaction (never in the CDC apply path), so an echo of our
//! own write cannot spawn a fresh delivery.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef};
use cayenne::CayenneTableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use tokio::task::JoinHandle;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

use super::write::write_back::execute_insert;
use crate::federated::FederatedTable;

/// Markers claimed per delivery pass.
const CLAIM_BATCH: usize = 1024;
/// Idle poll interval when the dirty set is empty (not a failure — the error
/// backoff must not grow on empty polls).
const POLL_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct WriteBackWorker {
    /// A write-clone of the durable-write-back Cayenne provider — shares the
    /// live table's catalog, listing fence, and keyset, so the marker CRUD and
    /// the point scan observe committed state.
    provider: Arc<CayenneTableProvider>,
    /// The federated source (upsert emulation = delete-by-PK + insert).
    federated: Arc<FederatedTable>,
    /// Primary-key column names, in key order.
    pk_columns: Vec<String>,
    dataset_name: String,
}

impl WriteBackWorker {
    /// Spawn the delivery loop; the returned handle is pushed onto the
    /// accelerated table's `handlers` and aborted when the table drops.
    pub(crate) fn spawn(
        provider: CayenneTableProvider,
        federated: Arc<FederatedTable>,
        dataset_name: String,
    ) -> JoinHandle<()> {
        let pk_columns = provider.pk_column_names();
        let worker = Self {
            provider: Arc::new(provider),
            federated,
            pk_columns,
            dataset_name,
        };
        tokio::spawn(async move { worker.run().await })
    }

    async fn run(&self) {
        // v1 delivers single-column primary keys (the common `id` shape). A
        // composite/absent key can't be turned into a simple `pk IN (...)`
        // filter here; leave those markers for a follow-up rather than deliver
        // incorrectly.
        if self.pk_columns.len() != 1 {
            tracing::warn!(
                dataset = %self.dataset_name,
                pk_columns = self.pk_columns.len(),
                "durable write-back worker: only single-column primary keys are supported in v1; \
                 markers for this dataset will accumulate undelivered"
            );
            return;
        }

        // Infinite Fibonacci backoff on delivery ERRORS (delivery must never
        // permanently give up). Rebuilt after every successful pass so a
        // transient failure never leaves us stuck at a long delay, and never
        // advanced by an empty poll (an empty dirty set is not a failure).
        let mut backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
        loop {
            match self.deliver_batch().await {
                Ok(delivered) => {
                    // Success — reset the error backoff.
                    backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
                    if delivered < CLAIM_BATCH {
                        // Dirty set drained (fewer than a full batch remained);
                        // idle-poll for the next commit — NOT an error, so the
                        // backoff stays reset.
                        tokio::time::sleep(POLL_INTERVAL).await;
                    }
                    // Else a full batch was claimed — more may remain; loop
                    // immediately to keep draining.
                }
                Err(e) => {
                    let delay = backoff.next_duration().unwrap_or(POLL_INTERVAL);
                    tracing::warn!(
                        dataset = %self.dataset_name,
                        error = %e,
                        "durable write-back delivery failed; retrying in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// One claim → read → deliver → clear pass. Returns the number of markers
    /// delivered (0 when the dirty set is empty).
    async fn deliver_batch(&self) -> DataFusionResult<usize> {
        let claimed = self
            .provider
            .list_dirty_keys(CLAIM_BATCH)
            .await
            .map_err(to_df_err)?;
        if claimed.is_empty() {
            return Ok(0);
        }

        let pk_bytes: Vec<Vec<u8>> = claimed.iter().map(|(bytes, _)| bytes.clone()).collect();
        let pk_arrays = self.provider.decode_pk_keys(&pk_bytes).map_err(to_df_err)?;
        let Some(pk_values) = pk_arrays.into_iter().next() else {
            return Ok(0);
        };
        let filter = pk_in_filter(&self.pk_columns[0], &pk_values)?;

        // Read the claimed keys' current committed values from the accelerator,
        // AFTER the claim (a newer commit bumps the marker above the claimed
        // sequence, so the clear below no-ops for it). Build the context from the
        // provider's SHARED RuntimeEnv (object-store registrations for S3, memory
        // pool, caches) — a fresh `SessionContext::new()` would lose them and fail
        // object-store-backed scans.
        let ctx =
            SessionContext::new_with_config_rt(SessionConfig::new(), self.provider.runtime_env());
        // `Arc<CayenneTableProvider>` coerces to the `Arc<dyn TableProvider>`
        // `read_table` expects at the call argument below.
        let accelerator = Arc::clone(&self.provider);
        let current = ctx
            .read_table(accelerator)?
            .filter(filter.clone())?
            .collect()
            .await?;
        let session_state = ctx.state();

        // Idempotent upsert emulation: delete the claimed keys from the source,
        // then insert the rows still present in the accelerator. Absent keys stay
        // deleted; present keys are re-inserted with their current value.
        let federated_provider = self.federated.table_provider().await;
        let delete_plan = federated_provider
            .delete_from(&session_state, vec![filter])
            .await?;
        let _ = datafusion::physical_plan::collect(delete_plan, session_state.task_ctx()).await?;

        if current.iter().any(|batch| batch.num_rows() > 0) {
            execute_insert(
                Arc::clone(&federated_provider),
                self.provider.table_schema(),
                current,
                InsertOp::Append,
                &session_state,
                None,
            )
            .await?;
        }

        // Ack: clear only markers still at/below the claimed sequence.
        self.provider
            .clear_dirty_keys(&claimed)
            .await
            .map_err(to_df_err)?;
        Ok(claimed.len())
    }
}

/// Build `pk_col IN (values…)` from a decoded primary-key array.
fn pk_in_filter(pk_col: &str, values: &ArrayRef) -> DataFusionResult<Expr> {
    let mut list: Vec<Expr> = Vec::with_capacity(values.len());
    for index in 0..values.len() {
        list.push(lit(ScalarValue::try_from_array(values.as_ref(), index)?));
    }
    Ok(col(pk_col).in_list(list, false))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "passed to `Result::map_err`, which moves the error value in"
)]
fn to_df_err(e: cayenne::provider::Error) -> DataFusionError {
    DataFusionError::Execution(format!("durable write-back: {e}"))
}
