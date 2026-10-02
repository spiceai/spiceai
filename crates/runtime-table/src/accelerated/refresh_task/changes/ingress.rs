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

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};

use cache::Caching;
use data_components::cdc::{self, ChangesStream};
use datafusion::error::{DataFusionError, Result};
use futures::StreamExt;
use runtime_acceleration::change_sink::batching::{CdcIngress, CoalescingLimits};
use runtime_acceleration::change_sink::{
    ChangeBatch, DurabilityObserver, Recovery, StorageDurability, Submission, WriteOptions,
    WriteReceipt,
};
use runtime_metrics::acceleration as metrics;
use runtime_status as status;
use tokio::sync::RwLock;

use super::{
    ApplyContext, CdcConfig, RefreshTask, SourceDurabilityObserver, StreamErrorType,
    flush_pending_source_commits, fold_committers, format_datafusion_error, handle_stream_error,
    join_pending_commit,
};
use crate::accelerated::refresh::Refresh;
use crate::accelerated::refresh_completion::RefreshCompletion;

type Committer = Box<dyn cdc::CommitChange + Send + Sync>;

/// Metadata only. Accepted row buffers belong exclusively to the sink owner.
struct PendingSource {
    submission: Submission,
    receipt: Option<WriteReceipt>,
    committer: Committer,
    ready: bool,
    source_timestamp: Option<i64>,
}

impl PendingSource {
    async fn published(&mut self) -> Result<()> {
        if self.receipt.is_none() {
            self.receipt = Some(self.submission.wait().await?);
        }
        let receipt = self
            .receipt
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("Missing CDC write receipt".into()))?;
        receipt.published().await
    }
}

#[derive(Default)]
struct SourceGroup {
    committers: Vec<Committer>,
    ready: bool,
    changed: bool,
    source_timestamp: Option<i64>,
}

async fn await_front(pending: &mut VecDeque<PendingSource>) -> Result<()> {
    match pending.front_mut() {
        Some(front) => front.published().await,
        None => std::future::pending().await,
    }
}

enum Event {
    Source(
        Option<
            std::result::Result<
                Result<cdc::ChangeEnvelope, cdc::StreamError>,
                Box<dyn std::any::Any + Send>,
            >,
        >,
    ),
    Admission,
    Published(Result<()>),
}

impl RefreshTask {
    pub(super) async fn consume_changes(
        &self,
        config: CdcConfig,
        refresh: Arc<RwLock<Refresh>>,
        changes_stream: ChangesStream,
        caching: Option<Weak<Caching>>,
        refresh_completion: Option<RefreshCompletion>,
        initial_load_completed: Arc<AtomicBool>,
    ) -> crate::accelerated::Result<()> {
        let dataset_name = self.dataset_name.clone();
        let labels = self.dataset_metric_labels.clone();
        let sql = refresh.read().await.display_sql();
        self.set_refresh_status(sql.as_deref(), status::ComponentStatus::Refreshing)
            .await;
        let sink = self.change_sink().await;
        let ingress = Arc::new(CdcIngress::new(
            &dataset_name,
            Arc::new(self.cdc_policy()),
            CoalescingLimits {
                max_inputs: config.max_coalesced_envelopes,
                max_bytes: config.max_coalesced_bytes,
                max_age: Duration::from_millis(config.max_coalesce_age_ms),
            },
        ));
        let observer = sink.capabilities().deferred_durability.then(|| {
            let observer = Arc::new(SourceDurabilityObserver::new(
                dataset_name.clone(),
                Arc::clone(&self.runtime_status),
            ));
            sink.set_durability_observer(Arc::clone(&observer) as Arc<dyn DurabilityObserver>);
            observer
        });
        #[cfg(test)]
        let write_context = util::session_state::session_context();
        #[cfg(test)]
        let state = write_context.state();
        let mut pending_commit = None;
        #[cfg(test)]
        let mut pending_finalize = None;
        let mut context = ApplyContext {
            refresh_sql: sql.as_deref(),
            dataset_name: &dataset_name,
            refresh: &refresh,
            metric_labels: &labels,
            caching: caching.as_ref(),
            refresh_completion: refresh_completion.as_ref(),
            initial_load_completed: &initial_load_completed,
            #[cfg(test)]
            write_ctx: &write_context,
            #[cfg(test)]
            write_session_state: &state,
            commit_timeout: config.commit_timeout,
            #[cfg(test)]
            pending_finalize: &mut pending_finalize,
            pending_commit: &mut pending_commit,
            deferred_commits: observer.as_ref(),
        };
        let mut source = AssertUnwindSafe(changes_stream).catch_unwind();
        let mut pending = VecDeque::new();
        let mut group = SourceGroup::default();
        let mut held: Option<cdc::ChangeEnvelope> = None;
        let mut source_ended = false;
        let mut received_timestamp = None;
        // Bound source metadata as well as storage admission. No source data
        // queue is created, and at most one unaccepted envelope is retained.
        let metadata_limit = config
            .prefetch_buffer
            .max(1)
            .saturating_add(config.max_coalesced_envelopes.max(1));

        loop {
            if source_ended && held.is_none() && pending.is_empty() {
                break;
            }
            let retained = pending
                .len()
                .saturating_add(group.committers.len())
                .saturating_add(
                    observer
                        .as_ref()
                        .map_or(0, |observer| observer.pending_count()),
                );
            if retained >= metadata_limit
                && pending.is_empty()
                && let Some(observer) = &observer
            {
                // Published-but-not-durable committers are source metadata too.
                // A durability barrier releases them before more rows are admitted.
                if let Some(message) = flush_pending_source_commits(
                    sink,
                    observer,
                    &dataset_name,
                    &self.runtime_status,
                )
                .await
                {
                    self.set_refresh_status(
                        sql.as_deref(),
                        status::ComponentStatus::error_with_message(message),
                    )
                    .await;
                    break;
                }
                continue;
            }
            let mut admission = None;
            let event = if let Some(envelope) = held.as_ref() {
                if envelope.history_unavailable() {
                    if pending.is_empty() {
                        let Some(envelope) = held.take() else {
                            continue;
                        };
                        if !self.consume_rebuild(&mut context, envelope, &ingress).await {
                            break;
                        }
                        continue;
                    }
                    Event::Published(await_front(&mut pending).await)
                } else {
                    // reserve does not own `held`. Losing this select race to a
                    // completion cannot drop source rows or their committer.
                    tokio::select! {
                        biased;
                        result = await_front(&mut pending) => Event::Published(result),
                        permit = sink.reserve() => {
                            admission = Some(permit);
                            Event::Admission
                        }
                    }
                }
            } else if source_ended || retained >= metadata_limit {
                Event::Published(await_front(&mut pending).await)
            } else {
                let receive_start = Instant::now();
                tokio::select! {
                    biased;
                    result = await_front(&mut pending) => Event::Published(result),
                    item = source.next() => {
                        ingress.record_receive_wait(receive_start);
                        Event::Source(item)
                    },
                }
            };
            let result: Result<bool> = match event {
                Event::Admission => {
                    let permit = match admission {
                        Some(Ok(permit)) => permit,
                        Some(Err(error)) => {
                            self.source_error(&context, &error).await;
                            break;
                        }
                        None => {
                            self.source_error(
                                &context,
                                &DataFusionError::Internal("Missing CDC admission permit".into()),
                            )
                            .await;
                            break;
                        }
                    };
                    let Some(envelope) = held.take() else {
                        continue;
                    };
                    let timestamp = (!envelope.is_heartbeat())
                        .then(|| envelope.source_commit_ts_ms())
                        .flatten();
                    let (committer, rows, ready, _) = envelope.into_lazy_parts();
                    let options = WriteOptions {
                        recovery: if committer.supports_deferral() {
                            Recovery::Replayable
                        } else {
                            Recovery::Durable
                        },
                        delete_batch_size: config.delete_subbatch_max.max(1),
                    };
                    match permit.submit(ChangeBatch::cdc_rows(rows, Arc::clone(&ingress)), options)
                    {
                        Ok(submission) => {
                            pending.push_back(PendingSource {
                                submission,
                                receipt: None,
                                committer,
                                ready,
                                source_timestamp: timestamp,
                            });
                            Ok(true)
                        }
                        Err(error) => Err(error),
                    }
                }
                Event::Published(Ok(())) => {
                    self.consume_published(&mut context, &mut pending, &mut group)
                        .await
                }
                Event::Published(Err(error)) => Err(error),
                Event::Source(None) => {
                    source_ended = true;
                    Ok(true)
                }
                Event::Source(Some(Err(_))) => Err(DataFusionError::Execution(
                    "CDC source stream panicked".into(),
                )),
                Event::Source(Some(Ok(Err(error)))) => {
                    self.consume_source_error(&mut context, &mut pending, &mut group, &error)
                        .await
                }
                Event::Source(Some(Ok(Ok(envelope)))) => {
                    if !envelope.is_heartbeat()
                        && let Some(timestamp) = envelope.source_commit_ts_ms()
                    {
                        received_timestamp = Some(
                            received_timestamp
                                .map_or(timestamp, |previous: i64| previous.max(timestamp)),
                        );
                        metrics::CDC_RECEIVED_COMMIT_UNIX_TIME_MS
                            .record(received_timestamp.unwrap_or(timestamp), labels.dataset());
                        if let Some(now) = util::time::system_time_to_unix_ms(SystemTime::now()) {
                            #[expect(
                                clippy::cast_precision_loss,
                                reason = "arrival lag is a millisecond histogram"
                            )]
                            let lag = now.saturating_sub(timestamp).max(0) as f64;
                            metrics::CDC_SOURCE_ARRIVAL_LAG_MS.record(lag, labels.dataset());
                        }
                    }
                    if envelope.is_no_op_heartbeat() && !envelope.history_unavailable() {
                        if envelope.is_dataset_ready() {
                            if let Some(last) = pending.back_mut() {
                                last.ready = true;
                            } else {
                                self.signal_dataset_ready(&context).await;
                            }
                        }
                    } else {
                        held = Some(envelope);
                    }
                    Ok(true)
                }
            };
            match result {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) => {
                    self.source_error(&context, &error).await;
                    break;
                }
            }
        }
        // Dropped metadata is never acknowledged. Accepted storage work still
        // belongs to the sink and does not depend on these observers.
        drop(pending);
        drop(held);
        if let Some(observer) = &observer
            && let Some(message) =
                flush_pending_source_commits(sink, observer, &dataset_name, &self.runtime_status)
                    .await
        {
            self.set_refresh_status(
                sql.as_deref(),
                status::ComponentStatus::error_with_message(message),
            )
            .await;
        }
        if let Some(commit) = context.pending_commit.take()
            && let Some(message) = join_pending_commit(
                commit,
                &dataset_name,
                self.runtime_status.is_shutdown(),
                config.commit_timeout,
            )
            .await
        {
            self.set_refresh_status(
                sql.as_deref(),
                status::ComponentStatus::error_with_message(message),
            )
            .await;
        }
        if source_ended && !self.runtime_status.is_shutdown() {
            tracing::warn!("Changes stream ended for dataset {dataset_name}");
        }
        Ok(())
    }

    async fn source_error(&self, context: &ApplyContext<'_>, error: &DataFusionError) {
        self.set_refresh_status(
            context.refresh_sql,
            status::ComponentStatus::error_with_message(format_datafusion_error(error)),
        )
        .await;
        if !self.runtime_status.is_shutdown() {
            tracing::error!("Error writing change for {}: {error}", context.dataset_name);
        }
    }

    async fn consume_source_error(
        &self,
        context: &mut ApplyContext<'_>,
        pending: &mut VecDeque<PendingSource>,
        group: &mut SourceGroup,
        error: &cdc::StreamError,
    ) -> Result<bool> {
        // Preserve the error's source ordering without admitting control
        // metadata to the storage queue.
        while !pending.is_empty() {
            await_front(pending).await?;
            if !self.consume_published(context, pending, group).await? {
                return Ok(false);
            }
        }
        if handle_stream_error(error, context.dataset_name) != StreamErrorType::Transient {
            self.set_refresh_status(
                context.refresh_sql,
                status::ComponentStatus::error_with_message(format_datafusion_error(error)),
            )
            .await;
        }
        Ok(true)
    }

    async fn consume_published(
        &self,
        context: &mut ApplyContext<'_>,
        pending: &mut VecDeque<PendingSource>,
        group: &mut SourceGroup,
    ) -> Result<bool> {
        let completed = pending
            .pop_front()
            .ok_or_else(|| DataFusionError::Internal("Missing CDC source completion".into()))?;
        let receipt = completed
            .receipt
            .ok_or_else(|| DataFusionError::Internal("Missing CDC source receipt".into()))?;
        group.committers.push(completed.committer);
        group.ready |= completed.ready;
        group.changed |= receipt.changed;
        group.source_timestamp = group.source_timestamp.max(completed.source_timestamp);
        if !receipt.batch_end {
            if pending.is_empty() {
                return Err(DataFusionError::Internal(
                    "CDC coalesced receipt is missing its final source observer".into(),
                ));
            }
            return Ok(true);
        }
        let group = std::mem::take(group);
        if group.changed {
            self.update_last_updated_at();
        }
        if let Some(callback) = &self.on_stream_batch_process_callback {
            let mut callback = callback.lock().await;
            callback().await;
        }
        if group.ready {
            self.signal_dataset_ready(context).await;
        }
        if group.changed
            && let Some(cache) = context.caching.and_then(Weak::upgrade)
            && let Err(error) = cache
                .invalidate_for_table(context.dataset_name.clone())
                .await
            && !self.runtime_status.is_shutdown()
        {
            tracing::error!(
                "Failed to invalidate cached results for dataset {}: {error}",
                context.dataset_name
            );
        }
        let acknowledged = self
            .acknowledge_published(
                context,
                fold_committers(group.committers),
                receipt.durability,
            )
            .await;
        if acknowledged && let Some(timestamp) = group.source_timestamp {
            metrics::CDC_APPLIED_COMMIT_UNIX_TIME_MS
                .record(timestamp, context.metric_labels.dataset());
            if let Some(now) = util::time::system_time_to_unix_ms(SystemTime::now()) {
                metrics::CDC_REPLICATION_LAG_MS.record(
                    now.saturating_sub(timestamp).max(0),
                    context.metric_labels.dataset(),
                );
            }
        }
        Ok(acknowledged)
    }

    async fn consume_rebuild(
        &self,
        context: &mut ApplyContext<'_>,
        envelope: cdc::ChangeEnvelope,
        ingress: &Arc<CdcIngress>,
    ) -> bool {
        if !self.prepare_rebuild(context).await {
            return false;
        }
        let no_op = envelope.is_no_op_heartbeat();
        let (committer, batch, ready, _) = match envelope.into_parts_offloaded().await {
            Ok(parts) => parts,
            Err(error) => {
                self.source_error(context, &DataFusionError::External(Box::new(error)))
                    .await;
                return false;
            }
        };
        if batch.rebuild_from_this_batch() {
            let data = batch.data_batch();
            let schema = data.schema();
            let rows = if data.num_rows() == 0 {
                Vec::new()
            } else {
                vec![data]
            };
            if !self.rebuild_from_batches(context, rows, schema).await {
                return false;
            }
            return self
                .run_finalize_side_effects(
                    context,
                    vec![committer],
                    ready,
                    StorageDurability::NotPromised,
                )
                .await;
        }
        if !self.rebuild_from_source(context).await {
            return false;
        }
        if no_op {
            if ready {
                self.signal_dataset_ready(context).await;
            }
            return true;
        }
        let options = WriteOptions {
            recovery: if committer.supports_deferral() {
                Recovery::Replayable
            } else {
                Recovery::Durable
            },
            delete_batch_size: self.cdc_delete_subbatch_max(),
        };
        let sink = self.change_sink().await;
        let permit = match sink.reserve().await {
            Ok(permit) => permit,
            Err(error) => {
                self.source_error(context, &error).await;
                return false;
            }
        };
        let submission = match permit.submit(
            ChangeBatch::cdc_rows(cdc::LazyChangeBatch::ready(batch), Arc::clone(ingress)),
            options,
        ) {
            Ok(submission) => submission,
            Err(error) => {
                self.source_error(context, &error).await;
                return false;
            }
        };
        let mut pending = VecDeque::from([PendingSource {
            submission,
            receipt: None,
            committer,
            ready,
            source_timestamp: None,
        }]);
        if let Err(error) = await_front(&mut pending).await {
            self.source_error(context, &error).await;
            return false;
        }
        match self
            .consume_published(context, &mut pending, &mut SourceGroup::default())
            .await
        {
            Ok(applied) => applied,
            Err(error) => {
                self.source_error(context, &error).await;
                false
            }
        }
    }
}
