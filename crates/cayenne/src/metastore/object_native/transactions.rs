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

//! Transactions across dataset logs.

use super::{
    Domain, invalid,
    registry::{DROPPED, ObjectNativeCatalog, REGISTRY, created_at, delete_under},
    replay_state,
    slot::HeldReservation,
};
use crate::catalog::CatalogResult;
use cayenne_metastore_actions::proto;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload, path::Path};
use prost::Message;
use std::{collections::HashSet, sync::Arc};

#[cfg(test)]
mod replay_tests;

/// A transaction's records under `transactions/<id>/`: the intent, created
/// before the first `Prepare`, and the decision, created if absent. Both are
/// [`proto::Intent`] and [`proto::Decision`] messages.
const INTENT: &str = "intent";
const DECISION: &str = "decision";

fn encode_decision(commit: bool) -> Vec<u8> {
    proto::Decision {
        outcome: if commit {
            proto::Outcome::Commit
        } else {
            proto::Outcome::Abort
        } as i32,
    }
    .encode_to_vec()
}

fn decode_decision(txn: &str, bytes: &[u8]) -> CatalogResult<bool> {
    let decision = proto::Decision::decode(bytes).map_err(invalid)?;
    match proto::Outcome::try_from(decision.outcome) {
        Ok(proto::Outcome::Commit) => Ok(true),
        Ok(proto::Outcome::Abort) => Ok(false),
        _ => Err(invalid(format!(
            "transaction {txn} has an unreadable decision (outcome {})",
            decision.outcome
        ))),
    }
}

/// The bytes of `transactions/<txn>/<name>`, or `None` when it does not exist.
async fn read_record(
    store: &Arc<dyn ObjectStore>,
    transactions: &Path,
    txn: &str,
    name: &str,
) -> CatalogResult<Option<bytes::Bytes>> {
    match store.get(&transactions.clone().join(txn).join(name)).await {
        Ok(object) => Ok(Some(object.bytes().await.map_err(invalid)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(invalid(error)),
    }
}

async fn read_intent(
    store: &Arc<dyn ObjectStore>,
    transactions: &Path,
    txn: &str,
) -> CatalogResult<Option<proto::Intent>> {
    read_record(store, transactions, txn, INTENT)
        .await?
        .map(|bytes| proto::Intent::decode(bytes).map_err(invalid))
        .transpose()
}

/// The recorded outcome of `txn`: `Some(true)` to commit.
async fn read_decision(
    store: &Arc<dyn ObjectStore>,
    transactions: &Path,
    txn: &str,
) -> CatalogResult<Option<bool>> {
    read_record(store, transactions, txn, DECISION)
        .await?
        .map(|bytes| decode_decision(txn, &bytes))
        .transpose()
}

/// Record `commit` as the outcome of `txn` under `transactions` unless one is
/// recorded, and return the recorded outcome. Exactly one outcome is recorded
/// while the transaction's records exist.
async fn decide(
    store: &Arc<dyn ObjectStore>,
    transactions: &Path,
    txn: &str,
    commit: bool,
) -> CatalogResult<bool> {
    const ATTEMPTS: u32 = 8;
    let path = transactions.clone().join(txn).join(DECISION);
    let body = encode_decision(commit);
    let mut last = None;
    for attempt in 0..ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(50 << attempt.min(6))).await;
        }
        match store
            .put_opts(
                &path,
                PutPayload::from(body.clone()),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => return Ok(commit),
            Err(object_store::Error::AlreadyExists { .. }) => {}
            // Unknown whether it landed: read it back, or try again.
            Err(error) => {
                last = Some(error.to_string());
                continue;
            }
        }
        match read_decision(store, transactions, txn).await {
            Ok(Some(recorded)) => return Ok(recorded),
            // Removed between the create and the read: try again.
            Ok(None) => last = Some("the decision was removed while it was read".to_string()),
            Err(error) => last = Some(error.to_string()),
        }
    }
    Err(invalid(format!(
        "could not record the decision of transaction {txn}: {}",
        last.unwrap_or_default()
    )))
}

/// Whether `txn`'s intent exists.
async fn intent_exists(
    store: &Arc<dyn ObjectStore>,
    transactions: &Path,
    txn: &str,
) -> CatalogResult<bool> {
    match store
        .head(&transactions.clone().join(txn).join(INTENT))
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(invalid(error)),
    }
}

/// The outcome of `txn` its coordinator may report: the decision object, read
/// while the intent exists. The intent is written once, before any decision,
/// and removed before the decision, so a decision read before the intent is
/// seen is the transaction's only one. Once the intent is gone, a decision may
/// be one created after the records were removed, so this returns `None`: the
/// transaction was finished by another process.
async fn confirmed_outcome(
    store: &Arc<dyn ObjectStore>,
    transactions: &Path,
    txn: &str,
) -> CatalogResult<Option<bool>> {
    let Some(decision) = read_decision(store, transactions, txn).await? else {
        return Ok(None);
    };
    Ok(intent_exists(store, transactions, txn)
        .await?
        .then_some(decision))
}

/// One attempt to record `commit` as the outcome of `txn` create-if-absent,
/// returning the outcome its coordinator may report ([`confirmed_outcome`]).
/// An error leaves it unknown whether the create landed.
async fn try_decide(
    store: &Arc<dyn ObjectStore>,
    transactions: &Path,
    txn: &str,
    commit: bool,
) -> CatalogResult<Option<bool>> {
    match store
        .put_opts(
            &transactions.clone().join(txn).join(DECISION),
            PutPayload::from(encode_decision(commit)),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) => Ok(intent_exists(store, transactions, txn)
            .await?
            .then_some(commit)),
        Err(object_store::Error::AlreadyExists { .. }) => {
            confirmed_outcome(store, transactions, txn).await
        }
        Err(error) => Err(invalid(error)),
    }
}

/// What a coordinator learned of its transaction's decision.
enum Decided {
    /// The transaction's decision.
    Recorded(bool),
    /// The records are gone: another process took the datasets over and
    /// finished the transaction.
    Finished,
    /// Storage did not confirm a decision before the deadline.
    Unrecorded(String),
}

/// Record `commit` as the outcome of `txn`, retrying until `deadline`; a commit
/// not recorded by then is aborted instead, so the outcome is settled in
/// bounded time whenever storage answers. An attempt still waiting at the
/// deadline is abandoned: if its create lands later, it is refused by the
/// decision already recorded, or it is the decision the abort reads back.
async fn decide_by(
    store: &Arc<dyn ObjectStore>,
    transactions: &Path,
    txn: &str,
    commit: bool,
    deadline: tokio::time::Instant,
) -> Decided {
    // Attempts to record the abort once the deadline has passed, and how long
    // each may wait for storage.
    const ABORT_ATTEMPTS: u32 = 3;
    const ABORT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    let mut wanted = commit;
    let mut aborts = 0;
    let mut attempt: u32 = 0;
    loop {
        let now = tokio::time::Instant::now();
        let limit = if now < deadline {
            deadline - now
        } else {
            ABORT_ATTEMPT_TIMEOUT
        };
        let attempted = tokio::time::timeout(limit, try_decide(store, transactions, txn, wanted))
            .await
            .unwrap_or_else(|_| Err(invalid("storage did not answer in time")));
        match attempted {
            Ok(Some(outcome)) => return Decided::Recorded(outcome),
            Ok(None) => return Decided::Finished,
            Err(error) => {
                if tokio::time::Instant::now() >= deadline {
                    if wanted {
                        wanted = false;
                    } else {
                        aborts += 1;
                        if aborts >= ABORT_ATTEMPTS {
                            return Decided::Unrecorded(error.to_string());
                        }
                    }
                }
            }
        }
        attempt += 1;
        let backoff = std::time::Duration::from_millis(50 << attempt.min(6));
        let now = tokio::time::Instant::now();
        let wait = if now < deadline {
            backoff.min(deadline - now)
        } else {
            backoff
        };
        tokio::time::sleep(wait).await;
    }
}

/// Resolve every prepared participant of `txn` on its `commit` outcome. The
/// caller holds each participant's reservation.
async fn resolve_participants(
    txn: &str,
    participants: &[(HeldReservation, Option<Vec<String>>)],
    commit: bool,
) -> Vec<CatalogResult<()>> {
    futures::future::join_all(participants.iter().filter_map(|(held, footprint)| {
        footprint
            .clone()
            .map(|footprint| held.0.resolve(txn, footprint, commit))
    }))
    .await
}

/// Finish `txn` once storage answers, for a coordinator that returned before
/// its outcome was recorded. Its deadline has passed, so it records an abort
/// unless a decision is recorded, resolves every participant it prepared, and
/// removes the records; it keeps the participants' reservations until then, so
/// other changes to them wait rather than being refused.
async fn finish_in_background(
    store: Arc<dyn ObjectStore>,
    transactions: Path,
    txn: String,
    participants: Vec<(HeldReservation, Option<Vec<String>>)>,
) {
    let mut attempt: u32 = 0;
    let commit = loop {
        match try_decide(&store, &transactions, &txn, false).await {
            Ok(Some(commit)) => break commit,
            Ok(None) => return,
            Err(error) => {
                attempt += 1;
                tracing::debug!(
                    "Cayenne object-native metastore could not yet record the outcome of transaction {txn}, retrying: {error}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100 << attempt.min(7))).await;
            }
        }
    };
    let resolved = resolve_participants(&txn, &participants, commit).await;
    drop(participants);
    if let Some(error) = resolved.into_iter().find_map(Result::err) {
        tracing::error!(
            "Cayenne object-native metastore recorded the outcome of transaction {txn} across datasets ({}) but could not apply it, so its datasets stop updating until spiced restarts, which applies it: {error}",
            if commit { "committed" } else { "aborted" }
        );
        return;
    }
    tracing::info!(
        "Cayenne object-native metastore recorded the outcome of transaction {txn} across datasets after its coordinator returned: {}",
        if commit { "committed" } else { "aborted" }
    );
    if let Err(error) = forget_records(&store, &transactions, &txn).await {
        tracing::debug!(
            "Cayenne object-native metastore left the records of resolved transaction {txn} for the next open to remove: {error}"
        );
    }
}

/// Remove `txn`'s records: the intent first, so a decision left by a failure
/// between the two is one with no intent, which [`remove_if_finished`] removes.
async fn forget_records(
    store: &Arc<dyn ObjectStore>,
    transactions: &Path,
    txn: &str,
) -> CatalogResult<()> {
    for name in [INTENT, DECISION] {
        match store
            .delete(&transactions.clone().join(txn).join(name))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(invalid(error)),
        }
    }
    Ok(())
}

/// A participant's log as storage holds it: its head lease, and whether it
/// holds `txn` prepared. A log that does not exist can take no `Prepare`.
async fn participant_state(
    store: &Arc<dyn ObjectStore>,
    log: Path,
    txn: &str,
) -> CatalogResult<(u64, bool)> {
    match store.head(&log.clone().join("head")).await {
        Ok(_) => {}
        Err(object_store::Error::NotFound { .. }) => return Ok((u64::MAX, false)),
        Err(error) => return Err(invalid(error)),
    }
    let (reader, opened) = object_store_occ::headlog::Reader::open(Arc::clone(store), log)
        .await
        .map_err(invalid)?;
    let lease = reader.state().head.lease;
    Ok((
        lease,
        replay_state(opened)?.state.prepared.contains_key(txn),
    ))
}

/// Whether every participant of `intent` is past the coordinator's lease, from
/// storage alone, and the participants still holding `txn` prepared.
async fn participants(
    store: &Arc<dyn ObjectStore>,
    intent: &proto::Intent,
    log: &impl Fn(&str) -> Path,
    txn: &str,
) -> CatalogResult<(bool, Vec<String>)> {
    let mut fenced = true;
    let mut prepared = Vec::new();
    for participant in &intent.participants {
        let (lease, holds) = participant_state(store, log(&participant.folder), txn).await?;
        fenced &= lease > participant.lease;
        if holds {
            prepared.push(participant.folder.clone());
        }
    }
    Ok((fenced, prepared))
}

/// Remove `txn`'s records when nothing can still need them, read from storage
/// alone: a decision is recorded, every participant's head lease is past the
/// lease the coordinator prepares under (so no `Prepare` of the coordinator's
/// can land), and no participant holds the transaction prepared. A decision with
/// no intent, created after the records were removed, goes too: no participant
/// holds the transaction prepared, so nothing applies it. `log` maps a
/// participant's folder to its log's prefix. Returns whether anything was
/// removed.
async fn remove_if_finished(
    store: &Arc<dyn ObjectStore>,
    transactions: &Path,
    log: &impl Fn(&str) -> Path,
    txn: &str,
) -> CatalogResult<bool> {
    let Some(intent) = read_intent(store, transactions, txn).await? else {
        let stray = read_record(store, transactions, txn, DECISION)
            .await?
            .is_some();
        if stray {
            forget_records(store, transactions, txn).await?;
        }
        return Ok(stray);
    };
    if read_decision(store, transactions, txn).await?.is_none() {
        return Ok(false);
    }
    let (fenced, prepared) = participants(store, &intent, log, txn).await?;
    if !fenced || !prepared.is_empty() {
        return Ok(false);
    }
    forget_records(store, transactions, txn).await?;
    Ok(true)
}

impl ObjectNativeCatalog {
    /// Commit publications on several datasets' logs as one transaction:
    /// write the intent, `Prepare` on every participant, decide by
    /// create-if-absent of the decision, then `Resolve` on every participant.
    /// Participants are reserved in folder order, so two transactions never
    /// wait on each other.
    pub(super) async fn commit_across(
        &self,
        mut groups: Vec<(Arc<Domain>, Vec<crate::catalog::CatalogPublication<'_>>)>,
    ) -> CatalogResult<()> {
        groups.sort_by(|(a, _), (b, _)| a.folder.cmp(&b.folder));
        let mut reservations = Vec::with_capacity(groups.len());
        for (domain, _) in &groups {
            reservations.push(HeldReservation::take(domain).await);
        }
        let txn = uuid::Uuid::now_v7().to_string();
        let deadline = tokio::time::Instant::now() + self.config.transaction_deadline;
        let intent = proto::Intent {
            participants: groups
                .iter()
                .map(|(domain, _)| proto::Participant {
                    folder: domain.folder.clone(),
                    lease: domain.opened.lease,
                })
                .collect(),
            deadline_unix_millis: (chrono::Utc::now()
                + chrono::Duration::from_std(self.config.transaction_deadline).map_err(invalid)?)
            .timestamp_millis(),
        };
        self.store
            .put_opts(
                &self.transactions.clone().join(txn.as_str()).join(INTENT),
                PutPayload::from(intent.encode_to_vec()),
                PutMode::Create.into(),
            )
            .await
            .map_err(invalid)?;
        let prepared = futures::future::join_all(
            groups
                .iter()
                .map(|(domain, publications)| domain.prepare(&txn, publications)),
        )
        .await;
        let all_prepared = prepared.iter().all(Result::is_ok);
        let mut prepare_error = None;
        let participants: Vec<(HeldReservation, Option<Vec<String>>)> = reservations
            .into_iter()
            .zip(prepared)
            .map(|(held, footprint)| match footprint {
                Ok(footprint) => (held, Some(footprint)),
                Err(error) => {
                    prepare_error.get_or_insert(error);
                    (held, None)
                }
            })
            .collect();
        let commit = match decide_by(
            &self.store,
            &self.transactions,
            &txn,
            all_prepared,
            deadline,
        )
        .await
        {
            Decided::Recorded(commit) => commit,
            Decided::Finished => {
                return Err(invalid(format!(
                    "the outcome of transaction {txn} across datasets is unknown: another process took its datasets over and finished it; check whether its changes are visible before retrying"
                )));
            }
            Decided::Unrecorded(cause) => {
                tokio::spawn(finish_in_background(
                    Arc::clone(&self.store),
                    self.transactions.clone(),
                    txn.clone(),
                    participants,
                ));
                // Only the coordinator records a commit, so one it never
                // tried to record cannot be the outcome.
                if !all_prepared {
                    return Err(prepare_error.unwrap_or_else(|| {
                        invalid(format!("transaction {txn} across datasets was aborted"))
                    }));
                }
                tracing::warn!(
                    "Cayenne object-native metastore could not record the outcome of transaction {txn} across datasets before its deadline, so it reports the outcome as unknown, and changes to its datasets wait until spiced records it: {cause}"
                );
                return Err(invalid(format!(
                    "the outcome of transaction {txn} across datasets is unknown: it could not be recorded before its deadline ({cause}); check whether its changes are visible before retrying"
                )));
            }
        };
        let resolved = resolve_participants(&txn, &participants, commit).await;
        drop(participants);
        let resolve_error = resolved.into_iter().find_map(Result::err);
        // Once every participant has resolved it, nothing reads the transaction's
        // records again. Any other outcome leaves them for recovery.
        if all_prepared && resolve_error.is_none() {
            self.forget(&txn).await;
        }
        if let Some(error) = resolve_error {
            // The decision is durable, so the outcome stands; this process
            // stops changing the dataset until a reopen applies it.
            tracing::error!(
                "Cayenne object-native metastore recorded the outcome of transaction {txn} across datasets ({}) but could not apply it, so its datasets stop updating until spiced restarts, which applies it: {error}",
                if commit { "committed" } else { "aborted" }
            );
        }
        if commit {
            Ok(())
        } else {
            Err(prepare_error.unwrap_or_else(|| {
                invalid(format!(
                    "transaction {txn} across datasets was aborted, so none of its changes were applied"
                ))
            }))
        }
    }

    /// The prefix of the log in folder `folder`: the registry's for the empty
    /// folder, a dataset's otherwise.
    fn log_of(&self, folder: &str) -> Path {
        if folder.is_empty() {
            self.prefix.clone()
        } else {
            self.datasets.clone().join(folder)
        }
    }

    /// Remove a transaction's records once the coordinator has resolved every
    /// participant. One that fails is removed when a catalog next opens.
    async fn forget(&self, txn: &str) {
        if let Err(error) = forget_records(&self.store, &self.transactions, txn).await {
            tracing::debug!(
                "Cayenne object-native metastore left the records of resolved transaction {txn} for the next open to remove: {error}"
            );
        }
    }

    /// Remove the logs no name refers to: a dataset dropped, or a create stopped
    /// before it was named. Only what is older than `reclaim_after` goes, since
    /// a newer one may belong to a process that took the registry over after this
    /// one opened it.
    pub(super) async fn reclaim(&self) -> CatalogResult<()> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(self.config.reclaim_after).map_err(invalid)?;
        let mut removed = 0;
        let named: HashSet<String> = self
            .registry
            .positions_under(REGISTRY)
            .await
            .into_iter()
            .filter_map(|(_, folder)| String::from_utf8(folder).ok())
            .collect();
        let listed = self
            .store
            .list_with_delimiter(Some(&self.datasets))
            .await
            .map_err(invalid)?;
        for prefix in listed.common_prefixes {
            let Some(folder) = prefix.filename() else {
                continue;
            };
            if named.contains(folder) {
                continue;
            }
            let ended = match self.store.head(&prefix.clone().join(DROPPED)).await {
                Ok(marker) => Some(marker.last_modified),
                Err(object_store::Error::NotFound { .. }) => created_at(folder),
                Err(error) => return Err(invalid(error)),
            };
            if ended.is_some_and(|ended| ended < cutoff) {
                delete_under(&self.store, &prefix).await?;
                removed += 1;
            }
        }
        if removed > 0 {
            tracing::debug!(
                "Cayenne object-native metastore removed {removed} dataset logs no dataset uses"
            );
        }
        Ok(())
    }

    /// Record `commit` as the outcome of `txn` unless one is recorded, and
    /// return the recorded outcome.
    async fn decide(&self, txn: &str, commit: bool) -> CatalogResult<bool> {
        decide(&self.store, &self.transactions, txn, commit).await
    }

    /// Finish every transaction a stopped process left behind. Opening the
    /// catalog took every log over, so the coordinator of each transaction under
    /// `transactions/` can prepare and decide nothing more: one with no decision
    /// is aborted, every participant holding it prepared is resolved from its
    /// decision, and its records are removed once storage shows no participant
    /// needs them ([`remove_if_finished`]).
    pub(super) async fn recover(&self) -> CatalogResult<()> {
        let listed = self
            .store
            .list_with_delimiter(Some(&self.transactions))
            .await
            .map_err(invalid)?;
        let mut finished = HashSet::new();
        for prefix in listed.common_prefixes {
            let Some(txn) = prefix.filename() else {
                continue;
            };
            self.finish(txn).await?;
            finished.insert(txn.to_string());
        }
        // Records survive while a log holds their transaction prepared, so a
        // prepared transaction without them means its outcome is lost.
        for domain in self.domains() {
            if let Some((txn, _)) = domain
                .unresolved()
                .await
                .into_iter()
                .find(|(txn, _)| !finished.contains(txn))
            {
                return Err(invalid(format!(
                    "transaction {txn} is prepared on the dataset log '{}' but its records under `transactions/` are gone, so its outcome is unknown",
                    domain.folder
                )));
            }
        }
        Ok(())
    }

    /// Decide, resolve and remove `txn` as [`Self::recover`] describes.
    async fn finish(&self, txn: &str) -> CatalogResult<()> {
        let log = |folder: &str| self.log_of(folder);
        if let Some(intent) = read_intent(&self.store, &self.transactions, txn).await? {
            let (fenced, prepared) = participants(&self.store, &intent, &log, txn).await?;
            let decision = match read_decision(&self.store, &self.transactions, txn).await? {
                Some(commit) => Some(commit),
                None if fenced => Some(self.decide(txn, false).await?),
                None => None,
            };
            if let Some(commit) = decision {
                for domain in self.domains() {
                    if !prepared.contains(&domain.folder) {
                        continue;
                    }
                    for (_, footprint) in domain
                        .unresolved()
                        .await
                        .into_iter()
                        .filter(|(prepared, _)| prepared == txn)
                    {
                        let reservation = domain.inner.reserve().await;
                        domain.resolve(txn, footprint, commit).await?;
                        drop(reservation);
                        tracing::info!(
                            "Cayenne object-native metastore resolved transaction {txn}, left prepared when spiced stopped: {}",
                            if commit { "committed" } else { "aborted" }
                        );
                    }
                }
            }
        }
        if remove_if_finished(&self.store, &self.transactions, &log, txn).await? {
            tracing::debug!(
                "Cayenne object-native metastore removed the records of finished transaction {txn}"
            );
        }
        Ok(())
    }
}
