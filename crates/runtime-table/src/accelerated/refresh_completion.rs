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

//! Level-triggered "a refresh finished" signal for an accelerated table.
//!
//! Callers ask two different questions of a refresh:
//!
//! * *"has the initial load landed?"* — [`RefreshCompletion::any`], satisfied by
//!   any refresh that has finished, including one that finished before the
//!   caller asked.
//! * *"has the refresh I just triggered finished?"* — [`RefreshCompletion::next`],
//!   satisfied only by the completion of a refresh *requested* after the waiter
//!   was taken.
//!
//! The second question is why a refresh carries a [`RefreshRequestId`]. A
//! refresh already in flight when the waiter was taken can finish a moment
//! later, and its completion answers a question nobody asked: it ran against
//! whatever the table looked like before the caller changed it. Counting
//! completions cannot tell the two apart, so each request is numbered when it is
//! issued and its completion is recorded under that number. A `next` waiter
//! remembers the highest number issued when it was taken and resolves only on a
//! completion recorded above it.
//!
//! Numbering at *issue* rather than at *start* is deliberate: a caller installs
//! its change, takes the waiter, then triggers, so every id issued after the
//! waiter belongs to a refresh that will read the change. A refresh issued
//! between the change and the waiter is merely not credited — conservative, and
//! never the other way round.
//!
//! A waiter is taken up front and awaited later, so a completion landing in
//! between resolves the wait instead of being dropped. That gap is why this is
//! not a [`tokio::sync::Notify`]: `notify_waiters` stores no permit, so a
//! completion that lands before the caller *creates* its `Notified` future
//! leaves the caller waiting for a refresh that already happened. The boundary
//! is creation, not polling — tokio guarantees a `Notified` receives
//! `notify_waiters` wakeups as soon as it exists, so subscribing early is what
//! closes the gap and polling early cannot. The paths this type replaced were
//! all late *construction*: the future was built after the refresh had already
//! been triggered, leaving nothing to poll in time.

use tokio::sync::watch;

/// Identifies one refresh request, so the completion it produces can be told
/// apart from the completion of a refresh that was already running.
///
/// Handed out by [`RefreshCompletion::issue`] and travelling with the request
/// until [`RefreshCompletion::record`] reports the refresh it started as done.
pub type RefreshRequestId = u64;

/// What every waiter is decided against. Held in one `watch` value so an issue,
/// a completion and a close are each a single atomic transition that also wakes
/// the waiters already registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompletionState {
    /// Highest [`RefreshRequestId`] handed out so far. Ids start at 1, so 0 is
    /// "nothing requested yet" and doubles as the threshold a waiter for *any*
    /// completion is decided against.
    issued: RefreshRequestId,
    /// Highest [`RefreshRequestId`] whose refresh has been recorded complete.
    completed: RefreshRequestId,
    /// Set once no further refresh can be recorded here, so a waiter taken
    /// afterwards resolves instead of blocking on one that cannot arrive.
    closed: bool,
}

/// Records refresh completions for one accelerated table and hands out waiters
/// for them.
///
/// Cloning shares the underlying signal.
#[derive(Debug, Clone)]
pub struct RefreshCompletion {
    state: watch::Sender<CompletionState>,
}

impl Default for RefreshCompletion {
    fn default() -> Self {
        Self::new()
    }
}

impl RefreshCompletion {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: watch::Sender::new(CompletionState {
                issued: 0,
                completed: 0,
                closed: false,
            }),
        }
    }

    /// Numbers a refresh about to be requested.
    ///
    /// Call this once per request, immediately before handing the request to
    /// whatever runs it, and pass the id back to [`RefreshCompletion::record`]
    /// when that request's refresh finishes. A [`RefreshCompletion::next`]
    /// waiter taken beforehand is decided against this number.
    ///
    /// Ids saturate rather than wrap so they stay ordered, which is what a
    /// waiter compares against. The ceiling is unreachable — 2^64 refreshes of
    /// one table in one process — and reaching it would strand waiters rather
    /// than release them early, so the failure direction is a readiness ack that
    /// is never sent rather than one sent for data that was never loaded.
    pub fn issue(&self) -> RefreshRequestId {
        let mut id = 0;
        self.state.send_modify(|state| {
            state.issued = state.issued.saturating_add(1);
            id = state.issued;
        });
        id
    }

    /// Records the refresh requested as `id` as complete, resolving every waiter
    /// that was taken before `id` was issued.
    pub fn record(&self, id: RefreshRequestId) {
        // `max` rather than assignment: completions arrive in request order
        // today, and a reordering must not walk the threshold backwards and
        // un-answer a waiter that has already been released.
        self.state
            .send_modify(|state| state.completed = state.completed.max(id));
    }

    /// Records a completion for refresh work that no caller requested — the CDC
    /// apply loop, which streams rather than answering triggers.
    ///
    /// Such a completion answers every waiter taken before it and none taken
    /// after, which is all an uncorrelated signal can honestly claim.
    ///
    /// One transition rather than `record(self.issue())`: the intermediate state
    /// of that pair is a request that has been issued and not completed, and a
    /// waiter taken there would skip this completion and wait for the following
    /// apply.
    pub fn record_untriggered(&self) {
        self.state.send_modify(|state| {
            state.issued = state.issued.saturating_add(1);
            state.completed = state.issued;
        });
    }

    /// Records that no refresh will ever run for this table in this process, so
    /// every waiter — including one taken after this call — resolves at once.
    ///
    /// A cluster scheduler holds accelerated tables it never refreshes locally;
    /// without this, a caller waiting on one waits for the life of the process.
    pub fn close(&self) {
        self.state.send_modify(|state| state.closed = true);
    }

    /// Takes a waiter for the first refresh recorded *after* this call.
    ///
    /// This is the question a caller that triggers a refresh is asking. Take the
    /// waiter before triggering — a waiter taken afterwards can miss the very
    /// refresh it triggered.
    #[must_use]
    pub fn next(&self) -> RefreshCompletionWaiter {
        self.waiter(false)
    }

    /// Takes a waiter for the first refresh recorded since the table was built,
    /// already satisfied if one has landed.
    ///
    /// This is the question a caller waiting on the initial load is asking; it
    /// cannot miss the load by asking late.
    #[must_use]
    pub fn any(&self) -> RefreshCompletionWaiter {
        self.waiter(true)
    }

    /// The highest request id recorded complete so far.
    ///
    /// Test-only: it lets a test assert that the refresh it is holding open has
    /// not finished, so a run that fails to reach the interleaving under test
    /// says so instead of passing vacuously.
    #[cfg(test)]
    pub(crate) fn completed_requests(&self) -> RefreshRequestId {
        self.state.borrow().completed
    }

    /// `accept_earlier` selects between the two questions above, expressed as the
    /// request id a completion has to exceed to answer this waiter.
    ///
    /// For *any* completion that is 0, which every issued id exceeds, so a
    /// refresh that finished before this call still answers. For the *next*
    /// completion it is the highest id issued so far, which only a request
    /// issued after this call exceeds.
    fn waiter(&self, accept_earlier: bool) -> RefreshCompletionWaiter {
        // Read the state back through the receiver rather than the sender: a
        // transition landing between subscribing and reading is either seen here
        // or wakes the receiver, never dropped between them.
        let receiver = self.state.subscribe();
        let threshold = if accept_earlier {
            0
        } else {
            receiver.borrow().issued
        };
        RefreshCompletionWaiter {
            receiver,
            threshold,
        }
    }
}

/// How a wait ended, so a caller that *acts* on a completion can tell a refresh
/// that happened from one that never will.
///
/// Both variants mean the caller should stop waiting; they differ in what it may
/// do next. A caller merely gating on the initial load can proceed either way; a
/// caller that treats the wait as proof a refresh landed — broadcasting
/// readiness, creating a follow-on schedule — must not proceed on
/// [`RefreshCompletionOutcome::Abandoned`].
///
/// `Answered` says the refresh this waiter was taken for completed — a `next`
/// waiter is bound to its own request by [`RefreshRequestId`] — but not that the
/// table it was taken from is still the live one. Revalidating a completion
/// against a table that may have been removed or rebuilt while it ran is tracked
/// in <https://github.com/spiceai/spiceai/issues/13603>.
///
/// Deliberately not `#[must_use]`: most waits are taken by a caller that acts on
/// nothing afterwards and only wants to block, and marking the type would make
/// every one of those state a choice it does not have. The callers that *do* act
/// on a completion are the ones this enum exists for, and each of them reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshCompletionOutcome {
    /// The question the waiter was taken for was answered: a refresh was
    /// recorded, or the signal was closed to say none will run here.
    Answered,
    /// Every [`RefreshCompletion`] was dropped before the question was answered,
    /// so no refresh ran and none can. The table the waiter was taken from is
    /// gone.
    Abandoned,
}

impl RefreshCompletionOutcome {
    /// Whether a refresh completed, or was declared never to run here.
    #[must_use]
    pub fn is_answered(self) -> bool {
        matches!(self, Self::Answered)
    }

    /// Whether the wait ended only because every recorder was dropped.
    #[must_use]
    pub fn is_abandoned(self) -> bool {
        matches!(self, Self::Abandoned)
    }
}

/// A pending wait for a refresh completion, taken from a [`RefreshCompletion`].
#[derive(Debug)]
pub struct RefreshCompletionWaiter {
    receiver: watch::Receiver<CompletionState>,
    /// The [`RefreshRequestId`] a recorded completion has to exceed to answer
    /// this waiter. See [`RefreshCompletion::waiter`].
    threshold: RefreshRequestId,
}

impl RefreshCompletionWaiter {
    /// Waits for the completion this waiter was taken for, reporting whether one
    /// arrived.
    ///
    /// Returns [`RefreshCompletionOutcome::Answered`] without waiting when the
    /// question was already answered when the waiter was taken, and
    /// [`RefreshCompletionOutcome::Abandoned`] when every recorder is dropped
    /// before it could be. Blocking on the latter would strand the caller rather
    /// than inform it, but it is not a completed refresh: the state is
    /// re-examined before each wait, so a completion recorded before the last
    /// recorder went still reads as `Answered`.
    ///
    /// The wait loops because not every transition answers this waiter — an
    /// issue, or the completion of a refresh already running when the waiter was
    /// taken, both wake it without deciding it.
    pub async fn wait(mut self) -> RefreshCompletionOutcome {
        loop {
            if self.is_answered() {
                return RefreshCompletionOutcome::Answered;
            }
            if self.receiver.changed().await.is_err() {
                return RefreshCompletionOutcome::Abandoned;
            }
        }
    }

    /// Whether the current state answers the question this waiter was taken for.
    fn is_answered(&self) -> bool {
        let state = *self.receiver.borrow();
        state.closed || state.completed > self.threshold
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::{RefreshCompletion, RefreshCompletionOutcome};

    const SHORT: Duration = Duration::from_millis(200);

    /// Records a refresh nobody was waiting to correlate with, the way a caller
    /// that only needs *some* completion on the table would produce one.
    fn record_one(completion: &RefreshCompletion) {
        let id = completion.issue();
        completion.record(id);
    }

    /// The lost wakeup this type exists to remove: a completion that lands
    /// between taking the waiter and awaiting it still resolves the wait.
    #[tokio::test]
    async fn next_observes_a_completion_recorded_before_the_wait() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        record_one(&completion);

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("a completion recorded before the wait must still resolve it");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }

    #[tokio::test]
    async fn next_observes_a_completion_recorded_during_the_wait() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        let recorder = completion.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            record_one(&recorder);
        });

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("a completion recorded during the wait must resolve it");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }

    /// `next` answers "the refresh I am about to trigger", so completions that
    /// predate the waiter must not satisfy it.
    #[tokio::test]
    async fn next_ignores_a_completion_recorded_before_the_waiter_was_taken() {
        let completion = RefreshCompletion::new();
        record_one(&completion);

        let waiter = completion.next();

        let _ = timeout(SHORT, waiter.wait())
            .await
            .expect_err("an earlier completion must not satisfy a waiter taken after it");
    }

    /// The regression this correlation exists for (#13544): a refresh that was
    /// *already requested* when the waiter was taken can finish a moment later,
    /// and its completion says nothing about the refresh the caller went on to
    /// trigger. Before request ids, this released the waiter — which on the
    /// partition-assignment path acked `PartitionsLoaded` for rows that had not
    /// been loaded, so the scheduler routed queries to an executor that would
    /// answer them incompletely.
    #[tokio::test]
    async fn next_ignores_the_completion_of_a_refresh_requested_before_it() {
        let completion = RefreshCompletion::new();
        // A periodic refresh is already in flight: issued, not yet complete.
        let periodic = completion.issue();

        let waiter = completion.next();

        // The caller installs its change and triggers its own refresh, which is
        // still running when the periodic one lands.
        let _triggered = completion.issue();
        completion.record(periodic);

        let _ = timeout(SHORT, waiter.wait()).await.expect_err(
            "a refresh requested before the waiter says nothing about the one it triggered",
        );
    }

    /// The other half of the same scenario: once the refresh the caller actually
    /// triggered lands, its waiter resolves.
    #[tokio::test]
    async fn next_is_answered_by_the_refresh_requested_after_it() {
        let completion = RefreshCompletion::new();
        let periodic = completion.issue();

        let waiter = completion.next();

        let triggered = completion.issue();
        completion.record(periodic);
        completion.record(triggered);

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("the triggered refresh must resolve its own waiter");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }

    /// A request arriving mid-refresh cancels the one in flight, which then
    /// never completes. A waiter must be released by the request that superseded
    /// it rather than wait forever for a completion that cannot arrive.
    #[tokio::test]
    async fn next_is_answered_by_a_later_request_when_the_earlier_one_never_completes() {
        let completion = RefreshCompletion::new();
        let _cancelled = completion.issue();

        let waiter = completion.next();

        let triggered = completion.issue();
        completion.record(triggered);

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("a superseded request must not strand the waiters behind it");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }

    /// Completions arrive in request order, but a threshold that could move
    /// backwards would un-answer a waiter that has already been released.
    #[tokio::test]
    async fn record_never_walks_the_threshold_backwards() {
        let completion = RefreshCompletion::new();
        let first = completion.issue();
        let second = completion.issue();

        let waiter = completion.next();
        let third = completion.issue();

        completion.record(third);
        completion.record(second);
        completion.record(first);

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("an out-of-order completion must not retract an answered wait");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }

    /// `any` answers "has the initial load landed", so it must be satisfied by a
    /// completion that predates the waiter.
    #[tokio::test]
    async fn any_is_satisfied_by_a_completion_recorded_before_the_waiter_was_taken() {
        let completion = RefreshCompletion::new();
        record_one(&completion);

        let outcome = timeout(SHORT, completion.any().wait())
            .await
            .expect("an earlier completion must satisfy a waiter for any completion");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }

    /// Correlation narrows `next` only. A caller gating on the initial load is
    /// asking whether the table has data, not which request produced it, so a
    /// refresh requested before the waiter still answers.
    #[tokio::test]
    async fn any_is_satisfied_by_the_completion_of_a_refresh_requested_before_it() {
        let completion = RefreshCompletion::new();
        let periodic = completion.issue();

        let waiter = completion.any();
        completion.record(periodic);

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("any completion answers the initial-load question");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }

    #[tokio::test]
    async fn any_waits_when_no_completion_has_been_recorded() {
        let completion = RefreshCompletion::new();

        let _ = timeout(SHORT, completion.any().wait())
            .await
            .expect_err("no completion has been recorded, so there is nothing to observe");
    }

    /// A request that has been issued but not completed is not a completion, so
    /// it must not satisfy the initial-load question either.
    #[tokio::test]
    async fn any_waits_while_a_request_is_only_issued() {
        let completion = RefreshCompletion::new();
        let _issued = completion.issue();

        let _ = timeout(SHORT, completion.any().wait())
            .await
            .expect_err("an issued request has loaded nothing yet");
    }

    /// The CDC apply loop answers no trigger. Its completions keep the
    /// pre-correlation semantics: everyone waiting when it lands is released.
    #[tokio::test]
    async fn an_untriggered_completion_releases_the_waiters_taken_before_it() {
        let completion = RefreshCompletion::new();
        let next_waiter = completion.next();
        let any_waiter = completion.any();

        completion.record_untriggered();

        for (waiter, label) in [(next_waiter, "next"), (any_waiter, "any")] {
            let outcome = timeout(SHORT, waiter.wait()).await.unwrap_or_else(|_| {
                panic!("an untriggered completion must release a `{label}` waiter taken before it")
            });
            assert_eq!(outcome, RefreshCompletionOutcome::Answered);
        }
    }

    #[tokio::test]
    async fn an_untriggered_completion_does_not_release_a_later_next_waiter() {
        let completion = RefreshCompletion::new();
        completion.record_untriggered();

        let _ = timeout(SHORT, completion.next().wait())
            .await
            .expect_err("a `next` waiter asks about work that has not happened yet");
    }

    #[tokio::test]
    async fn close_resolves_a_waiter_taken_before_it() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        completion.close();

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("closing must release a waiter already taken");
        assert_eq!(
            outcome,
            RefreshCompletionOutcome::Answered,
            "an explicit close is a deliberate answer, not an abandoned table"
        );
    }

    #[tokio::test]
    async fn close_resolves_a_waiter_taken_after_it() {
        let completion = RefreshCompletion::new();
        completion.close();

        let outcome = timeout(SHORT, completion.next().wait())
            .await
            .expect("closing must release a waiter taken afterwards");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }

    /// The table that records completions can be dropped while a caller is
    /// waiting; the caller learns that no completion is coming instead of
    /// blocking for the life of the process.
    #[tokio::test]
    async fn a_waiter_resolves_when_the_recorder_is_dropped() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        drop(completion);

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("dropping the recorder must release its waiters");
        assert_eq!(
            outcome,
            RefreshCompletionOutcome::Abandoned,
            "a released waiter with no completion behind it must not read as a refresh"
        );
    }

    /// A refresh that was requested but never completed is not a completion
    /// either: dropping the recorder mid-flight must report the table as gone,
    /// not the refresh as done.
    #[tokio::test]
    async fn a_request_left_in_flight_by_a_drop_is_abandoned() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        let _in_flight = completion.issue();
        drop(completion);

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("dropping the recorder must release its waiters");
        assert_eq!(outcome, RefreshCompletionOutcome::Abandoned);
    }

    /// A completion that landed before the last recorder went is still a
    /// completion: dropping the recorder afterwards must not downgrade an
    /// answered wait into an abandoned one.
    #[tokio::test]
    async fn a_completion_recorded_before_the_drop_still_reads_as_answered() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        record_one(&completion);
        drop(completion);

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("a recorded completion must resolve the wait");
        assert_eq!(
            outcome,
            RefreshCompletionOutcome::Answered,
            "the refresh happened; the recorder going afterwards does not unhappen it"
        );
    }

    /// An `any` waiter taken after the last recorder is gone has no completion
    /// behind it and must say so rather than resolving as one.
    #[tokio::test]
    async fn a_waiter_taken_after_the_recorder_is_dropped_is_abandoned() {
        let completion = RefreshCompletion::new();
        let waiter = completion.any();

        drop(completion);

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("a waiter with no live recorder must not block");
        assert_eq!(outcome, RefreshCompletionOutcome::Abandoned);
    }

    /// Waiters are independent: satisfying one must not consume the completion
    /// another is waiting for.
    #[tokio::test]
    async fn concurrent_waiters_all_observe_one_completion() {
        let completion = RefreshCompletion::new();
        let waiters: Vec<_> = (0..8).map(|_| completion.next()).collect();

        record_one(&completion);

        for waiter in waiters {
            let outcome = timeout(SHORT, waiter.wait())
                .await
                .expect("every waiter taken before the completion must resolve");
            assert_eq!(outcome, RefreshCompletionOutcome::Answered);
        }
    }

    /// Request ids saturate rather than wrap, because a waiter compares against
    /// them. The ceiling is unreachable in practice; what must hold is that ids
    /// stay ordered right up to it, so a waiter taken near the top still
    /// resolves.
    #[tokio::test]
    async fn a_waiter_resolves_at_the_top_of_the_id_range() {
        let completion = RefreshCompletion::new();
        completion
            .state
            .send_modify(|state| state.issued = u64::MAX - 2);

        let waiter = completion.next();
        let id = completion.issue();
        assert_eq!(id, u64::MAX - 1, "ids must stay ordered near the ceiling");
        completion.record(id);

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("an id below the ceiling must still resolve its waiter");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }
}
