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
//! Callers ask two different questions of a refresh, and both are answered from
//! the same count of completed refreshes:
//!
//! * *"has the initial load landed?"* — [`RefreshCompletion::any`], satisfied by
//!   a refresh that finished before the caller asked.
//! * *"has the refresh I just triggered finished?"* — [`RefreshCompletion::next`],
//!   satisfied only by a refresh recorded after the waiter was taken.
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

/// What every waiter is decided against. Held in one `watch` value so a
/// completion and a close are each a single atomic transition that also wakes
/// the waiters already registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompletionState {
    /// Refreshes recorded so far. Wrapping, because a waiter is decided by the
    /// `watch` version it subscribed at rather than by comparing this value.
    completions: u64,
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
                completions: 0,
                closed: false,
            }),
        }
    }

    /// Records a completed refresh, resolving every waiter taken before it.
    pub fn record(&self) {
        self.state
            .send_modify(|state| state.completions = state.completions.wrapping_add(1));
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

    /// `accept_earlier` selects between the two questions above: whether a
    /// refresh recorded before this call already answers it.
    fn waiter(&self, accept_earlier: bool) -> RefreshCompletionWaiter {
        // `subscribe` marks the receiver as having seen the value current now,
        // so it resolves on the next transition. The state is then read back
        // through the receiver rather than the sender: a completion landing
        // between the two is either seen here or wakes the receiver, never
        // dropped between them.
        let mut receiver = self.state.subscribe();
        let state = *receiver.borrow();
        if state.closed || (accept_earlier && state.completions > 0) {
            // Already answered — resolve on the first poll instead of waiting
            // for a transition that has happened, or can never happen.
            receiver.mark_changed();
        }
        RefreshCompletionWaiter { receiver }
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
/// `Answered` is the weaker half of that pair: it says *a* refresh completed, not
/// that it was the one this waiter's caller triggered, nor that the table is
/// still the live one. Binding a completion to its request and to a table
/// generation is tracked in
/// <https://github.com/spiceai/spiceai/issues/13603>.
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
}

impl RefreshCompletionWaiter {
    /// Waits for the completion this waiter was taken for, reporting whether one
    /// arrived.
    ///
    /// Returns [`RefreshCompletionOutcome::Answered`] without waiting when the
    /// question was already answered when the waiter was taken, and
    /// [`RefreshCompletionOutcome::Abandoned`] when every recorder is dropped
    /// before it could be. Blocking on the latter would strand the caller rather
    /// than inform it, but it is not a completed refresh: `changed` reports the
    /// transition it has already observed ahead of the closed channel, so a
    /// completion recorded before the last recorder went still reads as
    /// `Answered`.
    pub async fn wait(mut self) -> RefreshCompletionOutcome {
        if self.receiver.changed().await.is_ok() {
            RefreshCompletionOutcome::Answered
        } else {
            RefreshCompletionOutcome::Abandoned
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::{RefreshCompletion, RefreshCompletionOutcome};

    const SHORT: Duration = Duration::from_millis(200);

    /// The lost wakeup this type exists to remove: a completion that lands
    /// between taking the waiter and awaiting it still resolves the wait.
    #[tokio::test]
    async fn next_observes_a_completion_recorded_before_the_wait() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        completion.record();

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
            recorder.record();
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
        completion.record();

        let waiter = completion.next();

        let _ = timeout(SHORT, waiter.wait())
            .await
            .expect_err("an earlier completion must not satisfy a waiter taken after it");
    }

    /// `any` answers "has the initial load landed", so it must be satisfied by a
    /// completion that predates the waiter.
    #[tokio::test]
    async fn any_is_satisfied_by_a_completion_recorded_before_the_waiter_was_taken() {
        let completion = RefreshCompletion::new();
        completion.record();

        let outcome = timeout(SHORT, completion.any().wait())
            .await
            .expect("an earlier completion must satisfy a waiter for any completion");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }

    #[tokio::test]
    async fn any_waits_when_no_completion_has_been_recorded() {
        let completion = RefreshCompletion::new();

        let _ = timeout(SHORT, completion.any().wait())
            .await
            .expect_err("no completion has been recorded, so there is nothing to observe");
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

    /// A completion that landed before the last recorder went is still a
    /// completion: dropping the recorder afterwards must not downgrade an
    /// answered wait into an abandoned one.
    #[tokio::test]
    async fn a_completion_recorded_before_the_drop_still_reads_as_answered() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        completion.record();
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

        completion.record();

        for waiter in waiters {
            let outcome = timeout(SHORT, waiter.wait())
                .await
                .expect("every waiter taken before the completion must resolve");
            assert_eq!(outcome, RefreshCompletionOutcome::Answered);
        }
    }

    /// The completion count wraps rather than saturating, and a waiter is
    /// decided by the `watch` version it subscribed at, so a wrap cannot strand
    /// it.
    #[tokio::test]
    async fn a_waiter_resolves_across_a_generation_wrap() {
        let completion = RefreshCompletion::new();
        completion
            .state
            .send_modify(|state| state.completions = u64::MAX);

        let waiter = completion.next();
        completion.record();

        let outcome = timeout(SHORT, waiter.wait())
            .await
            .expect("a wrapping generation must still resolve its waiter");
        assert_eq!(outcome, RefreshCompletionOutcome::Answered);
    }
}
