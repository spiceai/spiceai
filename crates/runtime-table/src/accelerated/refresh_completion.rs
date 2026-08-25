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
//! completion that lands before the caller polls its future leaves the caller
//! waiting for a refresh that already happened.

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

/// A pending wait for a refresh completion, taken from a [`RefreshCompletion`].
#[derive(Debug)]
pub struct RefreshCompletionWaiter {
    receiver: watch::Receiver<CompletionState>,
}

impl RefreshCompletionWaiter {
    /// Waits for the completion this waiter was taken for.
    ///
    /// Returns without waiting when the question was already answered when the
    /// waiter was taken, and returns early when the table that records
    /// completions is gone — in both cases nothing is coming, and blocking would
    /// strand the caller rather than inform it.
    pub async fn wait(mut self) {
        let _ = self.receiver.changed().await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::RefreshCompletion;

    const SHORT: Duration = Duration::from_millis(200);

    /// The lost wakeup this type exists to remove: a completion that lands
    /// between taking the waiter and awaiting it still resolves the wait.
    #[tokio::test]
    async fn next_observes_a_completion_recorded_before_the_wait() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        completion.record();

        timeout(SHORT, waiter.wait())
            .await
            .expect("a completion recorded before the wait must still resolve it");
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

        timeout(SHORT, waiter.wait())
            .await
            .expect("a completion recorded during the wait must resolve it");
    }

    /// `next` answers "the refresh I am about to trigger", so completions that
    /// predate the waiter must not satisfy it.
    #[tokio::test]
    async fn next_ignores_a_completion_recorded_before_the_waiter_was_taken() {
        let completion = RefreshCompletion::new();
        completion.record();

        let waiter = completion.next();

        timeout(SHORT, waiter.wait())
            .await
            .expect_err("an earlier completion must not satisfy a waiter taken after it");
    }

    /// `any` answers "has the initial load landed", so it must be satisfied by a
    /// completion that predates the waiter.
    #[tokio::test]
    async fn any_is_satisfied_by_a_completion_recorded_before_the_waiter_was_taken() {
        let completion = RefreshCompletion::new();
        completion.record();

        timeout(SHORT, completion.any().wait())
            .await
            .expect("an earlier completion must satisfy a waiter for any completion");
    }

    #[tokio::test]
    async fn any_waits_when_no_completion_has_been_recorded() {
        let completion = RefreshCompletion::new();

        timeout(SHORT, completion.any().wait())
            .await
            .expect_err("no completion has been recorded, so there is nothing to observe");
    }

    #[tokio::test]
    async fn close_resolves_a_waiter_taken_before_it() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        completion.close();

        timeout(SHORT, waiter.wait())
            .await
            .expect("closing must release a waiter already taken");
    }

    #[tokio::test]
    async fn close_resolves_a_waiter_taken_after_it() {
        let completion = RefreshCompletion::new();
        completion.close();

        timeout(SHORT, completion.next().wait())
            .await
            .expect("closing must release a waiter taken afterwards");
    }

    /// The table that records completions can be dropped while a caller is
    /// waiting; the caller learns that no completion is coming instead of
    /// blocking for the life of the process.
    #[tokio::test]
    async fn a_waiter_resolves_when_the_recorder_is_dropped() {
        let completion = RefreshCompletion::new();
        let waiter = completion.next();

        drop(completion);

        timeout(SHORT, waiter.wait())
            .await
            .expect("dropping the recorder must release its waiters");
    }

    /// Waiters are independent: satisfying one must not consume the completion
    /// another is waiting for.
    #[tokio::test]
    async fn concurrent_waiters_all_observe_one_completion() {
        let completion = RefreshCompletion::new();
        let waiters: Vec<_> = (0..8).map(|_| completion.next()).collect();

        completion.record();

        for waiter in waiters {
            timeout(SHORT, waiter.wait())
                .await
                .expect("every waiter taken before the completion must resolve");
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

        timeout(SHORT, waiter.wait())
            .await
            .expect("a wrapping generation must still resolve its waiter");
    }
}
