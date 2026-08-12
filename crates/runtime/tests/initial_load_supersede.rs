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

//! The two ways out of an initial component load that will not finish:
//! `Runtime::wait_for_initial_load`, which waits for it under a bound of the
//! caller's choosing, and `Runtime::supersede_initial_load`, which abandons it.
//!
//! The load has no deadline of its own: `load_dataset` retries a transient
//! failure for as long as the runtime is up. Spice Cloud Connect starts before
//! the load for exactly that reason, and a deployment arriving mid-load must be
//! answered — applied once the load is over, or reported as not applied — rather
//! than reconciling against components the load has not registered yet.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::time::Duration;

use runtime::Runtime;

/// A load must be superseded exactly once, so exactly one caller owns
/// reconciling the partially-registered state it leaves behind.
#[tokio::test]
async fn supersede_initial_load_reports_a_single_winner() {
    let rt = Arc::new(Runtime::builder().build().await);

    assert!(
        rt.supersede_initial_load(),
        "the first caller is the one that stopped the load"
    );
    assert!(
        !rt.supersede_initial_load(),
        "a second caller has nothing left to stop and must apply normally"
    );
}

/// Superseding before the load starts must keep it from starting — the window
/// between Cloud Connect connecting and `load_components` being reached is
/// short, but a deployment landing inside it would otherwise have its app
/// re-loaded on top of itself.
#[tokio::test]
async fn a_load_superseded_before_it_starts_does_not_run() {
    let rt = Arc::new(Runtime::builder().build().await);

    assert!(rt.supersede_initial_load());

    tokio::time::timeout(Duration::from_secs(30), Arc::clone(&rt).load_components())
        .await
        .expect("a superseded load must return immediately instead of starting");

    assert!(
        !rt.supersede_initial_load(),
        "the load stays superseded once abandoned"
    );
}

/// Once the load has finished there is nothing to supersede, and a deployment
/// must take the ordinary `apply_app` path rather than the
/// reconcile-against-what-is-registered one.
#[tokio::test]
async fn a_completed_load_leaves_nothing_to_supersede() {
    let rt = Arc::new(Runtime::builder().build().await);

    tokio::time::timeout(Duration::from_secs(30), Arc::clone(&rt).load_components())
        .await
        .expect("an empty runtime finishes its component load");

    assert!(
        !rt.supersede_initial_load(),
        "a finished load must not report itself as superseded"
    );
}

/// A caller that cannot reconcile against a half-registered app waits for the
/// load, and the wait ends as soon as the load does.
#[tokio::test]
async fn waiting_for_the_initial_load_returns_when_it_finishes() {
    let rt = Arc::new(Runtime::builder().build().await);

    let waiting = tokio::spawn({
        let rt = Arc::clone(&rt);
        async move { rt.wait_for_initial_load(Duration::from_secs(30)).await }
    });

    tokio::time::timeout(Duration::from_secs(30), Arc::clone(&rt).load_components())
        .await
        .expect("an empty runtime finishes its component load");

    assert!(
        waiting.await.expect("the waiter finishes"),
        "the load finished, so the wait must report it settled"
    );
    assert!(
        rt.wait_for_initial_load(Duration::from_secs(30)).await,
        "a load that is already over must not make a later caller wait at all"
    );
}

/// A load that never finishes must not hold its caller: the bound is what turns
/// an unsatisfiable spicepod into an answered deployment instead of a stuck one.
#[tokio::test]
async fn waiting_for_a_load_that_never_runs_gives_up_at_the_bound() {
    let rt = Arc::new(Runtime::builder().build().await);

    let started = tokio::time::Instant::now();
    assert!(
        !rt.wait_for_initial_load(Duration::from_millis(200)).await,
        "a load still in flight must report itself as not settled"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "the wait must be the caller's bound, not an immediate answer"
    );
    assert!(
        rt.initial_load_in_flight(),
        "giving up on the wait leaves the load running"
    );
}

/// Abandoning the load settles it too: the caller that superseded it is the one
/// reconciling now, and anything else waiting on the load has nothing left to
/// wait for.
#[tokio::test]
async fn superseding_the_load_ends_the_wait() {
    let rt = Arc::new(Runtime::builder().build().await);

    let waiting = tokio::spawn({
        let rt = Arc::clone(&rt);
        async move { rt.wait_for_initial_load(Duration::from_secs(30)).await }
    });

    assert!(rt.supersede_initial_load());

    assert!(
        waiting.await.expect("the waiter finishes"),
        "a superseded load is over, so the wait must report it settled"
    );
}
