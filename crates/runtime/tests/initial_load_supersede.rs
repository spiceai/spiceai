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

//! `Runtime::supersede_initial_load` — the escape hatch that lets a
//! control-plane deployment stop a component load that will not finish.
//!
//! The initial load has no deadline: `load_dataset` retries a transient failure
//! for as long as the runtime is up. Spice Cloud Connect starts before the load
//! for exactly that reason, and a deployment arriving mid-load abandons it
//! rather than letting it keep registering datasets from the configuration it
//! is restarting away from.

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
