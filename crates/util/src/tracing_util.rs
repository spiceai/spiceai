/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::future::Future;

use tracing::{Dispatch, instrument::WithSubscriber, subscriber};

fn fmt_subscriber() -> tracing_subscriber::FmtSubscriber {
    tracing_subscriber::FmtSubscriber::builder()
        .with_ansi(true)
        .finish()
}

pub fn in_tracing_context<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    subscriber::with_default(fmt_subscriber(), f)
}

/// Async equivalent of [`in_tracing_context`]. Use this when the work that
/// needs a temporary subscriber is async (e.g. it `.await`s I/O). The given
/// future is polled with the temporary `FmtSubscriber` as its dispatcher, so
/// `tracing::*` events emitted from any awaited code reach a subscriber even
/// if the global subscriber has not been installed yet.
pub async fn in_tracing_context_async<F, R>(f: F) -> R
where
    F: Future<Output = R>,
{
    f.with_subscriber(Dispatch::new(fmt_subscriber())).await
}
