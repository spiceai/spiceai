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

//! Cancelling a spawned Vortex task must not corrupt memory.
//!
//! Cayenne's read and write paths spawn work onto the Vortex IO runtime and join it through
//! one-shot channels. Cancelling such a task drops a channel receiver that has already been
//! polled, while the sender is still delivering its result. A channel whose receiver releases
//! its stored waker from inside its own destructor frees that waker under the sender about to
//! call it, corrupting the heap and faulting on an indirect call through the freed vtable —
//! observed in production as a `SIGSEGV` on a `tokio-rt-worker` thread.
//!
//! This test exists in this repository, rather than only in the Vortex fork, so that bumping
//! the Vortex pin cannot silently reintroduce the hazard: it drives the cancellation directly,
//! and so fails on any pin that regresses it. Note that Cargo permits only one revision per
//! git source, so the `vortex-io` dev-dependency cannot drift away from the pins that the rest
//! of the workspace uses.
//!
//! It is a stress test rather than a deterministic one, and it detects the fault by killing
//! the test process — `SIGSEGV`, or `SIGABRT` via the allocator's heap checks — rather than by
//! failing an assertion, because the damage is done before control returns. Against an
//! affected pin it died within a second on every attempt.

use std::future::Future;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use futures::future::poll_fn;
use vortex_io::runtime::tokio::TokioRuntime;

/// Long enough to cover the window reliably; short enough for CI.
const RUN_FOR: Duration = Duration::from_secs(10);
/// Sized to match the layout of the channel that carries a spawned task's result, so the
/// allocation lands in the same size class as the production path.
type Payload = [u64; 11];
const WORKERS: u64 = 16;

/// Cheap deterministic jitter, so cancellation lands at varying points relative to the
/// spawned future's completion. A fixed delay would only ever probe one interleaving.
fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_cancelling_a_polled_vortex_task_is_sound() {
        let handle = TokioRuntime::current();
        let deadline = Instant::now() + RUN_FOR;

        let mut workers = Vec::new();
        for worker in 0..WORKERS {
            let handle = handle.clone();
            workers.push(tokio::spawn(async move {
                let mut seed = worker.wrapping_mul(7919).wrapping_add(1);
                let mut cancelled = 0u64;
                while Instant::now() < deadline {
                    let r = xorshift(&mut seed);
                    let spin = r % 2048;
                    let yields = (r >> 11) % 4;

                    let mut task = Box::pin(handle.spawn(async move {
                        for _ in 0..yields {
                            tokio::task::yield_now().await;
                        }
                        let acc = (0..spin).fold(0u64, |acc, i| acc.wrapping_add(i));
                        let out: Payload = [acc; 11];
                        out
                    }));

                    // Poll once so the task handle registers a waker, then drop it while the
                    // spawned future is still completing. This is what cancellation does.
                    let first = poll_fn(|cx| Poll::Ready(task.as_mut().poll(cx))).await;
                    if first.is_pending() {
                        for _ in 0..((r >> 24) % 3) {
                            tokio::task::yield_now().await;
                        }
                        drop(task);
                        cancelled += 1;
                    }
                }
                cancelled
            }));
        }

        let mut cancelled = 0u64;
        for worker in workers {
            cancelled += worker.await.expect("stress worker panicked");
        }
        assert!(cancelled > 0, "no cancellation actually raced a completion");
    }
}
