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

//! Admission order and adaptive recovery, driven through the public
//! [`RateController`] the way the HTTP connector drives it: a fixed set of
//! clients that each acquire a permit, send one request, record its outcome and
//! release the permit, in a loop.
//!
//! Every test runs on a paused clock, so the timings are exact and a run of
//! minutes takes milliseconds.

#![expect(clippy::expect_used, reason = "integration-test helpers")]

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use governor::Quota;
use parking_lot::Mutex;
use runtime_rate_control::{
    AdaptiveRateControl, Error, Permit, RateController, RateControllerBuilder, RequestOutcome,
};
use tokio::{task::JoinHandle, time::Instant};

const ORIGIN: &str = "http://127.0.0.1:37080";

/// One acquire attempt, as one client saw it.
#[derive(Clone, Copy, Debug)]
struct Attempt {
    /// When the client started waiting, from the start of the run.
    started: Duration,
    waited: Duration,
    timed_out: bool,
}

impl Attempt {
    fn ended(&self) -> Duration {
        self.started + self.waited
    }
}

/// The origin's health over time: failing for `failing` at the start of each
/// `cycle`, healthy for the rest of it.
#[derive(Clone, Copy, Debug)]
struct Outages {
    cycle: Duration,
    failing: Duration,
}

impl Outages {
    fn is_failing(&self, at: Duration) -> bool {
        Duration::from_nanos(
            u64::try_from(at.as_nanos() % self.cycle.as_nanos()).expect("fits in u64"),
        ) < self.failing
    }

    /// The moment each outage in `0..until` ended.
    fn recoveries(&self, until: Duration) -> Vec<Duration> {
        let cycles = u32::try_from(until.as_nanos().div_ceil(self.cycle.as_nanos()))
            .expect("a test runs a few cycles");
        (0..cycles)
            .map(|cycle| self.cycle * cycle + self.failing)
            .filter(|recovery| *recovery < until)
            .collect()
    }
}

/// Run `clients` closed-loop clients against `controller` for `duration`,
/// each sending one request per permit to an origin with the given `outages`.
/// Returns every acquire attempt, in no particular order.
async fn drive(
    controller: &Arc<RateController>,
    clients: usize,
    outages: Outages,
    duration: Duration,
) -> Vec<Attempt> {
    // Each request spends this long at the origin while it holds its permit.
    const REQUEST_LATENCY: Duration = Duration::from_millis(5);

    let start = Instant::now();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let tasks: Vec<_> = (0..clients)
        .map(|_| {
            let controller = Arc::clone(controller);
            let attempts = Arc::clone(&attempts);
            tokio::spawn(async move {
                while start.elapsed() < duration {
                    let started = start.elapsed();
                    let result = controller.acquire().await;
                    let timed_out = match &result {
                        Ok(_) => false,
                        Err(Error::AcquireTimeout { .. }) => true,
                        Err(error) => panic!("unexpected acquire error: {error}"),
                    };
                    attempts.lock().push(Attempt {
                        started,
                        waited: start.elapsed().saturating_sub(started),
                        timed_out,
                    });
                    if let Ok(permit) = result {
                        tokio::time::sleep(REQUEST_LATENCY).await;
                        controller.record_outcome(if outages.is_failing(start.elapsed()) {
                            RequestOutcome::Failure
                        } else {
                            RequestOutcome::Success
                        });
                        drop(permit);
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("client task should not panic");
    }
    Arc::try_unwrap(attempts)
        .expect("every client has finished")
        .into_inner()
}

fn adaptive(window: Duration) -> AdaptiveRateControl {
    AdaptiveRateControl::new(0.1, window).expect("valid adaptive settings")
}

/// Regression test for #14912: requests that queue while the origin fails are
/// admitted soon after it recovers, instead of waiting out their acquire bound
/// while later requests take every cell at the healthy charge.
///
/// The issue's reproduction: 20 clients, 10 requests per second, a 5s adaptive
/// window and a 30s acquire bound, against an origin that fails for 15s and
/// is then healthy for 35s, three times over.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn requests_queued_through_an_outage_are_admitted_after_it() {
    let rate = 10;
    let clients = 20;
    let outages = Outages {
        cycle: Duration::from_secs(50),
        failing: Duration::from_secs(15),
    };
    let run = Duration::from_secs(150);
    let controller = RateControllerBuilder::new()
        .add_quota_with_name(
            "requests_per_second",
            Quota::per_second(NonZeroU32::new(rate).expect("non-zero")),
        )
        .with_adaptive(adaptive(Duration::from_secs(5)), ORIGIN)
        .with_acquire_timeout(Duration::from_secs(30))
        .build();

    // The admission coefficient just before each recovery: how deep the
    // throttle is that the queued requests have to recover from.
    let at_recovery = tokio::spawn({
        let controller = Arc::clone(&controller);
        let start = Instant::now();
        async move {
            let mut coefficients = Vec::new();
            for recovery in outages.recoveries(run) {
                tokio::time::sleep_until(start + recovery.saturating_sub(Duration::from_millis(1)))
                    .await;
                coefficients.push(
                    controller
                        .admission_coefficient()
                        .expect("adaptive control is enabled"),
                );
            }
            coefficients
        }
    });
    let attempts = drive(&controller, clients, outages, run).await;
    let at_recovery = at_recovery.await.expect("the sampler should not panic");

    let timed_out: Vec<_> = attempts.iter().filter(|a| a.timed_out).collect();
    assert!(
        timed_out.is_empty(),
        "no request may wait out its acquire bound: {timed_out:?}"
    );

    // At the full rate, the queue of 20 drains in 2s. Adaptive control is
    // still restoring the rate in the first seconds after a recovery, so allow
    // three times that.
    let drain = Duration::from_secs(u64::try_from(clients).expect("fits") / u64::from(rate));
    for (recovery, coefficient) in outages.recoveries(run).into_iter().zip(at_recovery) {
        // Non-vacuous: the throttle at least halves the rate when the origin
        // recovers, so every request in the queue asked for at least twice the
        // healthy charge when it arrived — the charge that used to strand it.
        assert!(
            coefficient < 0.5,
            "the throttle should be engaged when the origin recovers at {recovery:?}, got coefficient {coefficient}"
        );

        let waiting: Vec<_> = attempts
            .iter()
            .filter(|a| a.started < recovery && a.ended() > recovery)
            .collect();
        assert_eq!(
            waiting.len(),
            clients,
            "every client should be waiting when the origin recovers at {recovery:?}"
        );
        let slowest = waiting
            .iter()
            .map(|a| a.ended().saturating_sub(recovery))
            .max()
            .unwrap_or_default();
        assert!(
            slowest <= drain * 3,
            "a request waiting when the origin recovered at {recovery:?} was admitted {slowest:?} after it"
        );
    }
}

/// A request queued for the quota is not overtaken by one that arrives after it
/// at a lighter charge. That overtaking is what stranded requests queued
/// during an outage: requests arriving after the recovery took each cell as it
/// refilled.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_later_request_does_not_overtake_one_queued_for_the_quota() {
    let controller = RateControllerBuilder::new()
        .add_quota_with_name(
            "requests_per_second",
            Quota::per_second(NonZeroU32::new(10).expect("non-zero")),
        )
        .with_adaptive(adaptive(Duration::from_secs(5)), ORIGIN)
        .build();

    // Half the bucket is spent and the origin fails, so the next request asks
    // for the whole bucket and waits for the other half to refill.
    for _ in 0..5 {
        drop(controller.acquire().await.expect("within the burst"));
    }
    for _ in 0..100 {
        controller.record_outcome(RequestOutcome::Failure);
    }
    let admitted = Arc::new(Mutex::new(Vec::new()));
    let first = spawn_acquire(&controller, &admitted, "first");
    tokio::task::yield_now().await;

    // The origin recovers, and a request arrives at the healthy charge, which
    // the half-full bucket could serve at once.
    tokio::time::sleep(Duration::from_millis(10)).await;
    for _ in 0..1000 {
        controller.record_outcome(RequestOutcome::Success);
    }
    assert_healthy(&controller);
    let second = spawn_acquire(&controller, &admitted, "second");

    let _permits = admit_both(first, second).await;
    assert_eq!(*admitted.lock(), ["first", "second"]);
}

/// A request waiting for concurrency slots is admitted once the adaptive
/// window's decay alone makes its charge fit the slots that are free, even
/// while the requests in flight report nothing and return nothing.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_concurrency_wait_is_admitted_when_decay_alone_makes_room() {
    let window = Duration::from_secs(5);
    let controller = RateControllerBuilder::new()
        .with_max_concurrent_requests(4)
        .with_adaptive(adaptive(window), ORIGIN)
        .with_acquire_timeout(Duration::from_secs(30))
        .build();

    // One slow request is in flight when the origin starts failing, so the
    // next request asks for all four slots while three are free.
    let in_flight = controller.acquire().await.expect("the first request");
    for _ in 0..100 {
        controller.record_outcome(RequestOutcome::Failure);
    }
    let queued_at = Instant::now();
    let queued = tokio::spawn({
        let controller = Arc::clone(&controller);
        async move { controller.acquire().await }
    });

    // With 100 failures and a 5s half-life, the charge falls to the three free
    // slots once the coefficient reaches 1/3: when the window holds
    // (1 - 1/3) / (100 / 3) = 1/50 of its failures, 5s * log2(50) = 28.2s on.
    let admitted = queued
        .await
        .expect("the task should not panic")
        .expect("decay makes room before the 30s acquire bound");
    let waited = queued_at.elapsed();
    assert!(
        (Duration::from_millis(28_200)..Duration::from_millis(28_300)).contains(&waited),
        "admitted after {waited:?}"
    );
    assert_eq!(
        controller.available_permits(),
        Some(0),
        "the request holds the three free slots it was charged"
    );
    drop(admitted);
    drop(in_flight);
}

/// A request waiting for concurrency slots re-reads its charge as soon as a
/// request in flight reports an outcome, since that request can hold its own
/// slot a while longer, reading the response.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_concurrency_wait_rereads_its_charge_when_an_outcome_is_reported() {
    let controller = RateControllerBuilder::new()
        .with_max_concurrent_requests(4)
        .with_adaptive(adaptive(Duration::from_secs(5)), ORIGIN)
        .with_acquire_timeout(Duration::from_secs(30))
        .build();

    // One request is in flight when the origin starts failing, so the next
    // request asks for all four slots while three are free.
    let in_flight = controller.acquire().await.expect("the first request");
    for _ in 0..100 {
        controller.record_outcome(RequestOutcome::Failure);
    }
    let queued_at = Instant::now();
    let queued = tokio::spawn({
        let controller = Arc::clone(&controller);
        async move { controller.acquire().await }
    });

    // A second later the origin recovers. The request in flight reports
    // success but keeps its slot.
    let recovery = Duration::from_secs(1);
    tokio::time::sleep(recovery).await;
    for _ in 0..1000 {
        controller.record_outcome(RequestOutcome::Success);
    }
    assert_healthy(&controller);

    let admitted = queued
        .await
        .expect("the task should not panic")
        .expect("the acquire should succeed");
    let waited = queued_at.elapsed();
    assert!(
        (recovery..recovery + Duration::from_millis(10)).contains(&waited),
        "admitted after {waited:?}; the outcome at {recovery:?} should have admitted it"
    );
    assert_eq!(
        controller.available_permits(),
        Some(2),
        "the request holds one slot, as a healthy request does"
    );
    drop(admitted);
    drop(in_flight);
}

/// A request queued for a concurrency slot is not overtaken by one that
/// arrives as the slot frees, before the queued request has run.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_later_request_does_not_overtake_one_queued_for_concurrency() {
    let controller = RateControllerBuilder::new()
        .with_max_concurrent_requests(1)
        .build();

    // The only slot is taken, so the next request queues for it.
    let in_flight = controller.acquire().await.expect("the first request");
    let admitted = Arc::new(Mutex::new(Vec::new()));
    let first = spawn_acquire(&controller, &admitted, "first");
    tokio::task::yield_now().await;

    // The slot frees and another request arrives before the queued one runs.
    drop(in_flight);
    let second = controller.acquire();
    tokio::pin!(second);
    assert!(
        futures::poll!(second.as_mut()).is_pending(),
        "the later request must wait behind the queued one"
    );
    assert_eq!(
        controller.available_permits(),
        Some(1),
        "the later request must not take the slot ahead of the queued one"
    );

    let first = tokio::time::timeout(Duration::from_secs(1), first)
        .await
        .expect("the queued request is admitted")
        .expect("the task should not panic");
    assert_eq!(*admitted.lock(), ["first"]);
    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(1), second)
        .await
        .expect("the later request is admitted once the slot frees again")
        .expect("the acquire should succeed");
    drop(second);
}

/// Acquire a permit on a new task and record `name` the moment it is admitted.
fn spawn_acquire(
    controller: &Arc<RateController>,
    admitted: &Arc<Mutex<Vec<&'static str>>>,
    name: &'static str,
) -> JoinHandle<Permit> {
    let controller = Arc::clone(controller);
    let admitted = Arc::clone(admitted);
    tokio::spawn(async move {
        let permit = controller
            .acquire()
            .await
            .expect("the acquire should succeed");
        admitted.lock().push(name);
        permit
    })
}

/// Wait for both acquires, bounded so a request that is never admitted fails
/// the test instead of hanging it, and hold both permits.
async fn admit_both(first: JoinHandle<Permit>, second: JoinHandle<Permit>) -> [Permit; 2] {
    let admit = |task: JoinHandle<Permit>| async move {
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("the request is admitted")
            .expect("the task should not panic")
    };
    [admit(first).await, admit(second).await]
}

/// A request that waits for a concurrency slot while the origin is failing is
/// charged the healthy count once it recovers, rather than the whole limit it
/// asked for when it arrived.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_concurrency_wait_takes_the_charge_in_force_when_admitted() {
    let window = Duration::from_secs(5);
    let controller = RateControllerBuilder::new()
        .with_max_concurrent_requests(4)
        .with_adaptive(adaptive(window), ORIGIN)
        .build();

    // The origin fails: a request now holds all four slots.
    for _ in 0..100 {
        controller.record_outcome(RequestOutcome::Failure);
    }
    let in_flight = controller.acquire().await.expect("the first request");
    assert_eq!(controller.available_permits(), Some(0));

    let queued = tokio::spawn({
        let controller = Arc::clone(&controller);
        async move { controller.acquire().await }
    });
    tokio::task::yield_now().await;
    assert!(!queued.is_finished(), "no slot is free yet");

    // The origin recovers while the request waits, then the request in flight
    // finishes.
    tokio::time::advance(window * 12).await;
    for _ in 0..100 {
        controller.record_outcome(RequestOutcome::Success);
    }
    assert_healthy(&controller);
    drop(in_flight);

    let admitted = tokio::time::timeout(Duration::from_secs(1), queued)
        .await
        .expect("a freed slot admits the waiting request")
        .expect("the task should not panic")
        .expect("the acquire should succeed");
    assert_eq!(
        controller.available_permits(),
        Some(3),
        "the waiting request holds one slot, as a healthy request does"
    );
    drop(admitted);
    assert_eq!(controller.available_permits(), Some(4));
}

/// A request waiting on a per-minute quota re-reads its charge while it
/// waits. Admitted at the charge it read on arrival, a request that queued at
/// the deepest throttle would need the whole minute's budget and outlast its
/// 30s acquire bound, however soon the origin recovered.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_quota_wait_takes_the_charge_in_force_when_admitted() {
    let per_minute = 60;
    let controller = RateControllerBuilder::new()
        .add_quota_with_name(
            "requests_per_minute",
            Quota::per_minute(NonZeroU32::new(per_minute).expect("non-zero")),
        )
        .with_adaptive(adaptive(Duration::from_secs(5)), ORIGIN)
        .with_acquire_timeout(Duration::from_secs(30))
        .build();

    // Spend the burst, then fail: the next request asks for the whole bucket.
    for _ in 0..per_minute {
        drop(controller.acquire().await.expect("within the burst"));
    }
    for _ in 0..100 {
        controller.record_outcome(RequestOutcome::Failure);
    }

    let queued_at = Instant::now();
    let queued = tokio::spawn({
        let controller = Arc::clone(&controller);
        async move { controller.acquire().await }
    });

    // The origin recovers between two re-reads.
    let recovery = Duration::from_millis(5500);
    tokio::time::sleep(recovery).await;
    assert!(!queued.is_finished(), "the bucket is still nearly empty");
    for _ in 0..1000 {
        controller.record_outcome(RequestOutcome::Success);
    }
    assert_healthy(&controller);

    queued
        .await
        .expect("the task should not panic")
        .expect("the waiting request is admitted at the healthy charge");
    // Admitted at its next re-read, within one healthy interval (1s) of the
    // recovery.
    let admitted_after = queued_at.elapsed();
    assert!(
        admitted_after > recovery && admitted_after <= recovery + Duration::from_secs(1),
        "admitted {admitted_after:?} after it queued, recovery was at {recovery:?}"
    );
}

/// A request that one quota charges above the healthy baseline has paid for the
/// origin's failures, and is counted in `adaptive_throttled_total` even when a
/// later quota then keeps it waiting past its acquire bound.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_throttled_charge_is_counted_when_a_later_quota_times_out() {
    let per_minute = 10;
    let controller = RateControllerBuilder::new()
        .add_quota_with_name(
            "requests_per_second",
            Quota::per_second(NonZeroU32::new(100).expect("non-zero")),
        )
        .add_quota_with_name(
            "requests_per_minute",
            Quota::per_minute(NonZeroU32::new(per_minute).expect("non-zero")),
        )
        .with_adaptive(adaptive(Duration::from_secs(5)), ORIGIN)
        .with_acquire_timeout(Duration::from_secs(1))
        .build();

    // Spend the per-minute burst, then fail about half the requests: the
    // coefficient settles near 0.5, so the next request is charged about twice
    // the healthy count. The per-second quota has room for that; the per-minute
    // quota will not for seconds.
    for _ in 0..per_minute {
        drop(controller.acquire().await.expect("within the burst"));
    }
    for request in 0..100 {
        controller.record_outcome(if request < 44 {
            RequestOutcome::Success
        } else {
            RequestOutcome::Failure
        });
    }
    let coefficient = controller
        .admission_coefficient()
        .expect("adaptive control is enabled");
    assert!(
        (0.4..0.6).contains(&coefficient),
        "the origin should be half throttled, got coefficient {coefficient}"
    );
    let throttled_before = controller.metrics().adaptive_throttled_total();

    let error = controller
        .acquire()
        .await
        .expect_err("the per-minute quota has nothing left for a second");
    assert!(
        matches!(error, Error::AcquireTimeout { .. }),
        "expected an acquire timeout, got {error:?}"
    );
    assert_eq!(
        controller.metrics().adaptive_throttled_total() - throttled_before,
        1,
        "the per-second quota charged the request above the healthy baseline"
    );
}

fn assert_healthy(controller: &RateController) {
    let coefficient = controller
        .admission_coefficient()
        .expect("adaptive control is enabled");
    assert!(
        (coefficient - 1.0).abs() < f64::EPSILON,
        "the origin should read as healthy, got coefficient {coefficient}"
    );
}
