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

//! A bounded, jittered retry loop for compare-and-swap writes.
//!
//! Every read-modify-write of shared state has the same shape: read the object
//! and its version, compute the change, write it conditionally, and start over
//! when another writer got there first. Two things go wrong when each feature
//! writes that loop itself: an unbounded loop spins forever under sustained
//! contention, and retrying in lockstep keeps colliding with the same writers.
//! [`retry_on_conflict`] bounds the attempts and spreads the retries with
//! jittered exponential backoff.

use std::future::Future;
use std::time::Duration;

/// How a compare-and-swap loop backs off while other writers keep winning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictRetry {
    /// Attempts before giving up, counting the first. At least 1.
    pub max_attempts: usize,
    /// Upper bound of the wait before the second attempt.
    pub initial_delay: Duration,
    /// Upper bound of any single wait.
    pub max_delay: Duration,
}

impl Default for ConflictRetry {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_delay: Duration::from_millis(25),
            max_delay: Duration::from_secs(2),
        }
    }
}

impl ConflictRetry {
    /// The wait before attempt `next_attempt` (2 for the first retry): equal
    /// jitter over an exponentially growing, capped window, so concurrent
    /// writers that collided once are unlikely to collide again.
    fn delay_before(&self, next_attempt: usize) -> Duration {
        let exponent = u32::try_from(next_attempt.saturating_sub(2)).unwrap_or(u32::MAX);
        let window = self
            .initial_delay
            .saturating_mul(2_u32.saturating_pow(exponent))
            .min(self.max_delay);
        let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
        let half = window_ms / 2;
        Duration::from_millis(half + rand::random_range(0..=window_ms - half))
    }
}

/// The outcome of one attempt of a compare-and-swap loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attempt<T> {
    /// The change landed.
    Done(T),
    /// Another writer changed the object first; read it again and retry.
    Conflict,
}

/// Why a compare-and-swap loop gave up.
#[derive(Debug)]
pub enum RetryOnConflictError<E> {
    /// Every attempt lost to a concurrent writer.
    ConflictsExhausted {
        /// Attempts made, all of them conflicts.
        attempts: usize,
    },
    /// An attempt failed for a reason other than a conflict.
    Failed(E),
}

impl<E: std::fmt::Display> std::fmt::Display for RetryOnConflictError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConflictsExhausted { attempts } => write!(
                f,
                "another writer changed the object before each of {attempts} attempts"
            ),
            Self::Failed(err) => write!(f, "{err}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for RetryOnConflictError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ConflictsExhausted { .. } => None,
            Self::Failed(err) => Some(err),
        }
    }
}

/// Runs `attempt` until it reports [`Attempt::Done`], fails, or has lost
/// `policy.max_attempts` times to concurrent writers.
///
/// `attempt` must re-read the object each time it runs: retrying a write
/// against the version that just lost can never succeed.
///
/// # Errors
///
/// Returns [`RetryOnConflictError::Failed`] with the first non-conflict error,
/// or [`RetryOnConflictError::ConflictsExhausted`] when every attempt
/// conflicted.
pub async fn retry_on_conflict<T, E, F, Fut>(
    policy: &ConflictRetry,
    mut attempt: F,
) -> Result<T, RetryOnConflictError<E>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Attempt<T>, E>>,
{
    let max_attempts = policy.max_attempts.max(1);
    for n in 1..=max_attempts {
        match attempt().await {
            Ok(Attempt::Done(value)) => return Ok(value),
            Ok(Attempt::Conflict) if n < max_attempts => {
                tokio::time::sleep(policy.delay_before(n + 1)).await;
            }
            Ok(Attempt::Conflict) => {}
            Err(err) => return Err(RetryOnConflictError::Failed(err)),
        }
    }
    Err(RetryOnConflictError::ConflictsExhausted {
        attempts: max_attempts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fast() -> ConflictRetry {
        ConflictRetry {
            max_attempts: 4,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(4),
        }
    }

    #[tokio::test]
    async fn returns_the_value_once_an_attempt_lands() {
        let calls = AtomicUsize::new(0);
        let result: Result<u32, RetryOnConflictError<String>> =
            retry_on_conflict(&fast(), || async {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    Ok(Attempt::Conflict)
                } else {
                    Ok(Attempt::Done(7))
                }
            })
            .await;
        assert_eq!(result.expect("third attempt lands"), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts_of_conflicts() {
        let calls = AtomicUsize::new(0);
        let result: Result<(), RetryOnConflictError<String>> =
            retry_on_conflict(&fast(), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Attempt::Conflict)
            })
            .await;
        assert!(
            matches!(
                result,
                Err(RetryOnConflictError::ConflictsExhausted { attempts: 4 })
            ),
            "{result:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 4, "exactly max_attempts");
    }

    #[tokio::test]
    async fn stops_at_the_first_non_conflict_error() {
        let calls = AtomicUsize::new(0);
        let result: Result<(), RetryOnConflictError<&str>> = retry_on_conflict(&fast(), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err("access denied")
        })
        .await;
        assert!(
            matches!(result, Err(RetryOnConflictError::Failed("access denied"))),
            "{result:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "failures are not retried");
    }

    #[test]
    fn delays_grow_and_stay_within_the_window() {
        let policy = ConflictRetry {
            max_attempts: 10,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(400),
        };
        for _ in 0..200 {
            let second = policy.delay_before(2);
            assert!((50..=100).contains(&second.as_millis()), "{second:?}");
            let third = policy.delay_before(3);
            assert!((100..=200).contains(&third.as_millis()), "{third:?}");
            let late = policy.delay_before(9);
            assert!((200..=400).contains(&late.as_millis()), "capped: {late:?}");
        }
    }

    #[test]
    fn zero_attempts_still_tries_once() {
        let policy = ConflictRetry {
            max_attempts: 0,
            ..fast()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime");
        let calls = AtomicUsize::new(0);
        let result: Result<(), RetryOnConflictError<String>> =
            runtime.block_on(retry_on_conflict(&policy, || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Attempt::Conflict)
            }));
        assert!(matches!(
            result,
            Err(RetryOnConflictError::ConflictsExhausted { attempts: 1 })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
