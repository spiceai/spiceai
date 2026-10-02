/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2016 Tibor Benke

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
use std::time::Duration;

use crate::backoff::Backoff;
use crate::error::Error;

/// Observes each transient failure and the delay before the next attempt.
pub trait Notify<E> {
    fn notify(&mut self, err: E, duration: Duration);
}

impl<E, F: FnMut(E, Duration)> Notify<E> for F {
    fn notify(&mut self, err: E, duration: Duration) {
        self(err, duration);
    }
}

/// A [`Notify`] that ignores every failure.
#[derive(Debug, Clone, Copy)]
pub struct NoopNotify;

impl<E> Notify<E> for NoopNotify {
    fn notify(&mut self, _: E, _: Duration) {}
}

/// Runs `operation` until it succeeds, fails permanently, or `backoff` gives
/// up, sleeping on the Tokio timer between attempts. `backoff` is reset first.
///
/// # Errors
///
/// Returns the permanent error, or the last transient error once `backoff`
/// returns `None`.
pub async fn retry<T, E, F, Fut, B>(backoff: B, operation: F) -> Result<T, E>
where
    B: Backoff,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error<E>>>,
{
    retry_notify(backoff, operation, NoopNotify).await
}

/// [`retry`], calling `notify` with each transient error and the delay before
/// the next attempt.
///
/// # Errors
///
/// Returns the permanent error, or the last transient error once `backoff`
/// returns `None`.
pub async fn retry_notify<T, E, F, Fut, B, N>(
    mut backoff: B,
    mut operation: F,
    mut notify: N,
) -> Result<T, E>
where
    B: Backoff,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error<E>>>,
    N: Notify<E>,
{
    backoff.reset();
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(Error::Permanent(err)) => return Err(err),
            Err(Error::Transient { err, retry_after }) => {
                let Some(delay) = retry_after.or_else(|| backoff.next_backoff()) else {
                    return Err(err);
                };
                notify.notify(err, delay);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use super::*;

    /// Yields the given delays, then stops.
    struct Delays(VecDeque<Duration>);

    impl Backoff for Delays {
        fn next_backoff(&mut self) -> Option<Duration> {
            self.0.pop_front()
        }
    }

    fn delays(millis: &[u64]) -> Delays {
        Delays(millis.iter().copied().map(Duration::from_millis).collect())
    }

    #[tokio::test(start_paused = true)]
    async fn retries_transient_errors_until_success() {
        let mut attempts = 0;
        let mut notified = Vec::new();
        let result = retry_notify(
            delays(&[10, 20, 30]),
            || {
                attempts += 1;
                let attempt = attempts;
                async move {
                    if attempt < 3 {
                        Err(Error::transient(attempt))
                    } else {
                        Ok("done")
                    }
                }
            },
            |err, delay| notified.push((err, delay)),
        )
        .await;

        assert_eq!(result, Ok("done"));
        assert_eq!(attempts, 3);
        assert_eq!(
            notified,
            [
                (1, Duration::from_millis(10)),
                (2, Duration::from_millis(20))
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sleeps_for_each_delay() {
        let start = tokio::time::Instant::now();
        let mut attempts = 0;
        let result: Result<(), &str> = retry(delays(&[100, 200]), || {
            attempts += 1;
            async { Err(Error::transient("nope")) }
        })
        .await;

        assert_eq!(result, Err("nope"));
        assert_eq!(attempts, 3);
        assert_eq!(start.elapsed(), Duration::from_millis(300));
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_error_is_returned_without_retrying() {
        let mut attempts = 0;
        let result: Result<(), &str> = retry(delays(&[10]), || {
            attempts += 1;
            async { Err(Error::permanent("fatal")) }
        })
        .await;

        assert_eq!(result, Err("fatal"));
        assert_eq!(attempts, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_overrides_the_backoff() {
        let mut attempts = 0;
        let mut notified = Vec::new();
        let result = retry_notify(
            delays(&[]),
            || {
                attempts += 1;
                let attempt = attempts;
                async move {
                    if attempt == 1 {
                        Err(Error::Transient {
                            err: "rate limited",
                            retry_after: Some(Duration::from_secs(7)),
                        })
                    } else {
                        Ok(attempt)
                    }
                }
            },
            |_, delay| notified.push(delay),
        )
        .await;

        assert_eq!(result, Ok(2));
        assert_eq!(notified, [Duration::from_secs(7)]);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_is_reset_before_the_first_attempt() {
        struct CountResets(Rc<Cell<usize>>);

        impl Backoff for CountResets {
            fn reset(&mut self) {
                self.0.set(self.0.get() + 1);
            }

            fn next_backoff(&mut self) -> Option<Duration> {
                None
            }
        }

        let resets = Rc::new(Cell::new(0));
        let result: Result<(), &str> = retry(CountResets(Rc::clone(&resets)), || async {
            Err(Error::transient("once"))
        })
        .await;

        assert_eq!(result, Err("once"));
        assert_eq!(resets.get(), 1);
    }
}
