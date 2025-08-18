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

use std::{sync::Arc, time::Duration};

use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
};
use snafu::ResultExt;
use snafu::prelude::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to acquire semaphore permit. {source}"))]
    SemaphoreAcquireError { source: tokio::sync::AcquireError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default)]
pub struct JitterConfig {
    min: Duration,
    max: Duration,
}

impl JitterConfig {
    #[must_use]
    pub fn new(min: Duration, max: Duration) -> Self {
        Self { min, max }
    }

    #[must_use]
    pub fn zero() -> Self {
        Self::new(Duration::ZERO, Duration::ZERO)
    }
}

#[derive(Debug, Default)]
pub struct RateControllerBuilder {
    jitter: Option<JitterConfig>,
    max_concurrent_requests: Option<usize>,
    quotas: Vec<Quota>,
}

impl RateControllerBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_jitter(mut self, jitter: JitterConfig) -> Self {
        self.jitter = Some(jitter);
        self
    }

    #[must_use]
    pub fn with_max_concurrent_requests(mut self, max_concurrent_requests: usize) -> Self {
        self.max_concurrent_requests = Some(max_concurrent_requests);
        self
    }

    #[must_use]
    pub fn add_quota(mut self, quota: Quota) -> Self {
        self.quotas.push(quota);
        self
    }

    #[must_use]
    pub fn with_quotas(mut self, quotas: Vec<Quota>) -> Self {
        self.quotas = quotas;
        self
    }

    #[must_use]
    pub fn build(self) -> Arc<RateController> {
        let jitter = self.jitter;
        let rate_limiters = self
            .quotas
            .into_iter()
            .map(|quota| Arc::new(RateLimiter::direct(quota)))
            .collect::<Vec<_>>();

        let semaphore = self
            .max_concurrent_requests
            .map(|max_concurrent_requests| Arc::new(Semaphore::new(max_concurrent_requests)));

        RateController::new(jitter, rate_limiters, semaphore)
    }
}

#[derive(Debug)]
pub struct RateController {
    jitter_config: JitterConfig,
    rate_limiters: Vec<Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>>,
    semaphore: Option<Arc<Semaphore>>,
}

#[derive(Debug)]
pub struct Permit {
    permit: Option<OwnedSemaphorePermit>,
    rate_controller: Arc<RateController>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            drop(permit);
        }
    }
}

impl Permit {
    /// Re-check the quotas from an existing permit.
    /// For example, a request was permitted but has failed and needs to be retried.
    /// The caller retains their permit, but needs to ensure the rate limiters are still ready.
    pub async fn until_ready(&self) {
        Arc::clone(&self.rate_controller).until_ready().await;
    }
}

impl RateController {
    #[must_use]
    pub fn builder() -> RateControllerBuilder {
        RateControllerBuilder::new()
    }

    async fn until_ready(self: Arc<Self>) {
        futures::future::join_all(
            self.rate_limiters
                .iter()
                .map(|limiter| limiter.until_ready()),
        )
        .await;
    }

    fn new(
        jitter: Option<JitterConfig>,
        rate_limiters: Vec<Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>>,
        semaphore: Option<Arc<Semaphore>>,
    ) -> Arc<Self> {
        let jitter_config = jitter.unwrap_or(JitterConfig {
            min: Duration::ZERO,
            max: Duration::ZERO,
        });

        Arc::new(Self {
            jitter_config,
            rate_limiters,
            semaphore,
        })
    }

    /// Acquires a permit from the rate limiter.
    /// Asynchronously waits for the rate limiters to be ready and optionally acquires a semaphore permit for maximum concurrency if configured.
    ///
    /// # Errors
    ///
    /// If the semaphore has been closed, this will return an error.
    pub async fn acquire(self: &Arc<Self>) -> Result<Permit> {
        let self_cloned = Arc::clone(self);

        // check for concurrency first - we may end up waiting for a concurrent request long enough that the rate limits clear
        let semaphore = if let Some(semaphore) = &self.semaphore {
            Some(
                Arc::clone(semaphore)
                    .acquire_owned()
                    .await
                    .context(SemaphoreAcquireSnafu)?,
            )
        } else {
            None
        };

        // check all of the rate limiters async
        Arc::clone(self).until_ready().await;

        // add jitter
        let jitter_wait = rand::random_range(self.jitter_config.min..=self.jitter_config.max);
        tokio::time::sleep(jitter_wait).await;

        Ok(Permit {
            permit: semaphore,
            rate_controller: self_cloned,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, time::Instant};

    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_acquire() {
        let rate_controller = RateControllerBuilder::new()
            .with_jitter(JitterConfig {
                min: Duration::from_millis(100),
                max: Duration::from_millis(200),
            })
            .with_max_concurrent_requests(5)
            .add_quota(Quota::per_second(
                NonZeroU32::new(10).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        let permit = rate_controller.acquire().await;
        assert!(
            permit.is_ok(),
            "Failed to acquire permit: {:?}",
            permit.err()
        );
        let permit = permit.expect("should be Ok");
        assert!(
            permit.permit.is_some(),
            "Semaphore permit should be Some if semaphore is configured"
        );

        // Test that semaphore restricts concurrency
        drop(permit);
        let permits = (0..5)
            .map(|_| rate_controller.acquire())
            .collect::<Vec<_>>();
        let mut results = futures::future::try_join_all(permits)
            .await
            .expect("Should acquire all permits");

        // the next request should block until one of the permits is dropped
        tokio::select! {
            _ = rate_controller.acquire() => {
                panic!("Expected semaphore to block, but it did not.");
            },
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        };

        // dropping one permit should allow the next request to immediately acquire a permit
        drop(
            results
                .pop()
                .expect("Should have at least one permit to drop"),
        );

        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit after dropping one: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.permit.is_some(), "Semaphore permit should be Some if semaphore is configured");
            },
            () = tokio::time::sleep(Duration::from_secs(1)) => {
                panic!("Expected to acquire a permit after dropping one, but timed out.");
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_permit_waits() {
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_minute(
                NonZeroU32::new(10).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        let permit = rate_controller.acquire().await;
        assert!(
            permit.is_ok(),
            "Failed to acquire permit: {:?}",
            permit.err()
        );
        let permit = permit.expect("should be Ok");

        // Make 9 more waits to full the quota from the permit
        futures::future::join_all((0..9).map(|_| permit.until_ready())).await;

        // The next request should wait until the rate limit is reset
        tokio::select! {
            () = permit.until_ready() => {
                panic!("Expected rate limiter to block, but it did not.");
            },
            () = tokio::time::sleep(Duration::from_secs(5)) => {}
        }

        // permit should be able to be ready after the rate limit resets
        tokio::select! {
            () = permit.until_ready() => {}
            () = tokio::time::sleep(Duration::from_secs(1)) => {
                panic!("Expected to be able to acquire a permit after rate limit reset, but timed out.");
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_per_second() {
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                NonZeroU32::new(2).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        // acquire all 2 permits at once, which should exhaust the rate limit
        futures::future::try_join_all((0..2).map(|_| rate_controller.acquire()))
            .await
            .expect("Should acquire all permits");

        // the next request should wait until free
        tokio::select! {
            _ = rate_controller.acquire() => {
                panic!("Expected rate limiter to block, but it did not.");
            },
            () = tokio::time::sleep(Duration::from_millis(400)) => {}
        }

        // next permit should occur after the next reset
        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit after rate limit reset: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.permit.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_millis(400)) => {
                panic!("Expected to acquire a permit after rate limit reset, but timed out.");
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_with_multiple_quotas() {
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                // purposely set a high per-second limit which should not be hit
                NonZeroU32::new(100).expect("NonZeroU32 should be non-zero"),
            ))
            .add_quota(Quota::per_minute(
                // should result in per minute quota being hit
                NonZeroU32::new(10).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        // acquire all 10 permits at once, which should exhaust the per-minute rate limit
        futures::future::try_join_all((0..10).map(|_| rate_controller.acquire()))
            .await
            .expect("Should acquire all permits");

        // the next request should wait until free
        tokio::select! {
            _ = rate_controller.acquire() => {
                panic!("Expected rate limiter to block, but it did not.");
            },
            // 10/minute is 1 every 6 seconds
            () = tokio::time::sleep(Duration::from_secs(5)) => {}
        }

        // next permit should occur after the next reset
        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit after rate limit reset: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.permit.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_secs(1)) => {
                panic!("Expected to acquire a permit after rate limit reset, but timed out.");
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_hits_multiple_quotas() {
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                // per-second will get hit first
                NonZeroU32::new(4).expect("NonZeroU32 should be non-zero"),
            ))
            .add_quota(Quota::per_minute(
                // then per-minute will get hit
                NonZeroU32::new(6).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        // acquire all 4 permits at once, which should exhaust the per-second rate limit
        futures::future::try_join_all((0..4).map(|_| rate_controller.acquire()))
            .await
            .expect("Should acquire all permits");

        // the next request should wait until free
        tokio::select! {
            _ = rate_controller.acquire() => {
                panic!("Expected rate limiter to block, but it did not.");
            },
            () = tokio::time::sleep(Duration::from_millis(200)) => {}
        }

        // next permit should occur after the next reset
        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit after rate limit reset: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.permit.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_millis(200)) => {
                panic!("Expected to acquire a permit after rate limit reset, but timed out.");
            }
        }

        // now we've hit the per-minute limit
        // the next request should wait until free
        tokio::select! {
            _ = rate_controller.acquire() => {
                panic!("Expected rate limiter to block, but it did not.");
            },
            // 6/minute is 1 every 10 seconds
            () = tokio::time::sleep(Duration::from_secs(9)) => {}
        }

        // next permit should occur after the next reset
        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit after rate limit reset: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.permit.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_secs(2)) => {
                panic!("Expected to acquire a permit after rate limit reset, but timed out.");
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_jitter() {
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                // purposely set a high per-second limit which should not be hit
                NonZeroU32::new(100).expect("NonZeroU32 should be non-zero"),
            ))
            .with_jitter(JitterConfig {
                min: Duration::from_millis(1000),
                max: Duration::from_millis(2000),
            })
            .build();

        // acquiring a permit should wait at least for the jitter minimum duration
        let start = Instant::now();
        tokio::select! {
            permit = rate_controller.acquire() => {
                let end = Instant::now();
                let elapsed = end.duration_since(start);
                assert!(elapsed >= Duration::from_millis(1000), "Expected at least 1000ms of jitter, but got {elapsed:?}");
                assert!(permit.is_ok(), "Failed to acquire permit: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.permit.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_millis(2000)) => {
                panic!("Expected to wait for up to 2000ms, but timed out.");
            }
        }

        // a rate limit without jitter should complete near immediately
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                // purposely set a high per-second limit which should not be hit
                NonZeroU32::new(100).expect("NonZeroU32 should be non-zero"),
            ))
            .build();

        tokio::select! {
            permit = rate_controller.acquire() => {
                assert!(permit.is_ok(), "Failed to acquire permit: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.permit.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_nanos(100)) => {
                panic!("Expected to acquire a permit immediately, but timed out.");
            }
        }

        // rate limiter with multiple quotas should apply jitter only once
        let rate_controller = RateControllerBuilder::new()
            .add_quota(Quota::per_second(
                NonZeroU32::new(100).expect("NonZeroU32 should be non-zero"),
            ))
            .add_quota(Quota::per_minute(
                NonZeroU32::new(10).expect("NonZeroU32 should be non-zero"),
            ))
            .with_jitter(JitterConfig {
                min: Duration::from_millis(1000),
                max: Duration::from_millis(2000),
            })
            .build();

        let start = Instant::now();
        tokio::select! {
            permit = rate_controller.acquire() => {
                let end = Instant::now();
                let elapsed = end.duration_since(start);
                assert!(elapsed >= Duration::from_millis(1000), "Expected at least 1000ms of jitter, but got {elapsed:?}");
                assert!(permit.is_ok(), "Failed to acquire permit: {:?}", permit.err());
                let permit = permit.expect("should be Ok");
                assert!(permit.permit.is_none(), "Semaphore permit should be None if semaphore is not configured");
            },
            () = tokio::time::sleep(Duration::from_millis(2000)) => {
                panic!("Expected to wait for up to 2000ms, but timed out.");
            }
        }
    }
}
