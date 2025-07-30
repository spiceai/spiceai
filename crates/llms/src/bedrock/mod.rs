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

pub mod embed;
pub mod rate_limit;

use std::sync::Arc;

use aws_sdk_bedrockruntime::{
    error::SdkError,
    operation::invoke_model::{InvokeModelError, InvokeModelOutput},
    primitives::Blob,
};
use governor::{RateLimiter, clock::DefaultClock, state::InMemoryState};
use snafu::ResultExt;
use tokio::sync::Semaphore;
use util::{
    RetryError,
    fibonacci_backoff::{FibonacciBackoff, FibonacciBackoffBuilder},
    retry,
};

use aws_config::SdkConfig;

use rate_limit::BedrockRateLimitConfig;

#[derive(Debug, Clone)]
pub struct BedrockClient {
    pub(crate) client: aws_sdk_bedrockruntime::Client,
    rate_limiter: Arc<RateLimiter<governor::state::NotKeyed, InMemoryState, DefaultClock>>,
    // Control the max number of concurrent requests
    semaphore: Arc<Semaphore>,
    // Retry strategy for transient or throttling errors
    retry_strategy: FibonacciBackoff,
    // Rate limiting configuration for logging and metrics
    rate_config: BedrockRateLimitConfig,
}

impl BedrockClient {
    #[must_use]
    pub fn new(config: &SdkConfig, rate_config: BedrockRateLimitConfig) -> Self {
        let client = aws_sdk_bedrockruntime::Client::new(config);
        Self {
            client,
            rate_limiter: Arc::new(RateLimiter::direct(rate_config.to_quota())),
            semaphore: Arc::new(Semaphore::new(rate_config.max_concurrent_invocations)),
            retry_strategy: default_retry_strategy(),
            rate_config,
        }
    }

    pub(crate) async fn do_the_thing(
        &self,
        model_id: impl Into<String>,
        body: impl Into<Vec<u8>>,
    ) -> Result<InvokeModelOutput, Box<dyn std::error::Error + Send + Sync>> {
        // Control num concurrent requests
        let _permit = self.semaphore.acquire().await.boxed()?;

        let model_id: String = model_id.into();
        let body: Vec<u8> = body.into();

        let response = retry(self.retry_strategy.clone(), || async {
            self.rate_limiter.until_ready().await;

            match self
                .client
                .invoke_model()
                .model_id(model_id.clone())
                .body(Blob::new(body.clone()))
                .content_type("application/json")
                .send()
                .await
            {
                Ok(response) => Ok(response),
                Err(e) => Err(match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        InvokeModelError::ThrottlingException(_) => {
                            tracing::debug!(
                                "Bedrock embedding model throttled, backing off and retrying..."
                            );
                            RetryError::transient(e)
                        }
                        _ => RetryError::permanent(e),
                    },
                    _ => RetryError::permanent(e),
                }),
            }
        })
        .await
        .boxed()?;

        Ok(response)
    }
}

fn default_retry_strategy() -> FibonacciBackoff {
    FibonacciBackoffBuilder::new().max_retries(Some(10)).build()
}
