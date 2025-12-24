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
#![allow(clippy::missing_errors_doc)]

pub mod chat;
pub mod embed;

use std::sync::Arc;

use aws_sdk_bedrockruntime::{
    Client,
    error::SdkError,
    operation::{
        converse::{ConverseError, ConverseOutput, builders::ConverseFluentBuilder},
        converse_stream::{
            ConverseStreamError, ConverseStreamOutput, builders::ConverseStreamFluentBuilder,
        },
        invoke_model::{InvokeModelError, InvokeModelOutput},
    },
    primitives::Blob,
};
use runtime_rate_control::RateController;
use snafu::ResultExt;
use util::{
    RetryError,
    fibonacci_backoff::{FibonacciBackoff, FibonacciBackoffBuilder},
    retry,
};

use aws_config::SdkConfig;

use crate::openai::default_rate_controller;

#[derive(Debug, Clone)]
pub struct BedrockClient {
    pub(crate) client: Arc<aws_sdk_bedrockruntime::Client>,
    // Retry strategy for transient or throttling errors
    retry_strategy: FibonacciBackoff,

    rate_controller: Arc<RateController>,
}

impl From<&SdkConfig> for BedrockClient {
    fn from(value: &SdkConfig) -> Self {
        BedrockClient::new(value, default_rate_controller())
    }
}

impl BedrockClient {
    #[must_use]
    pub fn new(config: &SdkConfig, rate_controller: Arc<RateController>) -> Self {
        let client = aws_sdk_bedrockruntime::Client::new(config).into();
        Self {
            client,
            rate_controller,
            retry_strategy: default_retry_strategy(),
        }
    }

    /// Perform a [Converse Stream API operation](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ConverseStream.html) with appropriate rate-limiting and retry logic.
    pub async fn do_converse_stream(
        &self,
        converse_build: ConverseStreamFluentBuilder,
    ) -> Result<ConverseStreamOutput, Box<dyn std::error::Error + Send + Sync>> {
        self.rate_limit_request_with_retry(move |_client| {
            let value = converse_build.clone();
            async move {
                match value.send().await {
                    Ok(response) => Ok(response),
                    Err(SdkError::ServiceError(service_error)) => match service_error.into_err() {
                        ConverseStreamError::ThrottlingException(throttle_e) => {
                            tracing::debug!(
                                "Bedrock model throttled whilst conversing, backing off and retrying..."
                            );
                            Err(RetryError::transient(
                                Box::new(throttle_e) as Box<dyn std::error::Error + Send + Sync>
                            ))
                        }
                        e => Err(RetryError::permanent(
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                        )),
                    },
                    Err(e) => Err(RetryError::permanent(
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                    ))
                }
            }
        })
        .await
    }

    /// Perform a Converse [API operation](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html) with appropriate rate-limiting and retry logic.
    pub async fn do_converse(
        &self,
        converse_build: ConverseFluentBuilder,
    ) -> Result<ConverseOutput, Box<dyn std::error::Error + Send + Sync>> {
        self.rate_limit_request_with_retry(move |_client| {
            let value = converse_build.clone();
            async move {
                match value.send().await {
                    Ok(response) => Ok(response),
                    Err(SdkError::ServiceError(service_error)) => match service_error.into_err() {
                        ConverseError::ThrottlingException(throttle_e) => {
                            tracing::debug!(
                                "Bedrock model throttled whilst conversing, backing off and retrying..."
                            );
                            Err(RetryError::transient(
                                Box::new(throttle_e) as Box<dyn std::error::Error + Send + Sync>
                            ))
                        }
                        e => Err(RetryError::permanent(
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                        )),
                    },
                    Err(e) => Err(RetryError::permanent(
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                    ))
                }
            }
        })
        .await
    }

    /// Perform an Invoke [API operation](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_InvokeModel.html) with appropriate rate-limiting and retry logic.
    pub async fn do_invoke(
        &self,
        model_id: impl Into<String>,
        body: impl Into<Vec<u8>>,
    ) -> Result<InvokeModelOutput, Box<dyn std::error::Error + Send + Sync>> {
        let model_id = model_id.into();
        let body = body.into();
        self.rate_limit_request_with_retry(move |client| {
            let b = body.clone();
            let m = model_id.clone();
            async move {
            match client
                .invoke_model()
                .model_id(m)
                .body(Blob::new(b))
                .content_type("application/json")
                .send()
                .await
            {
                Ok(response) => Ok(response),
                Err(SdkError::ServiceError(service_error)) => match service_error.into_err() {
                    InvokeModelError::ThrottlingException(throttle_e) => {
                        tracing::debug!(
                            "Bedrock model throttled whilst conversing, backing off and retrying..."
                        );
                        Err(RetryError::transient(
                            Box::new(throttle_e) as Box<dyn std::error::Error + Send + Sync>
                        ))
                    }
                    e => Err(RetryError::permanent(
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                    )),
                },
                Err(e) => Err(RetryError::permanent(
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                )),
            }}
        })
        .await
    }

    pub(crate) async fn rate_limit_request_with_retry<O, Fut, F>(
        &self,
        make_request: F,
    ) -> Result<O, Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn(Arc<Client>) -> Fut,
        Fut: std::future::Future<
                Output = Result<O, RetryError<Box<dyn std::error::Error + Send + Sync>>>,
            >,
        O: Send + 'static,
    {
        let permit = self.rate_controller.acquire().await.boxed()?;

        let result = retry(self.retry_strategy.clone(), || async {
            permit.until_ready().await.boxed()?;
            make_request(Arc::clone(&self.client)).await
        })
        .await;

        drop(permit);

        result
    }
}

fn default_retry_strategy() -> FibonacciBackoff {
    FibonacciBackoffBuilder::new().max_retries(Some(10)).build()
}
