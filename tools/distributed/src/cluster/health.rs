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

use anyhow::{Context, Result};
use std::sync::OnceLock;
use std::time::Duration;

static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

pub struct HealthCheck {
    pub max_attempts: usize,
    pub interval_ms: u64,
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self {
            max_attempts: 30,
            interval_ms: 1000,
        }
    }
}

impl HealthCheck {
    /// Wait for a service to be ready by polling its health endpoint.
    pub async fn wait_for_ready(&self, url: &str) -> Result<()> {
        for attempt in 1..=self.max_attempts {
            match self.check_health(url).await {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    if attempt < self.max_attempts {
                        tokio::time::sleep(Duration::from_millis(self.interval_ms)).await;
                    }
                }
                Err(e) => {
                    if attempt < self.max_attempts {
                        tokio::time::sleep(Duration::from_millis(self.interval_ms)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        Err(anyhow::anyhow!(
            "Service did not become ready after {} attempts",
            self.max_attempts
        ))
    }

    /// Check health of a service via HTTP GET to /health endpoint.
    pub async fn check_health(&self, url: &str) -> Result<bool> {
        let client = HTTP_CLIENT.get_or_init(|| {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        });

        let response = client
            .get(url)
            .send()
            .await
            .context("Failed to connect to health endpoint")?;
        Ok(response.status().is_success())
    }
}

/// Get the health check URL for a given HTTP port.
#[must_use]
pub fn get_health_url(http_port: u16) -> String {
    format!("http://127.0.0.1:{http_port}/health")
}
