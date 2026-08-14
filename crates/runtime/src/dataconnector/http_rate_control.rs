/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Re-export of connector HTTP rate control, which lives in
//! `data-http-rate-control` so a connector can reach it without the runtime.
//!
//! The crate's entry points name a [`ConnectorComponent`] and a spicepod name;
//! [`resolve_config`] here is the `&Dataset` convenience form for the runtime's
//! own connectors, which have a dataset handle in hand.

use std::collections::HashMap;
use std::hash::BuildHasher;

pub use data_http_rate_control::*;

use crate::component::dataset::DatasetSpec;
use crate::dataconnector::{ConnectorComponent, DataConnectorResult};
use crate::parameters::Parameters;

/// Resolve a dataset's rate-control configuration.
///
/// # Errors
/// Returns an invalid-configuration error for any unparseable or out-of-range
/// `rate_control_*` / `http_*` parameter.
pub fn resolve_config<S: BuildHasher>(
    params: &Parameters,
    runtime_params: Option<&HashMap<String, String, S>>,
    dataset: &DatasetSpec,
    dataconnector: &'static str,
) -> DataConnectorResult<HttpRateControlConfig> {
    resolve_config_for_component(
        params,
        runtime_params,
        &ConnectorComponent::from(dataset),
        dataconnector,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::builder::DatasetBuilder;
    #[cfg(feature = "rate-control")]
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;
    use url::Url;

    use crate::dataconnector::DataConnectorError;
    #[cfg(feature = "rate-control")]
    use futures::{StreamExt, TryStreamExt};
    #[cfg(feature = "rate-control")]
    use object_store::ObjectStore;

    async fn test_dataset() -> Dataset {
        let app = Arc::new(app::AppBuilder::new("rate_control_registry_test".to_string()).build());
        let runtime = Arc::new(crate::Runtime::builder().build().await);

        DatasetBuilder::try_new(
            "https://rate-control-registry.example.com/data".to_string(),
            "rate_control_registry_test",
        )
        .expect("test dataset builder should be valid")
        .with_app(app)
        .with_runtime(runtime)
        .build()
        .expect("test dataset should build")
    }

    fn test_config(max_concurrent_requests: usize) -> HttpRateControlConfig {
        HttpRateControlConfig {
            max_concurrent_requests: Some(max_concurrent_requests),
            requests_per_second: None,
            requests_per_minute: None,
            jitter_min: Duration::ZERO,
            jitter_max: Duration::ZERO,
        }
    }

    #[cfg(feature = "rate-control")]
    fn persisted_test_config() -> HttpRateControlConfig {
        HttpRateControlConfig {
            max_concurrent_requests: None,
            requests_per_second: Some(
                NonZeroU32::new(10).expect("test rate limit should be non-zero"),
            ),
            requests_per_minute: None,
            jitter_min: Duration::ZERO,
            jitter_max: Duration::ZERO,
        }
    }

    /// Wait for a persisted-state object keyed by this spicepod and origin. The
    /// exact key ends in a hash of the origin that the rate-control crate keeps
    /// private, so match on the `{spicepod}/{host}_{port}-` prefix it derives.
    #[cfg(feature = "rate-control")]
    async fn wait_for_persisted_origin(store: &Arc<dyn ObjectStore>, spicepod: &str, url: &Url) {
        let origin = rate_control_key(url);
        let host_and_port = origin
            .split_once("://")
            .map_or(origin.as_str(), |(_, rest)| rest)
            .replace(':', "_");
        let prefix = format!("{spicepod}/{host_and_port}-");
        let start = tokio::time::Instant::now();

        loop {
            // `next()` yields `Option<Result<_>>`, so a listing *error* must not
            // read as "the object appeared" — fail on it instead of polling on.
            let found = match store
                .list(None)
                .try_filter(|object| {
                    std::future::ready(object.location.as_ref().starts_with(&prefix))
                })
                .next()
                .await
            {
                Some(Ok(_)) => true,
                Some(Err(error)) => panic!("failed to list persisted state objects: {error}"),
                None => false,
            };
            if found {
                return;
            }

            assert!(
                start.elapsed() < Duration::from_secs(1),
                "no persisted state object was written under {prefix}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[cfg(feature = "rate-control")]
    #[tokio::test]
    async fn registry_persistence_task_persists_all_registered_origins() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let registry = Arc::new(HttpRateControlRegistry::with_persisted_governor_state(
            Arc::clone(&store),
            "",
            Duration::from_millis(20),
        ));
        registry.start_persistence_task();
        registry.start_persistence_task();

        let dataset = test_dataset().await;
        let config = persisted_test_config();
        let first_url = Url::parse("https://first.example.com/data").expect("first URL parses");
        let second_url = Url::parse("https://second.example.com/data").expect("second URL parses");

        let first = registry
            .shared_rate_controller_for_component(
                &first_url,
                &config,
                dataset.app.name.as_str(),
                &ConnectorComponent::from(&dataset),
                "https",
            )
            .await
            .expect("first controller should build");
        let second = registry
            .shared_rate_controller_for_component(
                &second_url,
                &config,
                dataset.app.name.as_str(),
                &ConnectorComponent::from(&dataset),
                "https",
            )
            .await
            .expect("second controller should build");

        let first_permit = first
            .controller
            .as_ref()
            .expect("first controller should exist")
            .acquire()
            .await
            .expect("first permit should acquire");
        drop(first_permit);
        let second_permit = second
            .controller
            .as_ref()
            .expect("second controller should exist")
            .acquire()
            .await
            .expect("second permit should acquire");
        drop(second_permit);

        wait_for_persisted_origin(&store, dataset.app.name.as_str(), &first_url).await;
        wait_for_persisted_origin(&store, dataset.app.name.as_str(), &second_url).await;
    }

    #[tokio::test]
    async fn rolled_back_controller_reservation_allows_new_config() {
        let registry = Arc::new(HttpRateControlRegistry::default());
        let url = Url::parse("https://rate-control-registry.example.com/data")
            .expect("test URL should parse");
        let dataset = test_dataset().await;

        let reservation = Arc::clone(&registry)
            .reserve_shared_rate_controller_for_component(
                &url,
                &test_config(2),
                dataset.app.name.as_str(),
                &ConnectorComponent::from(&dataset),
                "https",
            )
            .await
            .expect("initial reservation should succeed");
        reservation.rollback().await;

        let reservation = Arc::clone(&registry)
            .reserve_shared_rate_controller_for_component(
                &url,
                &test_config(3),
                dataset.app.name.as_str(),
                &ConnectorComponent::from(&dataset),
                "https",
            )
            .await
            .expect("rolled back reservation should not leave stale config");
        let shared = reservation.commit().await;

        assert_eq!(shared.config.max_concurrent_requests, Some(3));
    }

    #[tokio::test]
    async fn committed_controller_reservation_rejects_new_config() {
        let registry = Arc::new(HttpRateControlRegistry::default());
        let url = Url::parse("https://rate-control-registry-conflict.example.com/data")
            .expect("test URL should parse");
        let dataset = test_dataset().await;

        let reservation = Arc::clone(&registry)
            .reserve_shared_rate_controller_for_component(
                &url,
                &test_config(2),
                dataset.app.name.as_str(),
                &ConnectorComponent::from(&dataset),
                "https",
            )
            .await
            .expect("initial reservation should succeed");
        reservation.commit().await;

        let error = Arc::clone(&registry)
            .reserve_shared_rate_controller_for_component(
                &url,
                &test_config(3),
                dataset.app.name.as_str(),
                &ConnectorComponent::from(&dataset),
                "https",
            )
            .await
            .expect_err("committed reservation should keep the origin config");

        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("different rate-control settings"),
                    "expected conflict message, got: {message}"
                );
            }
            other => panic!("expected rate-control conflict, got: {other}"),
        }
    }
}
