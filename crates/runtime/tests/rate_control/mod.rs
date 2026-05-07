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

use std::{num::NonZeroU32, path::Path, sync::Arc, time::Duration};

use app::{App, AppBuilder};
use runtime::{
    Runtime,
    component::dataset::{Dataset, builder::DatasetBuilder},
    dataconnector::http_rate_control::HttpRateControlConfig,
};
use spicepod::component::runtime::{Runtime as SpicepodRuntime, SourceRateControl};
use url::Url;

const APP_NAME: &str = "rate_control_file_state_global";
const ORIGIN_URL: &str = "https://rate-control-file-state.example.com/data";

fn app_with_file_rate_control(state_location: &str, refresh_interval: &str) -> App {
    AppBuilder::new(APP_NAME)
        .with_runtime(SpicepodRuntime {
            source_rate_control: Some(SourceRateControl {
                state_location: Some(state_location.to_string()),
                params: None,
                refresh_interval: refresh_interval.to_string(),
                github_concurrent_connections_limit: None,
            }),
            ..Default::default()
        })
        .build()
}

fn dataset_for_runtime(app: &App, runtime: &Arc<Runtime>) -> Dataset {
    DatasetBuilder::try_new(ORIGIN_URL.to_string(), "rate_control_file_state")
        .expect("dataset builder should be valid")
        .with_app(Arc::new(app.clone()))
        .with_runtime(Arc::clone(runtime))
        .build()
        .expect("dataset should build")
}

fn global_rate_control_config() -> HttpRateControlConfig {
    HttpRateControlConfig {
        max_concurrent_requests: None,
        requests_per_second: None,
        requests_per_minute: Some(
            NonZeroU32::new(1).expect("test requests-per-minute should be non-zero"),
        ),
        jitter_min: Duration::ZERO,
        jitter_max: Duration::ZERO,
    }
}

fn contains_persisted_limiter_state(path: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(path) else {
        return false;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if contains_persisted_limiter_state(&path) {
                return true;
            }
        } else if path
            .extension()
            .is_some_and(|extension| extension == "json")
            && std::fs::read_to_string(&path)
                .ok()
                .and_then(|contents| serde_json::from_str::<serde_json::Value>(&contents).ok())
                .and_then(|value| value.get("limiters").cloned())
                .and_then(|limiters| limiters.as_object().map(serde_json::Map::len))
                .is_some_and(|len| len > 0)
        {
            return true;
        }
    }

    false
}

fn max_persisted_instance_count(path: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };

    let mut max_count = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            max_count = max_count.max(max_persisted_instance_count(&path));
        } else if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            let instance_count = std::fs::read_to_string(&path)
                .ok()
                .and_then(|contents| serde_json::from_str::<serde_json::Value>(&contents).ok())
                .and_then(|value| value.get("instances").cloned())
                .and_then(|instances| instances.as_object().map(serde_json::Map::len))
                .unwrap_or_default();
            max_count = max_count.max(instance_count);
        }
    }

    max_count
}

async fn wait_for_persisted_file_state(state_dir: &Path) {
    let start = tokio::time::Instant::now();
    loop {
        if contains_persisted_limiter_state(state_dir) {
            return;
        }

        assert!(
            start.elapsed() < Duration::from_secs(2),
            "file-backed rate-control state was not persisted"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_persisted_instance_count(state_dir: &Path, expected_count: usize) {
    let start = tokio::time::Instant::now();
    loop {
        let instance_count = max_persisted_instance_count(state_dir);
        if instance_count >= expected_count {
            return;
        }

        assert!(
            start.elapsed() < Duration::from_secs(2),
            "file-backed rate-control state persisted {instance_count} instances, expected at least {expected_count}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn file_state_location_shares_global_http_rate_control_state() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let state_dir = temp_dir.path().join("rate-control-state");
    let state_location = Url::from_directory_path(&state_dir)
        .expect("state dir should convert to file URL")
        .to_string();
    let origin_url = Url::parse(ORIGIN_URL).expect("origin URL should parse");
    let config = global_rate_control_config();

    let first_app = app_with_file_rate_control(&state_location, "20ms");
    let first_runtime = Arc::new(Runtime::builder().with_app(first_app.clone()).build().await);
    let first_dataset = dataset_for_runtime(&first_app, &first_runtime);
    let first_shared = first_runtime
        .http_rate_control_registry()
        .shared_rate_controller(&origin_url, &config, &first_dataset, "https")
        .await
        .expect("first shared controller should build");
    let first_controller = first_shared
        .controller
        .expect("first rate controller should be enabled");

    let first_permit = first_controller
        .acquire()
        .await
        .expect("first request should acquire immediately");
    drop(first_permit);

    wait_for_persisted_file_state(&state_dir).await;

    let second_app = app_with_file_rate_control(&state_location, "30s");
    let second_runtime = Arc::new(
        Runtime::builder()
            .with_app(second_app.clone())
            .build()
            .await,
    );
    let second_dataset = dataset_for_runtime(&second_app, &second_runtime);
    let second_shared = second_runtime
        .http_rate_control_registry()
        .shared_rate_controller(&origin_url, &config, &second_dataset, "https")
        .await
        .expect("second shared controller should build");
    let second_controller = second_shared
        .controller
        .expect("second rate controller should be enabled");

    wait_for_persisted_instance_count(&state_dir, 2).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::select! {
        acquired = second_controller.acquire() => {
            panic!("second runtime should honor file-backed global rate-control state, got: {acquired:?}");
        }
        () = tokio::time::sleep(Duration::from_millis(250)) => {}
    }
}
