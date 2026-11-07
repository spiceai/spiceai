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

//! A `RustFS` container: the local S3-compatible object store the integration tests
//! use in place of AWS S3.

use std::time::Duration;

use aws_sdk_s3::config::{Credentials, Region};
use bollard::secret::HealthConfig;

use crate::docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port};

pub(crate) const S3_ACCESS_KEY: &str = "spiceadmin";
pub(crate) const S3_SECRET_KEY: &str = "spiceintegrationsecret";
pub(crate) const S3_BUCKET: &str = "lake";

/// A `RustFS` object store in a container, healthy and reachable on its published port.
pub(crate) async fn start_object_store(name: &str) -> anyhow::Result<RunningContainer> {
    let container = ContainerRunnerBuilder::new(name)
        .image("rustfs/rustfs:latest".to_string())
        .publish_port(9000)
        .add_env_var("RUSTFS_ACCESS_KEY", S3_ACCESS_KEY)
        .add_env_var("RUSTFS_SECRET_KEY", S3_SECRET_KEY)
        .command(["/data"])
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "netstat -tulpn | grep 9000 || exit 1".to_string(),
            ]),
            interval: Some(500_000_000),
            timeout: Some(1_000_000_000),
            retries: Some(20),
            start_period: Some(2_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(Some(Duration::from_mins(2)))
        .await?;
    wait_for_tcp_port(
        "127.0.0.1",
        container.host_port(9000)?,
        Duration::from_mins(1),
    )
    .await?;
    Ok(container)
}

pub(crate) fn s3_client(endpoint: &str) -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .credentials_provider(Credentials::new(
                S3_ACCESS_KEY,
                S3_SECRET_KEY,
                None,
                None,
                "test",
            ))
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .behavior_version_latest()
            .build(),
    )
}

pub(crate) async fn create_bucket(endpoint: &str) -> anyhow::Result<()> {
    let s3 = s3_client(endpoint);
    // The store can accept connections a moment before it accepts requests.
    let mut last_error = None;
    for _ in 0..50 {
        match s3.create_bucket().bucket(S3_BUCKET).send().await {
            Ok(_) => return Ok(()),
            Err(e) => last_error = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("could not create bucket {S3_BUCKET}: {last_error:?}")
}
