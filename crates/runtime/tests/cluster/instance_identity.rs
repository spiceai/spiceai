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

use std::time::Duration;

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use super::harness::ClusterHarness;

/// Executors register under their instance identity, and the scheduler reaches
/// each one at its advertise address.
#[tokio::test(flavor = "multi_thread")]
async fn executors_register_under_their_instance_identity() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            configure_test_datafusion();
            let harness = ClusterHarness::builder().executors(2).start().await?;
            let result = async {
                harness.wait_for_executors(Duration::from_secs(30)).await?;
                let expected: Vec<(String, String)> = harness
                    .executors
                    .iter()
                    .enumerate()
                    .map(|(i, rt)| {
                        (
                            format!("executor{i}"),
                            format!(
                                "127.0.0.1:{}",
                                rt.datafusion().cluster_config.node_bind_address().port()
                            ),
                        )
                    })
                    .collect();
                assert_eq!(harness.registered_executors().await?, expected);
                Ok::<(), anyhow::Error>(())
            }
            .await;
            harness.shutdown().await;
            result
        })
        .await
}
