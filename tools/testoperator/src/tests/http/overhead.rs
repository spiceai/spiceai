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

use crate::{
    commands::HttpOverheadTestArgs,
    tests::{
        get_app_and_start_request,
        http::{get_http_component, get_payloads},
    },
};
use std::{sync::Arc, time::Duration};
use test_framework::{
    anyhow,
    arrow::array::ArrowNativeTypeOp,
    spiced::SpicedInstance,
    spicetest::{ConsistencyConfig, ConsistencySpiceTest},
};

/// Runs a test to ensure the P50 & p90 latencies do not increase by some threshold over the
/// duration of the test when N clients are sending queries concurrently.
pub(crate) async fn overhead_run(args: &HttpOverheadTestArgs) -> anyhow::Result<()> {
    let (_app, start_request) = get_app_and_start_request(&args.http.common, None)?;
    let component = get_http_component(&args.http)?;
    let payloads: Vec<_> = get_payloads(&args.http)?
        .into_iter()
        .map(Arc::from)
        .collect();

    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.http.common.ready_wait))
        .await?;

    Ok(())
}
