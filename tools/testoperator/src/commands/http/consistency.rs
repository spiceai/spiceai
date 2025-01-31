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
    args::HttpConsistencyTestArgs,
    commands::{get_app_and_start_request, util::Color},
    with_color,
};
use std::{sync::Arc, time::Duration};
use test_framework::{
    anyhow::{self, anyhow},
    arrow::array::ArrowNativeTypeOp,
    metrics::MetricCollector,
    spiced::SpicedInstance,
    spicetest::{
        http::consistency::{self, ConsistencyConfig},
        SpiceTest,
    },
    TestType,
};

/// Runs a test to ensure the P50 & p90 latencies do not increase by some threshold over the
/// duration of the test when N clients are sending queries concurrently.
pub async fn consistency_run(args: &HttpConsistencyTestArgs) -> anyhow::Result<()> {
    let (app, start_request) = get_app_and_start_request(&args.common)?;
    let component = args.http.get_http_component()?;
    let payloads: Vec<_> = args
        .http
        .get_payloads()?
        .into_iter()
        .map(Arc::from)
        .collect();

    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.common.ready_wait))
        .await?;

    let test = SpiceTest::new(
        app.name.clone(),
        spiced_instance,
        consistency::NotStarted::new(ConsistencyConfig::new(
            Duration::from_secs(args.common.duration),
            args.common.concurrency,
            payloads,
            component,
            Duration::from_secs(args.warmup),
            args.buckets,
            args.common.disable_progress_bars,
        )),
    );

    println!("{}", with_color!(Color::Blue, "Starting consistency test"));
    let test = test.start()?.wait().await?;
    let results = test.collect(TestType::HttpConsistency)?;

    let mut spiced_instance = test.end();

    results.show_records()?;

    let (p50, p95): (Vec<f64>, Vec<f64>) = results
        .metrics
        .iter()
        .map(|minute| (minute.median_duration, minute.percentile_95_duration))
        .unzip();
    if p50.len() >= 2 {
        let increase = p50
            .last()
            .ok_or(anyhow!("no p50 data"))?
            .div_checked(p50[0])?;
        if increase > args.increase_threshold {
            return Err(anyhow::anyhow!(with_color!(
                Color::RedBold,
                "p50 increase threshold exceeded: {} > {}",
                increase,
                args.increase_threshold
            )));
        }
    }

    if p95.len() >= 2 {
        let increase = p95
            .last()
            .ok_or(anyhow!("no p95 data"))?
            .div_checked(p95[0])?;
        if increase > args.increase_threshold {
            return Err(anyhow::anyhow!(with_color!(
                Color::RedBold,
                "p95 increase threshold exceeded: {} > {}",
                increase,
                args.increase_threshold
            )));
        }
    }

    println!(
        "{}",
        with_color!(Color::Green, "Consistency test completed!")
    );
    spiced_instance.stop()?;
    Ok(())
}
