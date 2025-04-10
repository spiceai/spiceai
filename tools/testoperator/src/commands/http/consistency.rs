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
    TestType,
    anyhow::{self, anyhow},
    arrow::util::pretty::print_batches,
    metrics::MetricCollector,
    spiced::SpicedInstance,
    spicetest::{
        SpiceTest,
        http::consistency::{self, ConsistencyConfig},
    },
};

/// Runs a test to ensure the P50 & p90 latencies do not increase by some threshold over the
/// duration of the test when N clients are sending queries concurrently.
#[allow(clippy::cast_precision_loss)]
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
        consistency::NotStarted::new(ConsistencyConfig::new(
            Duration::from_secs(args.common.duration),
            args.common.concurrency,
            payloads,
            component,
            Duration::from_secs(args.warmup),
            args.buckets,
            args.common.disable_progress_bars,
        )),
    )
    .with_spiced_instance(spiced_instance);

    println!("{}", with_color!(Color::Blue, "Starting consistency test"));
    let test = test.start()?.wait().await?;
    let results = test.collect(TestType::HttpConsistency)?;

    let mut spiced_instance = test.end()?;

    let records = results.build_records()?;
    print_batches(&records)?;

    let (p50, p95): (Vec<i64>, Vec<i64>) = results
        .metrics
        .iter()
        .map(|minute| (minute.median_duration_ms, minute.percentile_95_duration_ms))
        .unzip();
    if p50.len() >= 2 {
        let increase = *p50.last().ok_or(anyhow!("no p50 data"))? as f64 / p50[0] as f64;

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
        let increase = *p95.last().ok_or(anyhow!("no p95 data"))? as f64 / p95[0] as f64;
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
