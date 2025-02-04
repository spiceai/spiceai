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
    args::HttpOverheadTestArgs,
    commands::{get_app_and_start_request, util::Color},
    with_color,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::{sync::Arc, time::Duration};
use test_framework::{
    anyhow::{self, anyhow},
    arrow::array::ArrowNativeTypeOp,
    metrics::MetricCollector,
    spiced::SpicedInstance,
    spicetest::{
        http::{
            component::HttpComponent,
            overhead::{self, BaselineConfig},
            HttpConfig,
        },
        SpiceTest,
    },
    TestType,
};

/// Runs a test to ensure the P50 & p90 latencies do not increase by some threshold over the
/// duration of the test when N clients are sending queries concurrently.
pub(crate) async fn overhead_run(args: &HttpOverheadTestArgs) -> anyhow::Result<()> {
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

    let baseline_cfg = construct_baseline_cfg(args, &component, &payloads)?;

    let test = SpiceTest::new(
        app.name.clone(),
        spiced_instance,
        overhead::NotStarted::new(
            HttpConfig {
                duration: Duration::from_secs(args.common.duration),
                concurrency: args.common.concurrency,
                payloads,
                component,
                warmup: Duration::from_secs(0),
                disable_progress_bars: args.common.disable_progress_bars,
            },
            baseline_cfg,
        ),
    );

    println!("{}", with_color!(Color::Blue, "Starting overhead test"));
    let test = test.start()?.wait().await?;
    let results = test.collect(TestType::HttpOverhead)?;
    results.show_records()?;

    let mut spiced_instance = test.end();
    spiced_instance.stop()?;

    let Some(baseline) = results.metrics.iter().find(|q| q.query_name == "baseline") else {
        return Err(anyhow::anyhow!("Baseline results not found"));
    };
    let Some(spice) = results.metrics.iter().find(|q| q.query_name == "spice") else {
        return Err(anyhow::anyhow!("Spice results not found"));
    };

    check_threshold(
        spice.median_duration,
        baseline.median_duration,
        args.increase_threshold,
        "median",
    )?;
    check_threshold(
        spice.percentile_90_duration,
        baseline.percentile_90_duration,
        args.increase_threshold,
        "p90",
    )?;
    check_threshold(
        spice.percentile_95_duration,
        baseline.percentile_95_duration,
        args.increase_threshold,
        "p95",
    )?;

    Ok(())
}

/// Ensure that the relative increase in the spice duration compared to the baseline duration is less than the threshold.
/// Example
/// ```
/// check_threshold(10.0, 5.0, 1.1, "p50") // Ok
/// check_threshold(12.0, 5.0, 1.1, "p50") // Err
/// ```
fn check_threshold(
    spice_duration: f64,
    baseline_duration: f64,
    increase_threshold: f64,
    label: &str,
) -> Result<(), anyhow::Error> {
    if let Ok(multiple) = spice_duration.div_checked(baseline_duration) {
        if multiple > increase_threshold {
            return Err(anyhow::anyhow!(with_color!(Color::RedBold,
                "Spice {} duration ({}) increased beyond baseline ({}) by more than the threshold ({} > {})",
                label,
                spice_duration,
                baseline_duration,
                multiple,
                increase_threshold
            )));
        }
        println!("{}", with_color!(Color::Green,
                "Spice {} duration ({}) increased beyond baseline ({}) by less than the threshold ({} <= {})",
                label,
                spice_duration,
                baseline_duration,
                multiple,
                increase_threshold)
            );
    }
    Ok(())
}

fn construct_baseline_cfg(
    args: &HttpOverheadTestArgs,
    spice_component: &HttpComponent,
    spice_payloads: &[Arc<str>],
) -> anyhow::Result<BaselineConfig> {
    let baseline_component_name = args
        .base_component
        .clone()
        .unwrap_or(spice_component.component_name().clone());

    let base_payloads: Option<Vec<_>> = args
        .base_payload()?
        .map(|p| p.into_iter().map(Arc::from).collect());

    // No payloads implies Http component is OpenAI compatible. Can use `HttpComponent`.
    let base_component = if base_payloads.is_none() {
        spice_component
            .clone()
            .with_api_base(args.base_url.clone())
            .with_component_name(baseline_component_name)
    } else {
        HttpComponent::Generic {
            http_url: args.base_url.clone(),
            component_name: baseline_component_name,
        }
    };

    let baseline_client = if let Some(headers) = args.base_header.clone() {
        reqwest::Client::builder()
            .default_headers(parse_headers(&headers)?)
            .build()
            .map_err(|_| anyhow!("Invalid headers in `--base-headers`."))?
    } else {
        reqwest::Client::new()
    };
    Ok(BaselineConfig::new(
        base_component,
        baseline_client,
        base_payloads.unwrap_or(spice_payloads.to_vec()),
    ))
}

/// Parse headers from a list of strings (with format: `Key: Value`) into a `HeaderMap`.
fn parse_headers(headers: &[String]) -> Result<HeaderMap, anyhow::Error> {
    let mut header_map = HeaderMap::new();
    for h in headers {
        if let Some((key, value)) = h.split_once(':') {
            let key = key.trim().to_string();
            let value = value.trim().to_string();

            let header_name = key
                .parse::<HeaderName>()
                .map_err(|_| anyhow!("Invalid header key: {}", key))?;
            let header_value = HeaderValue::from_str(&value)
                .map_err(|_| anyhow!("Invalid header value for key: {}", key))?;

            header_map.insert(header_name, header_value);
        } else {
            return Err(anyhow!(
                "Invalid header format: {}. Expected format: 'Key: Value'",
                h
            ));
        }
    }
    Ok(header_map)
}
