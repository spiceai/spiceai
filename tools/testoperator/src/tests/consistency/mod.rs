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

use crate::commands::ConsistencyTestArgs;
use std::time::Duration;
use test_framework::{
    anyhow,
    app::App,
    arrow::array::ArrowNativeTypeOp,
    spiced::{SpicedInstance, StartRequest},
    spicepod::Spicepod,
    spicepod_utils::from_app,
    spicetest::{ConsistencyComponent, ConsistencyConfig, ConsistencySpiceTest},
};

const DEFAULT_API_BASE: &str = "http://localhost:8090/v1";

fn get_consistency_app_and_start_request(
    args: &ConsistencyTestArgs,
) -> anyhow::Result<(App, StartRequest)> {
    let spicepod = Spicepod::load_exact(args.spicepod_path.clone())?;
    let app = test_framework::app::AppBuilder::new(spicepod.name.clone())
        .with_spicepod(spicepod)
        .build();

    let start_req = StartRequest::new(args.spiced_path.clone(), from_app(app.clone()))?;
    Ok((app, start_req))
}

/// Runs a test to ensure the P50 & p90 latencies do not increase by some threshold over the
/// duration of the test when N clients are sending queries concurrently.
pub(crate) async fn run(args: &ConsistencyTestArgs) -> anyhow::Result<()> {
    let (_app, start_request) = get_consistency_app_and_start_request(args)?;
    let component = match (&args.model, &args.embedding) {
        (Some(_), Some(_)) => {
            return Err(anyhow::anyhow!(
                "Cannot specify both --model and --embedding"
            ));
        }
        (None, None) => {
            return Err(anyhow::anyhow!(
                "Must specify either --model or --embedding"
            ));
        }
        (Some(model), None) => ConsistencyComponent::Model {
            model: model.clone(),
            api_base: DEFAULT_API_BASE.to_string(),
        },
        (None, Some(embedding)) => ConsistencyComponent::Embedding {
            embedding: embedding.clone(),
            api_base: DEFAULT_API_BASE.to_string(),
        },
    };

    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.ready_wait))
        .await?;

    let test = ConsistencySpiceTest::new(
        spiced_instance,
        ConsistencyConfig {
            duration: Duration::from_secs(args.duration),
            buckets: args.buckets,
            concurrency: args.concurrency,
            component,
        },
    );
    let results = test.start().await?.wait().await?.get_result()?;

    let (p50, p90): (Vec<f64>, Vec<f64>) = results
        .iter()
        .map(|minute| (minute.median_duration, minute.percentile_90_duration))
        .unzip();

    if p50.len() >= 2 {
        let increase = p50.last().expect("no p50 data").div_checked(p50[0])?;
        if increase > args.increase_threshold {
            return Err(anyhow::anyhow!(
                "p50 increase threshold exceeded: {} > {}",
                increase,
                args.increase_threshold
            ));
        }
    }

    if p90.len() >= 2 {
        let increase = p90.last().expect("no p90 data").div_checked(p90[0])?;
        if increase > args.increase_threshold {
            return Err(anyhow::anyhow!(
                "p90 increase threshold exceeded: {} > {}",
                increase,
                args.increase_threshold
            ));
        }
    }

    println!("Consistency test completed!");
    Ok(())
}
