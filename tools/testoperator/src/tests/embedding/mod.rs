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

use crate::commands::EmbeddingTestArgs;
use std::time::Duration;
use test_framework::{
    anyhow,
    app::App,
    spiced::{SpicedInstance, StartRequest},
    spicepod::Spicepod,
    spicepod_utils::from_app,
};

fn get_embedding_app_and_start_request(
    args: &EmbeddingTestArgs,
) -> anyhow::Result<(App, StartRequest)> {
    let spicepod = Spicepod::load_exact(args.spicepod_path.clone())?;
    let app = test_framework::app::AppBuilder::new(spicepod.name.clone())
        .with_spicepod(spicepod)
        .build();

    let start_req = StartRequest::new(args.spiced_path.clone(), from_app(app.clone()))?;
    Ok((app, start_req))
}

pub(crate) async fn run(args: &EmbeddingTestArgs) -> anyhow::Result<()> {
    let (_app, start_request) = get_embedding_app_and_start_request(args)?;
    let mut spiced_instance = SpicedInstance::start(start_request).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(args.ready_wait))
        .await?;

    // let test = throughput_test.wait().await?;
    // let throughput_metric = test.get_throughput_metric(args.scale_factor.unwrap_or(1.0))?;
    // let metrics = test.collect()?;
    // let mut spiced_instance = test.end();
    // metrics.show()?;

    spiced_instance.show_memory_usage()?;
    spiced_instance.stop()?;

    println!("Embedding consistency test completed!");
    Ok(())
}
