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

use crate::Args;
use std::time::Duration;
use test_framework::{
    anyhow, flight::query_to_batches, spiced::SpicedInstance, spicepod_utils::from_app,
};

pub(crate) async fn run(args: &Args) -> anyhow::Result<()> {
    let app = test_framework::app::AppBuilder::new("test-operator")
        // .with_dataset(Dataset::new(
        //     "github:github.com/spiceai/spiceai/issues",
        //     "spiceai.issues",
        // ))
        .build();

    let mut spiced_instance = SpicedInstance::start(&args.spiced_path, from_app(app)).await?;

    spiced_instance
        .wait_for_ready(Duration::from_secs(10))
        .await?;

    let client = spiced_instance.flight_client().await?;

    let batches = query_to_batches(&client, "SELECT 1").await?;
    println!("Batches: {batches:?}");

    // Wait for input
    println!("Press Enter to stop");
    let _ = std::io::stdin().read_line(&mut String::new());

    spiced_instance.stop()?;

    println!("Spiced instance stopped");
    Ok(())
}
