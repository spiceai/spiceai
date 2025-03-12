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

use std::collections::BTreeMap;

use crate::args::CommonArgs;
use test_framework::{
    anyhow,
    app::App,
    spiced::StartRequest,
    spicepod::Spicepod,
    spicepod_utils::{from_app, make_spiceai_rw_dataset, set_read_write_api_key},
};

pub(crate) mod append;
pub(crate) mod bench;
pub(crate) mod data_consistency;
pub(crate) mod dispatch;
pub(crate) mod evals;
pub(crate) mod http;
pub(crate) mod load;
pub(crate) mod throughput;
mod util;
pub(crate) mod vector_search;
pub(crate) type RowCounts = BTreeMap<String, usize>;

const TEST_RESULTS_DATASET: &str = "test_results";
const TEST_RESULTS_API_KEY: &str = "test_results_api_key";

pub(crate) fn get_app_and_start_request(args: &CommonArgs) -> anyhow::Result<(App, StartRequest)> {
    let spicepod = Spicepod::load_exact(args.spicepod_path.clone())?;
    let mut app = test_framework::app::AppBuilder::new(spicepod.name.clone())
        .with_spicepod(spicepod)
        .build();

    if let Some(upload_results_dataset) = &args.upload_results_dataset {
        println!("UPLOAD_RESULTS_DATASET: {upload_results_dataset}");
        app.datasets.push(make_spiceai_rw_dataset(
            upload_results_dataset,
            TEST_RESULTS_DATASET,
            None,
        ));
        set_read_write_api_key(&mut app.runtime, TEST_RESULTS_API_KEY.to_string());
    }

    let start_request = StartRequest::new(args.spiced_path.clone(), from_app(app.clone()))?;
    let start_request = if let Some(ref data_dir) = args.data_dir {
        start_request.with_data_dir(data_dir.clone())
    } else {
        start_request
    };

    Ok((app, start_request))
}

pub(crate) fn env_export(args: &CommonArgs) -> anyhow::Result<()> {
    let (_, mut start_request) = get_app_and_start_request(args)?;

    start_request.prepare()?;
    let tempdir_path = start_request.get_tempdir_path();

    println!(
        "Exported spicepod environment to: {}",
        tempdir_path.to_string_lossy()
    );

    // Wait for input before exiting
    println!("Press Enter to exit...");
    std::io::stdin().read_line(&mut String::new())?;

    Ok(())
}
