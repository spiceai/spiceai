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

use super::bench;
use crate::args::DataConsistencyArgs;
use test_framework::anyhow;

pub(crate) async fn run(args: &DataConsistencyArgs) -> anyhow::Result<()> {
    // first benchmark run using the first spicepod
    let mut test_args = args.test_args.clone();
    let first_row_counts = bench::run(&test_args).await?;

    // second benchmark run using the second spicepod
    test_args
        .common
        .spicepod_path
        .clone_from(&args.compare_spicepod);
    let second_row_counts = bench::run(&test_args).await?;

    // compare the results
    let mut test_failed = false;
    for (query, count) in &first_row_counts {
        let second_count = second_row_counts.get(query).unwrap_or(&0);
        if count != second_count {
            println!(
                "FAIL - Data consistency check failed for query '{query}': {count} != {second_count}",
            );
            test_failed = true;
        }

        if *count == 0 {
            println!(
                "WARN - No data returned for query '{query}' for {:#?}",
                args.test_args.common.spicepod_path
            );
        }

        if *second_count == 0 {
            println!(
                "WARN - No data returned for query '{query}' for {:#?}",
                args.compare_spicepod
            );
        }
    }

    if test_failed {
        return Err(anyhow::anyhow!("Data consistency check failed"));
    }

    Ok(())
}
