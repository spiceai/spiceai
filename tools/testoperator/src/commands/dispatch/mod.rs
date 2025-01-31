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

use test_framework::{
    anyhow::{self, Result},
    gh_utils::{map_numbers_to_strings, GitHubWorkflow},
    octocrab,
    utils::scan_directory_for_yamls,
    TestType,
};

use crate::args::dispatch::{DispatchArgs, DispatchTestFile, DispatchTests, WorkflowArgs};

#[allow(clippy::too_many_lines)]
pub async fn dispatch(args: DispatchArgs) -> Result<()> {
    if !args.path.is_dir() && !args.path.is_file() {
        return Err(anyhow::anyhow!("Path must be a directory or a file"));
    }

    let octo_client = octocrab::instance().user_access_token(args.github_token)?;
    let test_type: TestType = args.workflow.into();
    let yaml_files = if args.path.is_dir() {
        scan_directory_for_yamls(&args.path)?
    } else {
        vec![args.path]
    };

    println!("Found {} YAML files to load", yaml_files.len());

    let tests = yaml_files
        .iter()
        .map(|path| {
            let file = std::fs::File::open(path)?;
            let tests: DispatchTestFile = serde_yaml::from_reader(file)?;

            Ok::<_, anyhow::Error>((path, tests))
        })
        .collect::<Result<Vec<_>>>()?;

    for (path, test) in tests {
        let mut payload = match (test_type, &test.tests) {
            (
                TestType::Benchmark,
                DispatchTests {
                    bench: Some(bench), ..
                },
            ) => {
                serde_json::json!(WorkflowArgs {
                    specific_args: bench.clone(),
                    spiced_commit: args.spiced_commit.clone(),
                })
            }
            (
                TestType::Throughput,
                DispatchTests {
                    throughput: Some(throughput),
                    ..
                },
            ) => {
                serde_json::json!(WorkflowArgs {
                    specific_args: throughput.clone(),
                    spiced_commit: args.spiced_commit.clone(),
                })
            }
            (
                TestType::Load,
                DispatchTests {
                    load: Some(load), ..
                },
            ) => {
                serde_json::json!(WorkflowArgs {
                    specific_args: load.clone(),
                    spiced_commit: args.spiced_commit.clone(),
                })
            }
            (
                TestType::HttpConsistency,
                DispatchTests {
                    http_consistency: Some(consistency),
                    ..
                },
            ) => {
                serde_json::json!(WorkflowArgs {
                    specific_args: consistency,
                    spiced_commit: args.spiced_commit.clone(),
                })
            }
            (
                TestType::HttpOverhead,
                DispatchTests {
                    http_overhead: Some(overhead),
                    ..
                },
            ) => {
                serde_json::json!(WorkflowArgs {
                    specific_args: overhead,
                    spiced_commit: args.spiced_commit.clone(),
                })
            }
            (TestType::Benchmark, _) => {
                println!("Test file {path:#?} does not contain a benchmark test");
                continue;
            }
            (TestType::Throughput, _) => {
                println!("Test file {path:#?} does not contain a throughput test");
                continue;
            }
            (TestType::Load, _) => {
                println!("Test file {path:#?} does not contain a load test");
                continue;
            }
            (TestType::HttpConsistency, _) => {
                println!("Test file {path:#?} does not contain an HTTP consistency test");
                continue;
            }
            (TestType::HttpOverhead, _) => {
                println!("Test file {path:#?} does not contain an HTTP overhead test");
                continue;
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Test type {test_type} not supported for dispatching"
                ))
            }
        };

        payload = map_numbers_to_strings(payload);

        println!("Dispatching {test_type} test from {path:#?}");
        GitHubWorkflow::new(
            "spiceai",
            "spiceai",
            test_type.workflow(),
            &args.workflow_commit,
        )
        .send(octo_client.actions(), Some(payload))
        .await?;

        // sleep to space out runs
        println!("Waiting for next run...");
        tokio::time::sleep(std::time::Duration::from_secs(45)).await;
    }

    Ok(())
}
