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
    gh_utils::GitHubWorkflow,
    octocrab,
    utils::scan_directory_for_yamls,
    TestType,
};

use crate::args::dispatch::{BenchWorkflowArgs, DispatchArgs, DispatchTestFile, LoadWorkflowArgs};

pub async fn dispatch(args: DispatchArgs) -> Result<()> {
    if !args.path.is_dir() && !args.path.is_file() {
        return Err(anyhow::anyhow!("Path must be a directory or a file"));
    }

    if std::env::var("GH_TOKEN").is_err() {
        return Err(anyhow::anyhow!(
            "A GitHub token must be set in the GH_TOKEN environment variable"
        ));
    }

    let octo_client = octocrab::instance().user_access_token(std::env::var("GH_TOKEN")?)?;
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

    let spiced_commit = std::env::var("SPICED_COMMIT").ok().unwrap_or_default();

    for (path, test) in tests {
        let payload = match test_type {
            TestType::Benchmark => {
                if let Some(bench) = test.tests.bench {
                    serde_json::json!(BenchWorkflowArgs {
                        bench_args: bench,
                        spiced_commit: spiced_commit.clone(),
                    })
                } else {
                    println!("Test file {path:#?} does not contain a benchmark test");
                    continue;
                }
            }
            TestType::Throughput => {
                if let Some(throughput) = test.tests.throughput {
                    serde_json::json!(BenchWorkflowArgs {
                        bench_args: throughput,
                        spiced_commit: spiced_commit.clone(),
                    })
                } else {
                    println!("Test file {path:#?} does not contain a throughput test");
                    continue;
                }
            }
            TestType::Load => {
                if let Some(load) = test.tests.load {
                    serde_json::json!(LoadWorkflowArgs {
                        load_args: load.clone(),
                        spiced_commit: spiced_commit.clone(),
                    })
                } else {
                    println!("Test file {path:#?} does not contain a load test");
                    continue;
                }
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Test type {test_type} not supported for dispatching"
                ))
            }
        };

        println!("Dispatching {test_type} test from {path:#?}");
        GitHubWorkflow::new("spiceai", "spiceai", test_type.workflow(), "trunk")
            .send(octo_client.actions(), Some(payload))
            .await?;

        // sleep to space out runs
        println!("Waiting for next run...");
        tokio::time::sleep(std::time::Duration::from_secs(45)).await;
    }

    Ok(())
}
