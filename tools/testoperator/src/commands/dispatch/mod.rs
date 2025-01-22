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

use std::path::PathBuf;
use test_framework::{
    anyhow::{self, Result},
    gh_utils::GitHubWorkflow,
    octocrab, TestType,
};

use crate::args::dispatch::{BenchWorkflowArgs, DispatchArgs, DispatchTestFile};

/// Recursively scan a directory for YAML files
fn scan_directory_for_yamls(path: &PathBuf) -> Result<Vec<PathBuf>> {
    let mut files = vec![];

    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            files.append(&mut scan_directory_for_yamls(&path)?);
        } else if path.is_file() && path.extension().map_or(false, |ext| ext == "yaml") {
            files.push(path);
        }
    }

    Ok(files)
}

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

            Ok::<_, anyhow::Error>(tests)
        })
        .collect::<Result<Vec<_>>>()?;

    for test in tests {
        match test_type {
            TestType::Benchmark => {
                if let Some(bench) = test.tests.bench {
                    println!("Running benchmark test: {bench:?}");
                    GitHubWorkflow::new("spiceai", "spiceai", test_type.workflow(), "trunk")
                        .send(
                            octo_client.actions(),
                            Some(serde_json::json!(BenchWorkflowArgs {
                                bench_args: bench,
                                spiced_commit: String::new(), // TODO: source spiced commit from env
                            })),
                        )
                        .await?;
                }
            }
            TestType::Throughput => {
                if let Some(throughput) = test.tests.throughput {
                    println!("Running throughput test: {throughput:?}");
                }
            }
            TestType::Load => {
                if let Some(load) = test.tests.load {
                    println!("Running load test: {load:?}");
                }
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Test type {test_type} not supported for dispatching"
                ))
            }
        }
    }

    Ok(())
}
