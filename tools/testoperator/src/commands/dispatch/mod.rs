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

use std::time::Duration;

use test_framework::{
    TestType,
    anyhow::{self, Result},
    gh_utils::{GitHubWorkflow, map_numbers_to_strings},
    octocrab::{self, Octocrab},
    utils::scan_directory_for_yamls,
};

use crate::args::dispatch::{DispatchArgs, DispatchTestFile, WorkflowArgs};

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
            let tests: DispatchTestFile = serde_yaml::from_reader(file)
                .map_err(|e| anyhow::anyhow!("Failed to parse {}: {e}", path.display()))?;

            Ok::<_, anyhow::Error>((path, tests))
        })
        .collect::<Result<Vec<_>>>()?;

    // Collect all test instances for the selected test type
    let mut tests_to_dispatch = Vec::new();

    for (path, test_file) in tests {
        match test_type {
            TestType::Benchmark => {
                for bench in &test_file.tests.bench {
                    tests_to_dispatch.push((
                        path,
                        serde_json::json!(WorkflowArgs {
                            specific_args: bench
                                .clone()
                                .with_update_snapshots(args.update_snapshots.into()),
                            spiced_commit: args.spiced_commit.clone(),
                        }),
                    ));
                }
            }
            TestType::Load => {
                for load in &test_file.tests.load {
                    tests_to_dispatch.push((
                        path,
                        serde_json::json!(WorkflowArgs {
                            specific_args: load.clone(),
                            spiced_commit: args.spiced_commit.clone(),
                        }),
                    ));
                }
            }
            TestType::Throughput => {
                for throughput in &test_file.tests.throughput {
                    tests_to_dispatch.push((
                        path,
                        serde_json::json!(WorkflowArgs {
                            specific_args: throughput.clone(),
                            spiced_commit: args.spiced_commit.clone(),
                        }),
                    ));
                }
            }
            TestType::HttpConsistency => {
                for consistency in &test_file.tests.http_consistency {
                    tests_to_dispatch.push((
                        path,
                        serde_json::json!(WorkflowArgs {
                            specific_args: consistency.clone(),
                            spiced_commit: args.spiced_commit.clone(),
                        }),
                    ));
                }
            }
            TestType::HttpOverhead => {
                for overhead in &test_file.tests.http_overhead {
                    tests_to_dispatch.push((
                        path,
                        serde_json::json!(WorkflowArgs {
                            specific_args: overhead.clone(),
                            spiced_commit: args.spiced_commit.clone(),
                        }),
                    ));
                }
            }
            TestType::Append => {
                for append in &test_file.tests.append {
                    tests_to_dispatch.push((
                        path,
                        serde_json::json!(WorkflowArgs {
                            specific_args: append.clone(),
                            spiced_commit: args.spiced_commit.clone(),
                        }),
                    ));
                }
            }
            _ => {
                println!("Test type {test_type} not supported for dispatching");
            }
        }
    }

    if tests_to_dispatch.is_empty() {
        println!("No tests found for test type {test_type}");
        return Ok(());
    }

    let total_tests = tests_to_dispatch.len();
    for (index, (path, mut payload)) in tests_to_dispatch.into_iter().enumerate() {
        payload = map_numbers_to_strings(payload);

        println!(
            "{}/{} - Dispatching {test_type} test from {}",
            index + 1,
            total_tests,
            path.display(),
        );

        let workflow = GitHubWorkflow::new(
            "spiceai",
            "spiceai",
            test_type.workflow(),
            &args.workflow_commit,
        );

        match args.max_concurrent {
            Some(max_concurrent) => {
                // Dispatch workflow while waiting for an available slot, limiting to max_concurrent parallel runs
                dispatch_workflow_with_concurrency(
                    workflow,
                    &octo_client,
                    Some(payload),
                    max_concurrent,
                )
                .await?;
            }
            None => {
                // Dispatch workflow without concurrency limit
                workflow.send(octo_client.actions(), Some(payload)).await?;
            }
        }

        // sleep to space out runs
        println!("Waiting for next run...");
        tokio::time::sleep(std::time::Duration::from_secs(80)).await;
    }

    Ok(())
}

/// Dispatches the workflow, waiting until the number of active runs is below the limit
/// or until the 30 minutes max wait time expires.
///
/// - `max_concurrent`: maximum number of active runs allowed
async fn dispatch_workflow_with_concurrency(
    workflow: GitHubWorkflow,
    octo: &Octocrab,
    input: Option<serde_json::Value>,
    max_concurrent: usize,
) -> Result<()> {
    println!(
        "Checking for available slot to run workflow (limit: {max_concurrent} concurrent runs)..."
    );
    if let Err(err) = wait_for_slot(
        &workflow,
        octo,
        max_concurrent,
        Duration::from_secs(1800), // 30 mins
    )
    .await
    {
        eprintln!("Error waiting for slot: {err}");
    }

    workflow.send(octo.actions(), input).await
}

/// Waits until the number of already queued runs is below the given limit,
/// or until the timeout expires.
///
/// This is used to limit the number of concurrent workflow runs on GitHub Actions.
/// - `max_concurrent`: maximum number of active runs allowed
async fn wait_for_slot(
    workflow: &GitHubWorkflow,
    octo: &Octocrab,
    max_concurrent: usize,
    timeout: Duration,
) -> Result<()> {
    let start_time = std::time::Instant::now();

    loop {
        let num_active = workflow.active_runs_count(octo).await?;
        if num_active < max_concurrent {
            println!("✅ Dispatch slot available! Currently {num_active} active runs.");
            break;
        }

        // Check if we've exceeded the maximum wait time
        if start_time.elapsed() >= timeout {
            return Err(anyhow::anyhow!(
                "Timeout: waited {} seconds for available slot but {num_active} runs still active",
                timeout.as_secs()
            ));
        }

        let remaining_time = timeout.saturating_sub(start_time.elapsed());
        println!(
            "🕒 {num_active} run(s) already active — waiting for slot... ({} seconds remaining)",
            remaining_time.as_secs()
        );

        tokio::time::sleep(Duration::from_secs(30)).await;
    }
    Ok(())
}
