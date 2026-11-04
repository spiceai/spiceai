/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Cold-start test: start `spiced` with an empty data directory, time how long
//! its accelerations take to load and report ready, and fail on a regression.
//!
//! Two checks guard the result. A work bound (`--max-full-compactions`) catches
//! a load that rewrites its own rows, independent of how fast the runner is. A
//! relative bound (`--max-ready-ratio`) compares the candidate's median and
//! slowest time to ready against a baseline binary run on the same host in the
//! same job, which tolerates noisy shared runners where an absolute threshold
//! would not.

use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::array::{Array, Int64Array};
use datafusion::common::TableReference;
use serde::Serialize;
use test_framework::{
    anyhow,
    app::App,
    flight::query_to_batches,
    git,
    spiced::{SpicedInstance, StartRequest},
    spicepod_utils::from_app,
};

use crate::args::ColdStartArgs;

/// `/metrics` address of the `spiced` under test. Runs are sequential, so one
/// fixed address never has two listeners.
const METRICS_ADDR: &str = "127.0.0.1:9095";

#[derive(Debug, Serialize)]
struct DatasetResult {
    name: String,
    initial_load_ms: f64,
    full_compactions: u64,
    rows: i64,
}

#[derive(Debug, Serialize)]
struct RunResult {
    ready_ms: u64,
    datasets: Vec<DatasetResult>,
}

#[derive(Debug, Serialize)]
struct BinaryResult {
    spiced_path: PathBuf,
    version: String,
    runs: Vec<RunResult>,
}

impl BinaryResult {
    fn median_ready_ms(&self) -> u64 {
        median(self.runs.iter().map(|run| run.ready_ms).collect())
    }

    fn slowest_ready_ms(&self) -> u64 {
        self.runs
            .iter()
            .map(|run| run.ready_ms)
            .max()
            .unwrap_or_default()
    }
}

#[derive(Debug, Serialize)]
struct ColdStartResults {
    spicepod: PathBuf,
    /// The candidate's build commit (`SPICED_COMMIT`), which its version string omits.
    spiced_commit: String,
    testoperator_commit: String,
    cpu_cores: String,
    candidate: BinaryResult,
    baseline: Option<BinaryResult>,
    failures: Vec<String>,
}

pub(crate) async fn run(args: &ColdStartArgs) -> anyhow::Result<()> {
    anyhow::ensure!(args.runs > 0, "--runs must be at least 1");
    anyhow::ensure!(
        args.max_ready_ratio.is_finite() && args.max_ready_ratio > 0.0,
        "--max-ready-ratio must be a positive finite number"
    );
    let app = super::load_app(&args.common).await?;
    let datasets: Vec<String> = app
        .datasets
        .iter()
        .filter(|ds| ds.acceleration.as_ref().is_some_and(|accel| accel.enabled))
        .map(|ds| ds.name.clone())
        .collect();
    anyhow::ensure!(
        !datasets.is_empty(),
        "spicepod {} has no accelerated dataset to load",
        args.common.spicepod_path.display()
    );

    let mut candidate = BinaryResult {
        spiced_path: executable_path(&args.common.spiced_path_buf())?,
        version: String::new(),
        runs: Vec::new(),
    };
    let mut baseline = args
        .baseline_spiced_path
        .as_deref()
        .map(|path| {
            Ok::<_, anyhow::Error>(BinaryResult {
                spiced_path: executable_path(path)?,
                version: String::new(),
                runs: Vec::new(),
            })
        })
        .transpose()?;

    // Interleave the two binaries and swap which goes first in every other pair,
    // so steady drift in the host's load during the job favours neither.
    for run in 1..=args.runs {
        let candidate_first = run % 2 == 0;
        if candidate_first {
            record_cold_start(args, &app, &mut candidate, &datasets, run, "candidate").await?;
        }
        if let Some(baseline) = baseline.as_mut() {
            record_cold_start(args, &app, baseline, &datasets, run, "baseline").await?;
        }
        if !candidate_first {
            record_cold_start(args, &app, &mut candidate, &datasets, run, "candidate").await?;
        }
    }

    let failures = evaluate(args, &candidate, baseline.as_ref());
    let results = ColdStartResults {
        spicepod: args.common.spicepod_path.clone(),
        spiced_commit: std::env::var("SPICED_COMMIT").unwrap_or_else(|_| "unknown".to_string()),
        testoperator_commit: git::get_commit_sha(),
        cpu_cores: args.cpu_cores.clone(),
        candidate,
        baseline,
        failures,
    };

    let summary = markdown_summary(&results);
    println!("{summary}");
    if let Some(path) = &args.summary_out {
        append_file(path, &summary)?;
    }
    if let Some(path) = &args.results_out {
        std::fs::write(path, serde_json::to_vec_pretty(&results)?)?;
    }

    anyhow::ensure!(
        results.failures.is_empty(),
        "Cold-start test failed: {}",
        results.failures.join("; ")
    );
    Ok(())
}

async fn record_cold_start(
    args: &ColdStartArgs,
    app: &App,
    binary: &mut BinaryResult,
    datasets: &[String],
    run: usize,
    label: &str,
) -> anyhow::Result<()> {
    println!("Cold start {run}/{} ({label})", args.runs);
    let (version, result) = cold_start(args, app, &binary.spiced_path, datasets).await?;
    binary.version = version;
    binary.runs.push(result);
    Ok(())
}

/// Start one `spiced` with an empty data directory and measure it until ready.
async fn cold_start(
    args: &ColdStartArgs,
    app: &App,
    spiced_path: &Path,
    datasets: &[String],
) -> anyhow::Result<(String, RunResult)> {
    let start_request = StartRequest::new(spiced_path.to_path_buf(), from_app(app.clone()))?
        .with_additional_args(vec![
            "--cpu-cores".to_string(),
            args.cpu_cores.clone(),
            "--metrics".to_string(),
            METRICS_ADDR.to_string(),
        ]);
    let mut instance = SpicedInstance::start(start_request).await?;
    let launched = Instant::now();
    let measured = measure(args, &instance, launched, datasets).await;
    let version = instance.version().to_string();
    instance.stop()?;
    Ok((version, measured?))
}

async fn measure(
    args: &ColdStartArgs,
    instance: &SpicedInstance,
    launched: Instant,
    datasets: &[String],
) -> anyhow::Result<RunResult> {
    let timeout = Duration::from_secs(args.common.ready_wait);
    while !instance.is_ready().await {
        anyhow::ensure!(
            launched.elapsed() < timeout,
            "spiced did not report ready within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let ready_ms = u64::try_from(launched.elapsed().as_millis())?;

    let metrics = reqwest::get(format!("http://{METRICS_ADDR}/metrics"))
        .await?
        .error_for_status()?
        .text()
        .await?;
    let spice_client = Arc::new(instance.spice_client(None, true).await?);
    let mut results = Vec::with_capacity(datasets.len());
    for name in datasets {
        let batches = query_to_batches(Arc::clone(&spice_client), &count_sql(name), None).await?;
        let rows = batches
            .first()
            .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
            .filter(|counts| counts.len() == 1)
            .map(|counts| counts.value(0))
            .ok_or_else(|| anyhow::anyhow!("COUNT(*) on dataset '{name}' returned no count"))?;
        // Metric labels carry the name as the runtime registers it, e.g.
        // `Sales.Orders` as `sales.orders`.
        let label = TableReference::parse_str(name).to_string();
        // Every accelerated dataset records its initial load, so a missing
        // series means the labels did not match; without this check the
        // compaction count below would read as zero instead of failing.
        let initial_load_ms = metric_sum(
            &metrics,
            "dataset_acceleration_refresh_duration_ms_sum",
            &[("dataset", &label)],
        )
        .ok_or_else(|| {
            anyhow::anyhow!(
                "spiced reported no initial load for dataset '{name}' (label '{label}')"
            )
        })?;
        results.push(DatasetResult {
            name: name.clone(),
            initial_load_ms,
            full_compactions: counter(metric_sum(
                &metrics,
                "cayenne_compaction_outcome_total",
                &[
                    ("table", &label),
                    ("kind", "full"),
                    ("outcome", "committed"),
                ],
            )),
            rows,
        });
    }
    Ok(RunResult {
        ready_ms,
        datasets: results,
    })
}

fn evaluate(
    args: &ColdStartArgs,
    candidate: &BinaryResult,
    baseline: Option<&BinaryResult>,
) -> Vec<String> {
    let mut failures = Vec::new();

    // Every run loads the same source, so every run must hold the same rows.
    let all_runs = candidate
        .runs
        .iter()
        .chain(baseline.iter().flat_map(|b| b.runs.iter()));
    let mut expected_rows: Vec<(&str, i64)> = Vec::new();
    for run in all_runs {
        for dataset in &run.datasets {
            match expected_rows.iter().find(|(name, _)| *name == dataset.name) {
                None => expected_rows.push((&dataset.name, dataset.rows)),
                Some((_, rows)) if *rows != dataset.rows => failures.push(format!(
                    "dataset '{}' loaded {} rows in one run and {rows} in another",
                    dataset.name, dataset.rows
                )),
                Some(_) => {}
            }
        }
    }

    if let Some(max) = args.max_full_compactions {
        for (index, run) in candidate.runs.iter().enumerate() {
            for dataset in run.datasets.iter().filter(|d| d.full_compactions > max) {
                failures.push(format!(
                    "candidate run {} committed {} full compactions of dataset '{}' before ready (max {max})",
                    index + 1,
                    dataset.full_compactions,
                    dataset.name
                ));
            }
        }
    }

    // The median tolerates one noisy start; the slowest start catches a
    // regression that only some cold starts hit.
    if let Some(baseline) = baseline {
        for (statistic, candidate_ms, baseline_ms) in [
            (
                "median",
                candidate.median_ready_ms(),
                baseline.median_ready_ms(),
            ),
            (
                "slowest",
                candidate.slowest_ready_ms(),
                baseline.slowest_ready_ms(),
            ),
        ] {
            let baseline_ms = baseline_ms.max(1);
            #[expect(
                clippy::cast_precision_loss,
                reason = "millisecond durations are far below 2^52"
            )]
            let ratio = candidate_ms as f64 / baseline_ms as f64;
            if ratio > args.max_ready_ratio {
                failures.push(format!(
                    "{statistic} time to ready {candidate_ms} ms is {ratio:.2}x the baseline's {baseline_ms} ms (max {:.2}x)",
                    args.max_ready_ratio
                ));
            }
        }
    }

    failures
}

/// `spiced` is launched from a temporary directory, so a relative path to it is
/// made absolute here; a bare name is left to the `PATH` lookup.
fn executable_path(path: &Path) -> anyhow::Result<PathBuf> {
    if path.is_relative() && path.components().count() > 1 {
        Ok(std::fs::canonicalize(path)?)
    } else {
        Ok(path.to_path_buf())
    }
}

/// `COUNT(*)` over a dataset, naming it as the runtime registers it: parsed as
/// a table reference, with each part quoted so that a schema-qualified name such
/// as `sales.orders` or a reserved word such as `order` stays one identifier.
fn count_sql(dataset: &str) -> String {
    let table = TableReference::parse_str(dataset);
    let quoted: Vec<String> = [table.catalog(), table.schema(), Some(table.table())]
        .into_iter()
        .flatten()
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect();
    format!("SELECT COUNT(*) FROM {}", quoted.join("."))
}

/// Sum of every sample of `name` whose labels include all of `labels`.
fn metric_sum(metrics: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let mut found = None;
    for line in metrics.lines() {
        let Some(rest) = line.strip_prefix(name) else {
            continue;
        };
        let Some((label_set, value)) = rest
            .strip_prefix('{')
            .and_then(|rest| rest.rsplit_once("} "))
        else {
            continue;
        };
        let matches = labels
            .iter()
            .all(|(key, value)| label_set.contains(&format!("{key}=\"{value}\"")));
        if let (true, Ok(value)) = (matches, value.trim().parse::<f64>()) {
            found = Some(found.unwrap_or(0.0) + value);
        }
    }
    found
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Prometheus counters are non-negative integers"
)]
fn counter(value: Option<f64>) -> u64 {
    value.unwrap_or_default() as u64
}

fn median(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    let mid = values.len() / 2;
    match values.len() {
        0 => 0,
        len if len % 2 == 0 => values[mid - 1].midpoint(values[mid]),
        _ => values[mid],
    }
}

fn markdown_summary(results: &ColdStartResults) -> String {
    let mut out = String::new();
    let verdict = if results.failures.is_empty() {
        "passed"
    } else {
        "failed"
    };
    let _ = writeln!(
        out,
        "### Cold start {verdict}: `{}` (`--cpu-cores {}`)\n\nCandidate commit `{}`, testoperator commit `{}`.\n",
        results.spicepod.display(),
        results.cpu_cores,
        results.spiced_commit,
        results.testoperator_commit
    );
    let _ = writeln!(
        out,
        "| Binary | Version | Median ready (ms) | Run | Ready (ms) | Dataset | Initial load (ms) | Full compactions | Rows |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|---|---|---|");
    let binaries = std::iter::once(("candidate", &results.candidate))
        .chain(results.baseline.iter().map(|b| ("baseline", b)));
    for (label, binary) in binaries {
        let median_ms = binary.median_ready_ms();
        for (index, run) in binary.runs.iter().enumerate() {
            for dataset in &run.datasets {
                let load = format!("{:.0}", dataset.initial_load_ms);
                let _ = writeln!(
                    out,
                    "| {label} | {} | {median_ms} | {} | {} | {} | {load} | {} | {} |",
                    binary.version.trim(),
                    index + 1,
                    run.ready_ms,
                    dataset.name,
                    dataset.full_compactions,
                    dataset.rows
                );
            }
        }
    }
    for failure in &results.failures {
        let _ = writeln!(out, "\n- **Failure:** {failure}");
    }
    out
}

fn append_file(path: &Path, contents: &str) -> anyhow::Result<()> {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const METRICS: &str = r#"# TYPE dataset_acceleration_refresh_duration_ms histogram
dataset_acceleration_refresh_duration_ms_sum{dataset="audit_log",mode="append"} 42256.29
dataset_acceleration_refresh_duration_ms_sum{dataset="runtime.metrics",mode="full"} 1.68
cayenne_compaction_outcome_total{kind="full",outcome="committed",table="audit_log"} 5
cayenne_compaction_outcome_total{kind="full",outcome="declined_below_trigger",table="audit_log"} 9
cayenne_compaction_outcome_total{kind="subset_current",outcome="committed",table="audit_log"} 2
"#;

    #[test]
    fn metric_sum_matches_every_requested_label() {
        let load = metric_sum(
            METRICS,
            "dataset_acceleration_refresh_duration_ms_sum",
            &[("dataset", "audit_log")],
        );
        assert!(
            load.is_some_and(|ms| (ms - 42_256.29).abs() < 1e-6),
            "{load:?}"
        );
        let full = metric_sum(
            METRICS,
            "cayenne_compaction_outcome_total",
            &[
                ("table", "audit_log"),
                ("kind", "full"),
                ("outcome", "committed"),
            ],
        );
        assert_eq!(counter(full), 5);
        assert_eq!(
            metric_sum(
                METRICS,
                "cayenne_compaction_outcome_total",
                &[("table", "orders")]
            ),
            None
        );
    }

    fn binary(ready_ms: &[u64], full_compactions: u64, rows: i64) -> BinaryResult {
        BinaryResult {
            spiced_path: PathBuf::from("spiced"),
            version: "v0".to_string(),
            runs: ready_ms
                .iter()
                .map(|&ready_ms| RunResult {
                    ready_ms,
                    datasets: vec![DatasetResult {
                        name: "audit_log".to_string(),
                        initial_load_ms: 0.0,
                        full_compactions,
                        rows,
                    }],
                })
                .collect(),
        }
    }

    fn args(max_full_compactions: Option<u64>) -> ColdStartArgs {
        use clap::Parser as _;
        let mut args = ColdStartArgs::parse_from(["cold-start"]);
        args.max_full_compactions = max_full_compactions;
        args
    }

    #[test]
    fn count_sql_quotes_each_part_of_a_dataset_name() {
        assert_eq!(
            count_sql("audit_events"),
            r#"SELECT COUNT(*) FROM "audit_events""#
        );
        assert_eq!(
            count_sql("sales.order"),
            r#"SELECT COUNT(*) FROM "sales"."order""#
        );
        assert_eq!(
            count_sql("Sales.Orders"),
            r#"SELECT COUNT(*) FROM "sales"."orders""#
        );
    }

    #[test]
    fn executable_path_resolves_only_relative_paths_with_a_directory() {
        assert_eq!(
            executable_path(Path::new("spiced")).expect("bare name"),
            PathBuf::from("spiced")
        );
        assert_eq!(
            executable_path(Path::new("/usr/local/bin/spiced")).expect("absolute path"),
            PathBuf::from("/usr/local/bin/spiced")
        );
        let relative = Path::new("./Cargo.toml");
        let resolved = executable_path(relative).expect("relative path");
        assert!(resolved.is_absolute(), "{resolved:?}");
        assert_eq!(
            resolved,
            std::fs::canonicalize(relative).expect("canonicalize")
        );
    }

    #[test]
    fn evaluate_flags_a_slower_candidate_against_the_baseline() {
        let failures = evaluate(
            &args(None),
            &binary(&[43_000, 44_000, 42_000], 0, 10),
            Some(&binary(&[2_000, 2_100, 1_900], 0, 10)),
        );
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(
            failures[0].starts_with("median time to ready"),
            "{failures:?}"
        );
        assert!(
            failures[1].starts_with("slowest time to ready"),
            "{failures:?}"
        );

        let failures = evaluate(
            &args(None),
            &binary(&[2_500, 2_400, 2_600], 0, 10),
            Some(&binary(&[2_000, 2_100, 1_900], 0, 10)),
        );
        assert!(failures.is_empty(), "{failures:?}");
    }

    #[test]
    fn median_averages_the_middle_pair_of_an_even_count() {
        assert_eq!(median(vec![3_000, 1_000]), 2_000);
        assert_eq!(median(vec![1_000, 3_000, 2_000]), 2_000);
        assert_eq!(median(vec![4, 1, 3, 2]), 2);
        assert_eq!(median(Vec::new()), 0);
    }

    #[test]
    fn evaluate_flags_a_regression_only_some_cold_starts_hit() {
        let failures = evaluate(
            &args(None),
            &binary(&[2_000, 2_000, 43_000], 0, 10),
            Some(&binary(&[2_000, 2_000, 2_000], 0, 10)),
        );
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(
            failures[0].starts_with("slowest time to ready"),
            "{failures:?}"
        );
    }

    #[test]
    fn evaluate_flags_rewrites_and_row_mismatches() {
        let failures = evaluate(&args(Some(0)), &binary(&[2_000], 5, 10), None);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("5 full compactions"), "{failures:?}");

        let failures = evaluate(
            &args(None),
            &binary(&[2_000], 0, 10),
            Some(&binary(&[2_000], 0, 9)),
        );
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("rows"), "{failures:?}");
    }
}
