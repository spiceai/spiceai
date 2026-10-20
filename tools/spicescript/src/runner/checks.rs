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

//! Assertions about a running instance: dataset status, its log, its files
//! and its metrics.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use serde_json::Value;

use crate::script::{LogStep, MetricCondition, MetricWait, StatusStep};

use super::{Instance, POLL, Sources, namespace, one_line, settled};

/// Checks the files under `dir` by their paths relative to it: each
/// `contains` pattern matches one, and no `lacks` pattern matches any.
pub(super) fn assert_files(
    dir: &std::path::Path,
    contains: &[regex::Regex],
    lacks: &[regex::Regex],
) -> Result<String> {
    let mut files = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next)
            .with_context(|| format!("Failed to list {}", next.display()))?
        {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if let Ok(relative) = path.strip_prefix(dir) {
                files.push(relative.to_string_lossy().into_owned());
            }
        }
    }
    let mut problems = Vec::new();
    for pattern in contains {
        if !files.iter().any(|f| pattern.is_match(f)) {
            problems.push(format!("no file matches /{pattern}/"));
        }
    }
    for pattern in lacks {
        for file in files.iter().filter(|f| pattern.is_match(f)) {
            problems.push(format!("/{pattern}/ matches {file}"));
        }
    }
    ensure!(
        problems.is_empty(),
        "under {}: {}",
        dir.display(),
        problems.join("; ")
    );
    Ok(format!("{} files", files.len()))
}

/// Checks `spiced.log` against `step`, retrying as its timing asks.
pub(super) async fn assert_log(instance: &Instance, step: &LogStep) -> Result<String> {
    let started = Instant::now();
    loop {
        let result = check_log(&instance.spiced.log_path(), step);
        if let Some(outcome) = settled(step.timing, started, result) {
            return outcome;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub(super) fn check_log(path: &std::path::Path, step: &LogStep) -> Result<String> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    // `spiced` colors its log; patterns are written against the plain text.
    let text = strip_ansi(&raw);
    let lines: Vec<&str> = text.lines().collect();
    let mut problems = Vec::new();
    if step.clean {
        problems.extend(
            lines
                .iter()
                .filter(|l| reports_problem(l) && !step.allow.iter().any(|a| a.is_match(l)))
                .map(|l| format!("unexpected: {}", one_line(l, 300))),
        );
    }
    for pattern in &step.contains {
        if !lines.iter().any(|l| pattern.is_match(l)) {
            problems.push(format!("no line matches /{pattern}/"));
        }
    }
    for pattern in &step.lacks {
        if let Some(l) = lines.iter().find(|l| pattern.is_match(l)) {
            problems.push(format!("/{pattern}/ matched: {}", one_line(l, 300)));
        }
    }
    for (pattern, count) in &step.count {
        let seen = lines.iter().filter(|l| pattern.is_match(l)).count();
        if !count.holds(seen) {
            problems.push(format!(
                "/{pattern}/ matched {seen} lines, expected {count}"
            ));
        }
    }
    ensure!(
        problems.is_empty(),
        "{} in {}:\n{}",
        if problems.len() == 1 {
            "1 log problem".to_string()
        } else {
            format!("{} log problems", problems.len())
        },
        path.display(),
        problems.join("\n")
    );
    Ok(format!("{} lines", lines.len()))
}

/// A log line that reports an error or a failure, the way `check-spice-log`
/// reads one.
pub(super) fn reports_problem(line: &str) -> bool {
    let line = line.to_ascii_lowercase();
    line.contains("error") || line.contains("failed")
}

/// Removes ANSI color sequences (`ESC [ … m`).
pub(super) fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// The sum of the series named `name` whose labels include `labels`, from a
/// Prometheus exposition; `None` when no series matches.
pub(super) fn metric_value(
    text: &str,
    name: &str,
    labels: &BTreeMap<String, String>,
) -> Option<f64> {
    let mut sum = None;
    for line in text.lines().filter(|l| !l.starts_with('#')) {
        let (series, value) = line.rsplit_once(' ')?;
        let (series_name, series_labels) = match series.split_once('{') {
            Some((n, rest)) => (n, rest.trim_end_matches('}')),
            None => (series, ""),
        };
        if series_name != name {
            continue;
        }
        let has = |k: &String, v: &String| {
            series_labels
                .split(',')
                .any(|pair| pair.trim() == format!("{k}=\"{v}\""))
        };
        if labels.iter().all(|(k, v)| has(k, v))
            && let Ok(v) = value.parse::<f64>()
        {
            *sum.get_or_insert(0.0) += v;
        }
    }
    sum
}

/// Polls `/metrics` until the condition holds. With `drive`, writes new data
/// to the source each round, and gives up after the rounds rather than a time.
pub(super) async fn wait_metric(
    instance: &mut Instance,
    w: &MetricWait,
    sources: &Sources,
) -> Result<String> {
    let started = Instant::now();
    let read = |text: &str, name: &str| metric_value(text, name, &w.labels);
    let baseline = match &w.condition {
        MetricCondition::IncreasedBy {
            since: Some(mark), ..
        } => read(
            instance
                .metric_marks
                .get(mark)
                .with_context(|| format!("no `mark-metrics: {mark}` before this step"))?,
            &w.name,
        )
        .unwrap_or(0.0),
        _ => read(&instance.spiced.metrics().await?, &w.name).unwrap_or(0.0),
    };
    let mut round = 0u32;
    let mut last_drive = Instant::now();
    loop {
        if let Some(drive) = &w.drive
            && (round == 0 || last_drive.elapsed() >= drive.every)
        {
            ensure!(
                round < drive.rounds,
                "{:?} not met after {} rounds of `{}` ({:.0}s)",
                w.condition,
                drive.rounds,
                one_line(&drive.sql, 80),
                started.elapsed().as_secs_f64()
            );
            round += 1;
            let sql = instance.sql(&drive.sql.replace("${round}", &round.to_string()))?;
            let namespace = namespace(instance, &drive.source)?;
            sources.get(&drive.source)?.exec(&namespace, &sql).await?;
            last_drive = Instant::now();
        }
        let text = instance.spiced.metrics().await?;
        let value = read(&text, &w.name);
        let met = match (&w.condition, value) {
            (_, None) => false,
            (MetricCondition::Above(x), Some(v)) => v > *x,
            (MetricCondition::Below(x), Some(v)) => v < *x,
            (MetricCondition::BelowMetric(other), Some(v)) => {
                read(&text, other).is_some_and(|o| v < o)
            }
            (MetricCondition::Increases, Some(v)) => v > baseline,
            (MetricCondition::IncreasedBy { amount, .. }, Some(v)) => {
                // A counter that overshoots will not come back: fail now.
                ensure!(
                    v - baseline <= *amount + f64::EPSILON,
                    "{} rose by {} from {baseline}, more than {amount}",
                    w.name,
                    v - baseline
                );
                (v - baseline - amount).abs() <= f64::EPSILON
            }
        };
        let last = format!("{} = {value:?}", w.name);
        if met {
            return Ok(if round > 0 {
                format!("{last} after {round} rounds")
            } else {
                last
            });
        }
        ensure!(
            w.drive.is_some() || started.elapsed() < w.timeout,
            "{:?} not met within {:?}; last {last}",
            w.condition,
            w.timeout
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Checks a dataset's status until its timing is satisfied; returns what it saw.
pub(super) async fn assert_status(instance: &mut Instance, step: &StatusStep) -> Result<String> {
    let started = Instant::now();
    loop {
        let attempt = async {
            let info = instance.spiced.dataset_info(&step.dataset).await?;
            let status = info
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let message = info
                .get("error_message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let ready = instance.spiced.ready().await;
            let seen = format!(
                "status={status} ready={ready} message={}",
                one_line(&message, 160)
            );
            if let Some(want) = &step.status {
                ensure!(&status == want, "expected status {want}, saw {seen}");
            }
            if let Some(want) = &step.message_contains {
                ensure!(
                    message.contains(want.as_str()),
                    "expected a message containing '{want}', saw {seen}"
                );
            }
            if let Some(want) = step.ready {
                ensure!(ready == want, "expected ready={want}, saw {seen}");
            }
            let set = |field: &String| info.get(field).is_some_and(|v| !v.is_null());
            for field in &step.has {
                ensure!(set(field), "expected `{field}` to be set, saw {info}");
            }
            for field in &step.lacks {
                ensure!(!set(field), "expected no `{field}`, saw {info}");
            }
            Ok::<_, anyhow::Error>(seen)
        }
        .await;
        if let Some(outcome) = settled(step.timing, started, attempt) {
            return outcome;
        }
        tokio::time::sleep(POLL).await;
    }
}
