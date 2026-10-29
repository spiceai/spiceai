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

//! Requests to the runtime other than queries: refreshes, the `spice` CLI,
//! plain HTTP steps, and questions put to a source directly.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;

use crate::script::{HttpStep, Timing};

use super::{Instance, Sources, max_wait, metric_value, namespace, one_line, settled};

/// Triggers a refresh and waits for it to finish. The endpoint only starts a
/// refresh, so the outcome is read from the dataset's status: a new
/// `last_refresh` (recorded only on success) or a new `Error`.
pub(super) async fn refresh(
    instance: &mut Instance,
    dataset: &str,
    expect_error: bool,
    refresh_sql: Option<&str>,
) -> Result<()> {
    const TIMEOUT: Duration = Duration::from_mins(5);
    let field = |info: &Value, key: &str| info.get(key).and_then(Value::as_str).map(str::to_string);
    // A runtime that does not report `last_refresh` (releases before it was
    // added) still publishes the time of the last refresh as a metric.
    let labels = BTreeMap::from([("dataset".to_string(), dataset.to_string())]);
    let refreshed_at = async |instance: &Instance| -> Result<Option<f64>> {
        let text = instance.spiced.metrics().await?;
        Ok(metric_value(
            &text,
            "dataset_acceleration_last_refresh_unix_time_ms",
            &labels,
        ))
    };
    // On a cluster the executors hold the acceleration and refresh it; the
    // scheduler forwards the request and its own status does not move, and
    // executors serve no metrics. A refresh is done when an executor logs
    // another finished load of the dataset.
    let loaded = regex::Regex::new(&format!(
        r"Loaded .* for dataset {} in ",
        regex::escape(dataset)
    ))?;
    let executor_loads = |instance: &Instance| -> usize {
        instance
            .executors
            .iter()
            .filter_map(|e| std::fs::read_to_string(e.log_path()).ok())
            .map(|text| text.lines().filter(|l| loaded.is_match(l)).count())
            .sum()
    };
    // And has failed when an executor logs another failed load of it.
    let failed = regex::Regex::new(&format!(
        r"Failed to load data for dataset {} ",
        regex::escape(dataset)
    ))?;
    let executor_failures = |instance: &Instance| -> Vec<String> {
        instance
            .executors
            .iter()
            .filter_map(|e| std::fs::read_to_string(e.log_path()).ok())
            .flat_map(|text| {
                text.lines()
                    .filter(|l| failed.is_match(l))
                    .map(|l| one_line(l, 300))
                    .collect::<Vec<_>>()
            })
            .collect()
    };
    let before = instance.spiced.dataset_info(dataset).await?;
    let last_before = field(&before, "last_refresh");
    let by_metric = before.get("last_refresh").is_none();
    let metric_before = if by_metric {
        refreshed_at(instance).await?
    } else {
        None
    };
    let executors_before = executor_loads(instance);
    let failures_before = executor_failures(instance).len();
    let error_before = field(&before, "error_message");
    instance.spiced.refresh(dataset, refresh_sql).await?;
    let limit = max_wait(TIMEOUT);
    let deadline = Instant::now() + limit;
    let outcome = loop {
        let info = instance.spiced.dataset_info(dataset).await?;
        let last = field(&info, "last_refresh");
        let error = field(&info, "error_message");
        if last.is_some() && last != last_before {
            break Ok(());
        }
        if !instance.executors.is_empty() {
            if executor_loads(instance) > executors_before {
                break Ok(());
            }
            let failures = executor_failures(instance);
            if let Some(failure) = failures.get(failures_before) {
                break Err(format!("an executor: {failure}"));
            }
        }
        if by_metric {
            let at = refreshed_at(instance).await?;
            if at.is_some() && at != metric_before {
                break Ok(());
            }
        }
        if field(&info, "status").as_deref() == Some("Error") && error != error_before {
            break Err(error.unwrap_or_default());
        }
        if Instant::now() > deadline {
            bail!(
                "refresh of '{dataset}' did not finish within {limit:?} (status: {})",
                field(&info, "status").unwrap_or_default()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    match (outcome, expect_error) {
        (Ok(()), false) => Ok(()),
        (Err(e), true) => {
            say!("        refresh failed as expected: {}", one_line(&e, 160));
            Ok(())
        }
        (Ok(()), true) => {
            bail!("refresh of '{dataset}' succeeded, but the script expected it to fail")
        }
        (Err(e), false) => bail!("refresh of '{dataset}' failed: {e}"),
    }
}

/// Runs the `spice` CLI against the instance's runtime and checks its exit
/// status and output.
pub(super) async fn spice_cli(
    instance: &Instance,
    args: &[String],
    output_contains: Option<&str>,
    expect_error: bool,
) -> Result<String> {
    let args = args
        .iter()
        .map(|a| instance.sql(a))
        .collect::<Result<Vec<_>>>()?;
    // `spice sql` reaches the runtime over Flight, which it addresses
    // separately from HTTP.
    let flight = (args.first().map(String::as_str) == Some("sql"))
        .then(|| ["--endpoint".to_string(), instance.spiced.flight_endpoint()]);
    let out = tokio::process::Command::new(&instance.spice_cli)
        .args(&args)
        .args(["--http-endpoint", &instance.spiced.http_endpoint()])
        .args(flight.iter().flatten())
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .with_context(|| {
            format!(
                "Failed to run {}; pass --spice-cli",
                instance.spice_cli.display()
            )
        })?;
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    match (out.status.success(), expect_error) {
        (true, true) => bail!(
            "`spice {}` succeeded, but the script expected it to fail: {}",
            args.join(" "),
            output.trim()
        ),
        (false, false) => bail!(
            "`spice {}` exited with {}: {}",
            args.join(" "),
            out.status,
            output.trim()
        ),
        _ => {}
    }
    if let Some(expected) = output_contains {
        let expected = instance.sql(expected)?;
        ensure!(
            output.contains(&expected),
            "`spice {}` printed no '{expected}': {}",
            args.join(" "),
            output.trim()
        );
    }
    Ok(one_line(output.trim(), 100))
}

/// Sends the step's request, retrying as its timing asks, until the response
/// carries what the step expects.
pub(super) async fn http_step(instance: &Instance, step: &HttpStep) -> Result<String> {
    let started = Instant::now();
    let headers = step
        .headers
        .iter()
        .map(|(k, v)| Ok((k.clone(), instance.sql(v)?)))
        .collect::<Result<Vec<_>>>()?;
    let body = step.body.as_deref().map(|b| instance.sql(b)).transpose()?;
    let path = with_query(&step.path, &step.query);
    loop {
        let result = async {
            let (status, got, text) = instance
                .spiced
                .http(
                    &step.method,
                    &path,
                    &headers,
                    body.as_deref(),
                    &step.key,
                )
                .await?;
            let mut problems = Vec::new();
            if let Some(expected) = step.status
                && status != expected
            {
                problems.push(format!("status {status}, expected {expected}"));
            }
            for (name, value) in &step.expect_headers {
                let seen: Vec<&str> = got
                    .iter()
                    .filter(|(k, _)| k == name)
                    .map(|(_, v)| v.as_str())
                    .collect();
                if !seen.iter().any(|v| v.contains(value.as_str())) {
                    problems.push(format!("header {name}: expected '{value}', got {seen:?}"));
                }
            }
            for name in &step.lacks_headers {
                if let Some((_, v)) = got.iter().find(|(k, _)| k == name) {
                    problems.push(format!("header {name} should be absent, got '{v}'"));
                }
            }
            if let Some(needle) = &step.body_contains
                && !text.contains(needle.as_str())
            {
                problems.push(format!("body lacks '{needle}': {}", one_line(&text, 200)));
            }
            for pattern in &step.body_matches {
                if !pattern.is_match(&text) {
                    problems.push(format!(
                        "body does not match /{pattern}/: {}",
                        one_line(&text, 2000)
                    ));
                }
            }
            for pattern in &step.body_lacks {
                if let Some(m) = pattern.find(&text) {
                    problems.push(format!(
                        "body matches /{pattern}/ at '{}'",
                        one_line(m.as_str(), 200)
                    ));
                }
            }
            problems.extend(crate::json_assert::check_all(&step.json, &text));
            ensure!(problems.is_empty(), "{}", problems.join("; "));
            let shown: Vec<String> = step
                .expect_headers
                .iter()
                .filter_map(|(name, _)| {
                    got.iter()
                        .find(|(k, _)| k == name)
                        .map(|(k, v)| format!("{k}: {v}"))
                })
                .collect();
            Ok(format!("{status} {}", shown.join(", ")))
        }
        .await;
        if let Some(outcome) = settled(step.timing, started, result) {
            return outcome;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// `path` with `query` encoded and appended, after any query string `path`
/// already carries.
fn with_query(path: &str, query: &[(String, String)]) -> String {
    if query.is_empty() {
        return path.to_string();
    }
    let encoded: Vec<String> = query
        .iter()
        .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
        .collect();
    let separator = if path.contains('?') { '&' } else { '?' };
    format!("{path}{separator}{}", encoded.join("&"))
}

/// Percent-encodes everything but RFC 3986's unreserved characters.
fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Asks a source directly and compares its text answer, row for row in order,
/// with `expected` (cells as text, `null` for NULL).
pub(super) async fn source_query(
    instance: &Instance,
    sources: &Sources,
    source: &str,
    sql: &str,
    expected: &[Vec<Value>],
    timing: &Timing,
) -> Result<String> {
    let namespace = namespace(instance, source)?;
    let src = sources.get(source)?;
    let sql = instance.sql(sql)?;
    let expected: Vec<Vec<Option<String>>> = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| match v {
                    Value::Null => None,
                    Value::String(s) => Some(s.clone()),
                    other => Some(other.to_string()),
                })
                .collect()
        })
        .collect();
    let started = Instant::now();
    loop {
        let result = async {
            let answer = src.query(&namespace, &sql).await?;
            ensure!(
                answer.rows == expected,
                "the source answered {:?}, expected {expected:?}",
                answer.rows
            );
            Ok(format!("{} rows", answer.rows.len()))
        }
        .await;
        if let Some(outcome) = settled(*timing, started, result) {
            return outcome;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
