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

//! Running an instance as a cluster: executor arguments, readiness, and
//! the spicepod changes a scheduler needs.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};

use crate::script::Script;

use super::{CLUSTER_ASSIGNMENT_INTERVAL, Instance};

/// Starts every executor of the instance that is not running, and waits for
/// the scheduler to report all the running ones ready to take work.
/// Starts the instance's executors and waits for them: until they are ready
/// to take work, or, with `joined_only` (a `start` that does not wait for
/// readiness), until they have registered with the scheduler, since an
/// executor holding a dataset that cannot load never reports ready.
pub(super) async fn start_executors(
    instance: &mut Instance,
    timeout: Duration,
    joined_only: bool,
) -> Result<()> {
    if instance.executors.is_empty() {
        return Ok(());
    }
    let active = instance.active_executors;
    for executor in instance.executors.iter_mut().take(active) {
        if !executor.is_running() {
            executor.start(&instance.spicepod_yaml)?;
            executor.wait_health(timeout).await?;
        }
    }
    let mut running = 0;
    for executor in &mut instance.executors {
        if executor.is_running() {
            running += 1;
        }
    }
    let logs: Vec<String> = instance
        .executors
        .iter()
        .map(|e| e.log_path().display().to_string())
        .collect();
    instance
        .spiced
        .wait_executors(running, timeout, joined_only)
        .await
        .with_context(|| {
            format!(
                "the executors did not join the scheduler; see {}",
                logs.join(", ")
            )
        })
}

/// Turns off the SQL results cache unless the script configures it. A cached
/// answer is up to a TTL old, so a query repeated soon after a change could be
/// answered without reading the acceleration or the source at all: a check that
/// the source's change arrived, or that an acceleration serves while its source
/// is down, would compare against the cache instead.
/// `--cluster`: the scheduler and executor arguments, with node mTLS from the
/// `cluster-pki/` directory nearest above the script.
pub(super) fn cluster_args(
    script: &Script,
    context: &Value,
    executors: usize,
) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let script_dir = std::path::absolute(&script.dir)?;
    let pki = script_dir
        .ancestors()
        .map(|d| d.join("cluster-pki"))
        .find(|d| d.join("ca.crt").exists())
        .ok_or_else(|| {
            anyhow!(
                "a cluster needs a cluster-pki/ directory above {}",
                script_dir.display()
            )
        })?;
    let file = |name: &str| pki.join(name).display().to_string();
    let scheduler_port = context["instance"]["port"].to_string();
    let mtls = |node: &str| {
        vec![
            "--node-mtls-ca-certificate-file".to_string(),
            file("ca.crt"),
            "--node-mtls-certificate-file".to_string(),
            file(&format!("{node}.crt")),
            "--node-mtls-key-file".to_string(),
            file(&format!("{node}.key")),
        ]
    };
    let mut scheduler: Vec<String> = [
        "--role",
        "scheduler",
        "--node-advertise-address",
        "127.0.0.1",
        "--node-bind-address",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    scheduler.push(format!("127.0.0.1:{scheduler_port}"));
    scheduler.extend(mtls("scheduler"));
    let mut all = Vec::new();
    for n in 0..executors {
        let port = if n == 0 {
            context["instance"]["executor_port"].to_string()
        } else {
            crate::docker::free_port()?.to_string()
        };
        let mut executor: Vec<String> = ["--role", "executor", "--scheduler-address"]
            .iter()
            .map(ToString::to_string)
            .collect();
        executor.push(format!("https://127.0.0.1:{scheduler_port}"));
        executor.extend([
            "--node-advertise-address".to_string(),
            "127.0.0.1".to_string(),
            "--node-bind-address".to_string(),
        ]);
        executor.push(format!("127.0.0.1:{port}"));
        executor.extend(mtls("executor"));
        all.push(executor);
    }
    Ok((scheduler, all))
}

/// The first acceleration engine in the spicepod a cluster cannot run
/// (anything but Cayenne and Arrow).
pub(super) fn undistributed_engine(spicepod: &Value) -> Option<String> {
    spicepod
        .get("datasets")?
        .as_array()?
        .iter()
        .filter_map(|d| d.get("acceleration"))
        .filter(|a| a.get("enabled") == Some(&Value::Bool(true)))
        .filter_map(|a| a.get("engine").and_then(Value::as_str))
        .find(|e| !matches!(*e, "cayenne" | "arrow"))
        .map(str::to_string)
}

/// Why a Cayenne acceleration in this spicepod cannot be partitioned, which
/// a cluster requires of every acceleration: one explicitly in memory, or
/// one maintaining aggregates, is refused with `partition_by`.
pub(super) fn unpartitionable_acceleration(spicepod: &Value) -> Option<String> {
    spicepod.get("datasets")?.as_array()?.iter().find_map(|d| {
        let a = d.get("acceleration")?;
        if a.get("enabled") != Some(&Value::Bool(true))
            || a.get("engine").and_then(Value::as_str).unwrap_or("arrow") != "cayenne"
        {
            return None;
        }
        let name = d.get("name").and_then(Value::as_str).unwrap_or("?");
        if a.get("mode").and_then(Value::as_str) == Some("memory") {
            Some(format!(
                "'{name}' is an in-memory Cayenne acceleration, which cannot be partitioned"
            ))
        } else if a.get("maintained_aggregates").is_some() {
            Some(format!(
                "'{name}' maintains aggregates, which a partitioned Cayenne acceleration cannot"
            ))
        } else {
            None
        }
    })
}

/// `--cluster`: what a scheduler requires of a spicepod that a single runtime
/// does not — a state location, Cayenne in files, and partitions on every
/// acceleration: the column `defaults.cluster-partition-by` names for it,
/// else its primary key's first column.
pub(super) fn make_cluster_ready(
    spicepod: &mut Value,
    dir: &std::path::Path,
    partition_by: &BTreeMap<String, String>,
) {
    let Some(root) = spicepod.as_object_mut() else {
        return;
    };
    let runtime = root.entry("runtime").or_insert_with(|| json!({}));
    if let Some(runtime) = runtime.as_object_mut() {
        let state = runtime.entry("state").or_insert_with(|| json!({}));
        if let Some(state) = state.as_object_mut() {
            state
                .entry("location")
                .or_insert_with(|| json!(format!("file://{}/state", dir.display())));
        }
        // The scheduler waits one assignment interval (30 s by default) for
        // executors to join before assigning partitions; a script's executors
        // join within a second or two.
        let scheduler = runtime.entry("scheduler").or_insert_with(|| json!({}));
        if let Some(scheduler) = scheduler.as_object_mut() {
            scheduler
                .entry("partition_assignment_interval")
                .or_insert_with(|| json!(CLUSTER_ASSIGNMENT_INTERVAL));
        }
    }
    let Some(datasets) = root.get_mut("datasets").and_then(Value::as_array_mut) else {
        return;
    };
    for dataset in datasets {
        let dataset_name = dataset
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string);
        let Some(acceleration) = dataset
            .get_mut("acceleration")
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        if acceleration.get("enabled") != Some(&Value::Bool(true)) {
            continue;
        }
        // Partitioned Cayenne lives in files; one that leaves the mode to its
        // default (memory) is given files rather than skipped.
        if acceleration.get("engine").and_then(Value::as_str) == Some("cayenne")
            && !acceleration.contains_key("mode")
        {
            acceleration.insert("mode".into(), json!("file"));
        }
        if acceleration.contains_key("partition_by") {
            continue;
        }
        let named = dataset_name
            .as_deref()
            .and_then(|n| partition_by.get(n))
            .or_else(|| partition_by.get("*"))
            .cloned();
        let key = named.or_else(|| {
            acceleration
                .get("primary_key")
                .and_then(Value::as_str)
                .and_then(|k| k.trim_matches(|c| c == '(' || c == ')').split(',').next())
                .map(str::trim)
                .filter(|k| !k.is_empty())
                .map(str::to_string)
        });
        if let Some(key) = key {
            acceleration.insert(
                "partition_by".into(),
                json!([{ "bucket": format!("bucket(4, {key})") }]),
            );
        }
    }
}
