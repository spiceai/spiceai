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

use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::{CLUSTER_ASSIGNMENT_INTERVAL, Instance, Options, executors_in_containers};
use crate::script::{Action, Script};

/// The address a script's executors and sources meet at when its executors
/// run in containers behind a rootless Docker daemon, which then run on the
/// bridge network (see [`crate::spiced::ContainerRun::bridge`]): this
/// machine's routable one. `None` for every other script.
pub(super) async fn bridge_address(
    script: &Script,
    options: &Options,
) -> Result<Option<std::net::IpAddr>> {
    if !cfg!(target_os = "linux")
        || !executors_in_containers(script, options)
        || !crate::docker::daemon_is_rootless().await
    {
        return Ok(None);
    }
    crate::docker::routable_address()
        .map(Some)
        .context("executors in containers behind a rootless Docker daemon")
}

/// The source kinds a spicepod may name when its executors run in bridge
/// containers: an object store, which [`published_to_bridge`] puts where they
/// reach it, behind a secret of the run's own; and files and `DuckDB`
/// databases, which they read from the mounted work directory. Every other
/// source serves on loopback, out of the executors' reach, so as not to
/// publish a database with fixed or no credentials on every interface.
const BRIDGEABLE_KINDS: [&str; 3] = ["rustfs", "files", "duckdb"];

/// Whether a source of `kind` publishes its ports at the bridge address too,
/// not only on loopback (see [`BRIDGEABLE_KINDS`]).
pub(super) fn published_to_bridge(kind: &str) -> bool {
    kind == "rustfs"
}

/// Refuses a run with bridged executors whose spicepod, a variant of it, or a
/// `start` or `restart` patch names a source of a kind not in
/// [`BRIDGEABLE_KINDS`]: the executors could not reach it, and the run would
/// fail later and less clearly. A source the spicepod does not name, such as
/// a script's oracle, stays on loopback.
pub(super) fn check_bridged_sources<'a>(
    script: &Script,
    sources: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<()> {
    let patches = script.steps.iter().filter_map(|step| match &step.action {
        Action::Start { patch, .. } | Action::Restart { patch, .. } => patch.as_ref(),
        _ => None,
    });
    let mut texts = vec![script.spicepod.to_string()];
    for variant in script.variants.values().chain(patches) {
        texts.extend(variant.spicepod.iter().map(ToString::to_string));
        texts.extend(variant.datasets.values().map(ToString::to_string));
        texts.extend(variant.catalogs.values().map(ToString::to_string));
    }
    for (name, kind) in sources {
        let named = [format!("sources.{name}."), format!("sources.{name}}}")]
            .iter()
            .any(|needle| texts.iter().any(|text| text.contains(needle.as_str())));
        if named && !BRIDGEABLE_KINDS.contains(&kind) {
            anyhow::bail!(
                "executors in containers behind a rootless Docker daemon reach only {} sources, \
                 and the spicepod names source '{name}' ({kind})",
                BRIDGEABLE_KINDS.join(", ")
            );
        }
    }
    Ok(())
}

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
    for (index, executor) in instance.executors.iter_mut().enumerate().take(active) {
        if !executor.is_running() {
            if let Some(name) = executor.container_name() {
                // The combination reserved this room before it took its
                // `--jobs` slots; this waits only if something outside the
                // run took it meanwhile, or for a restart.
                crate::keyquota::wait_for_room(1, &format!("executor container {name}")).await?;
            }
            executor.start(&instance.spicepod_yaml)?;
            executor.wait_health(timeout).await?;
            // Healthy, so its container is running and holds its own key.
            if let Some(key_room) = &mut instance.key_room {
                key_room.executor_started(index);
            }
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
/// run's generated CA and certificates.
///
/// Every node advertises, and the scheduler binds, loopback, unless the
/// executors run in bridge containers (`bridge`): then that address, the one
/// they reach this machine and each other's published ports at, while each
/// executor binds every interface inside its container.
pub(super) fn cluster_args(
    pki: &std::path::Path,
    context: &Value,
    executors: usize,
    bridge: Option<std::net::IpAddr>,
) -> Result<(Vec<String>, Vec<Vec<String>>)> {
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
    let advertise = bridge
        .unwrap_or(std::net::Ipv4Addr::LOCALHOST.into())
        .to_string();
    let executor_bind = if bridge.is_some() {
        "0.0.0.0"
    } else {
        "127.0.0.1"
    };
    let mut scheduler: Vec<String> = [
        "--role",
        "scheduler",
        "--node-advertise-address",
        &advertise,
        "--node-bind-address",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    scheduler.push(format!("{advertise}:{scheduler_port}"));
    scheduler.extend(mtls("scheduler"));
    let mut all = Vec::new();
    for n in 0..executors {
        let port = if n == 0 {
            context["instance"]["executor_port"].to_string()
        } else if bridge.is_some() {
            // Published on `bridge` as well as loopback, so free on both.
            crate::docker::free_port_on(std::net::Ipv4Addr::UNSPECIFIED.into())?.to_string()
        } else {
            crate::docker::free_port()?.to_string()
        };
        let mut executor: Vec<String> = ["--role", "executor", "--scheduler-address"]
            .iter()
            .map(ToString::to_string)
            .collect();
        executor.push(format!("https://{advertise}:{scheduler_port}"));
        executor.extend([
            "--node-advertise-address".to_string(),
            advertise.clone(),
            "--node-bind-address".to_string(),
        ]);
        executor.push(format!("{executor_bind}:{port}"));
        executor.extend(mtls("executor"));
        all.push(executor);
    }
    Ok((scheduler, all))
}

/// Executors in bridge containers (see [`crate::spiced::ContainerRun::bridge`])
/// reach a source at this machine's routable address, not at loopback, which
/// is their own: so every loopback address in the spicepod becomes `address`.
/// The sources of such a run also publish their ports there.
pub(super) fn spicepod_for_bridge(spicepod: &mut Value, address: std::net::IpAddr) {
    match spicepod {
        Value::String(text) if text.contains("127.0.0.1") => {
            *text = text.replace("127.0.0.1", &address.to_string());
        }
        Value::Array(items) => {
            for item in items {
                spicepod_for_bridge(item, address);
            }
        }
        Value::Object(map) => {
            for value in map.values_mut() {
                spicepod_for_bridge(value, address);
            }
        }
        _ => {}
    }
}

/// The engine an acceleration block runs, or `None` when it is disabled,
/// with the Spicepod's defaults applied: omitted `enabled` is true, and an
/// omitted `engine` is Cayenne.
fn accelerated_engine(acceleration: &Value) -> Option<&str> {
    if acceleration.get("enabled").and_then(Value::as_bool) == Some(false) {
        return None;
    }
    Some(
        acceleration
            .get("engine")
            .and_then(Value::as_str)
            .unwrap_or("cayenne"),
    )
}

/// The first acceleration engine in the spicepod a cluster cannot run
/// (anything but Cayenne and Arrow).
pub(super) fn undistributed_engine(spicepod: &Value) -> Option<String> {
    spicepod
        .get("datasets")?
        .as_array()?
        .iter()
        .filter_map(|d| d.get("acceleration"))
        .filter_map(accelerated_engine)
        .find(|e| !matches!(*e, "cayenne" | "arrow"))
        .map(str::to_string)
}

/// Why a Cayenne acceleration in this spicepod cannot be partitioned, which
/// a cluster requires of every acceleration: one explicitly in memory, or
/// one maintaining aggregates, is refused with `partition_by`.
pub(super) fn unpartitionable_acceleration(spicepod: &Value) -> Option<String> {
    spicepod.get("datasets")?.as_array()?.iter().find_map(|d| {
        let a = d.get("acceleration")?;
        if accelerated_engine(a) != Some("cayenne") {
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
        let Some(engine) = dataset
            .get("acceleration")
            .and_then(accelerated_engine)
            .map(str::to_string)
        else {
            continue;
        };
        let Some(acceleration) = dataset
            .get_mut("acceleration")
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        // Partitioned Cayenne lives in files; one that leaves the mode to its
        // default (memory) is given files rather than skipped.
        if engine == "cayenne" && !acceleration.contains_key("mode") {
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        check_bridged_sources, cluster_args, make_cluster_ready, published_to_bridge,
        spicepod_for_bridge, undistributed_engine, unpartitionable_acceleration,
    };

    /// The value after `flag` in `args`.
    fn after<'a>(args: &'a [String], flag: &str) -> &'a str {
        let i = args.iter().position(|a| a == flag).expect(flag);
        &args[i + 1]
    }

    #[test]
    fn nodes_meet_on_loopback_unless_the_executors_are_bridged() {
        let context = json!({ "instance": { "port": 29170, "executor_port": 29160 } });
        let pki = std::path::Path::new("pki");
        let (scheduler, executors) =
            cluster_args(pki, &context, 1, None).expect("cluster arguments");
        assert_eq!(after(&scheduler, "--node-bind-address"), "127.0.0.1:29170");
        assert_eq!(after(&scheduler, "--node-advertise-address"), "127.0.0.1");
        let executor = &executors[0];
        assert_eq!(
            after(executor, "--scheduler-address"),
            "https://127.0.0.1:29170"
        );
        assert_eq!(after(executor, "--node-advertise-address"), "127.0.0.1");
        assert_eq!(after(executor, "--node-bind-address"), "127.0.0.1:29160");

        let address = std::net::Ipv4Addr::new(10, 244, 0, 183).into();
        let (scheduler, executors) =
            cluster_args(pki, &context, 2, Some(address)).expect("cluster arguments");
        assert_eq!(
            after(&scheduler, "--node-bind-address"),
            "10.244.0.183:29170"
        );
        assert_eq!(
            after(&scheduler, "--node-advertise-address"),
            "10.244.0.183"
        );
        assert_eq!(executors.len(), 2);
        for executor in &executors {
            assert_eq!(
                after(executor, "--scheduler-address"),
                "https://10.244.0.183:29170"
            );
            assert_eq!(after(executor, "--node-advertise-address"), "10.244.0.183");
            assert!(after(executor, "--node-bind-address").starts_with("0.0.0.0:"));
        }
        assert_eq!(after(&executors[0], "--node-bind-address"), "0.0.0.0:29160");
    }

    #[test]
    fn bridged_executors_reach_only_vetted_sources_the_spicepod_names() {
        let script = crate::script::Script::parse(
            "name: s\n\
             sources: { lake: { kind: rustfs }, db: { kind: postgres }, oracle: { kind: postgres } }\n\
             spicepod:\n  datasets:\n    - from: s3://${sources.lake.bucket}/orders/\n      name: orders\n      params: { $merge: '${sources.lake.params}', file_format: parquet }\n\
             variants:\n  pg: { datasets: { '*': { params: { pg_port: '${sources.db.port}' } } } }\n\
             steps: [{ start: {} }]",
        )
        .expect("script parses");
        // The object store the spicepod names, and an oracle it does not.
        check_bridged_sources(&script, [("lake", "rustfs"), ("oracle", "postgres")])
            .expect("an object store and an unnamed oracle are allowed");
        // A database named only by a variant would be published with fixed
        // credentials: refused.
        let error = check_bridged_sources(&script, [("lake", "rustfs"), ("db", "postgres")])
            .expect_err("a named database is refused");
        assert!(error.to_string().contains("'db' (postgres)"), "{error}");
        // A source whose name only extends a named one's is not named.
        check_bridged_sources(&script, [("lak", "kafka")]).expect("not named");
        // A source named only by a restart's patch is refused too.
        let patched = crate::script::Script::parse(
            "name: s\n\
             sources: { lake: { kind: rustfs }, feed: { kind: kafka } }\n\
             spicepod: { datasets: [{ from: 's3://${sources.lake.bucket}/t/', name: t }] }\n\
             steps:\n  - start: {}\n  - restart:\n      patch: { datasets: { t: { params: { kafka_bootstrap_servers: '${sources.feed.bootstrap}' } } } }",
        )
        .expect("script parses");
        let error = check_bridged_sources(&patched, [("lake", "rustfs"), ("feed", "kafka")])
            .expect_err("a source named by a patch is refused");
        assert!(error.to_string().contains("'feed' (kafka)"), "{error}");
        assert!(published_to_bridge("rustfs"));
        assert!(!published_to_bridge("postgres"));
        assert!(!published_to_bridge("mock-http"));
    }

    #[test]
    fn a_bridged_spicepod_names_sources_at_the_peer_address() {
        let mut spicepod = json!({
            "datasets": [{
                "from": "s3://i0-default/orders/",
                "params": { "s3_endpoint": "http://127.0.0.1:29150", "s3_region": "us-east-1" },
                "acceleration": { "refresh_check_interval": 10 }
            }]
        });
        spicepod_for_bridge(
            &mut spicepod,
            std::net::Ipv4Addr::new(10, 244, 0, 183).into(),
        );
        assert_eq!(
            spicepod,
            json!({
                "datasets": [{
                    "from": "s3://i0-default/orders/",
                    "params": { "s3_endpoint": "http://10.244.0.183:29150", "s3_region": "us-east-1" },
                    "acceleration": { "refresh_check_interval": 10 }
                }]
            })
        );
    }

    fn pod(acceleration: &serde_json::Value) -> serde_json::Value {
        json!({ "datasets": [{ "name": "t", "acceleration": acceleration }] })
    }

    #[test]
    fn applies_the_spicepod_acceleration_defaults() {
        // Omitted `enabled` is true.
        assert_eq!(
            undistributed_engine(&pod(&json!({ "engine": "duckdb" }))).as_deref(),
            Some("duckdb")
        );
        assert_eq!(
            undistributed_engine(&pod(&json!({ "enabled": false, "engine": "duckdb" }))),
            None
        );
        // Omitted `engine` is Cayenne.
        assert_eq!(
            unpartitionable_acceleration(&pod(&json!({ "enabled": true, "mode": "memory" })))
                .as_deref(),
            Some("'t' is an in-memory Cayenne acceleration, which cannot be partitioned")
        );
        let mut spicepod = pod(&json!({ "primary_key": "id" }));
        make_cluster_ready(
            &mut spicepod,
            std::path::Path::new("/w"),
            &std::collections::BTreeMap::new(),
        );
        assert_eq!(
            spicepod["datasets"][0]["acceleration"],
            json!({
                "primary_key": "id",
                "mode": "file",
                "partition_by": [{ "bucket": "bucket(4, id)" }]
            })
        );
    }
}
