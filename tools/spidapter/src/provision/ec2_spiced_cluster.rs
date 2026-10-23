// Copyright 2026 Spice AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A temporary `spiced` scheduler + executor cluster on EC2.
//!
//! Every resource carries `spice:purpose=ballista-bench` and the run's IDs as
//! tags, so a workflow can remove anything a failed run leaves behind. Each
//! node also schedules its own shutdown, which terminates the instance.
//!
//! Nodes authenticate each other with mTLS. The certificate of a node names
//! its private IP, which is only known after launch, so each node signs its
//! own certificate at boot with a CA created for this run.

use std::collections::HashMap;
use std::time::Duration;

use aws_config::Region;
use aws_sdk_ec2::Client as Ec2Client;
use aws_sdk_ec2::types::{
    BlockDeviceMapping, EbsBlockDevice, HttpTokensState, InstanceMetadataOptionsRequest,
    InstanceNetworkInterfaceSpecification, InstanceType, IpPermission, IpRange, ResourceType,
    ShutdownBehavior, Tag, TagSpecification, UserIdGroupPair, VolumeType,
};
use base64::Engine as _;
use spicepod::component::runtime::{
    Scheduler, default_max_partition_assignments_per_interval, default_max_partitions_per_executor,
    default_partition_assignment_interval, default_partition_discovery_timeout,
};
use system_adapter_protocol::DatasetConfig;
use tokio::process::Command as TokioCommand;
use uuid::Uuid;

use super::super::{
    LocalProcesses, LocalRunState, RunState, SetupConfig, generate_initial_spicepod,
    serialize_spicepod,
};
use crate::args::StdioArgs;
use crate::scenario::{Ec2ClusterConfig, ScpConfig};

const PURPOSE_TAG: &str = "spice:purpose";
const PURPOSE: &str = "ballista-bench";
const HTTP_PORT: u16 = 8090;
const FLIGHT_PORT: u16 = 50051;
const NODE_PORT: u16 = 50052;
/// `spiced` serves `/metrics` only on its `--metrics` address.
const METRICS_PORT: u16 = 9090;
const SCHEDULER_STATE_LOCATION: &str = "file:///data/scheduler-state";
/// EC2 rejects user data above 16 KiB before base64 encoding.
const MAX_USER_DATA_BYTES: usize = 16 * 1024;
/// Boot, Docker install and image pull come before `spiced` starts.
const MIN_READY_WAIT: Duration = Duration::from_mins(15);

/// Resources one EC2 cluster run owns. `terminate` empties it, so dropping it
/// afterwards does nothing.
pub(crate) struct Ec2Cluster {
    pub(crate) region: String,
    pub(crate) instance_ids: Vec<String>,
    pub(crate) security_group_id: Option<String>,
}

impl Ec2Cluster {
    pub(crate) async fn terminate(&mut self) -> anyhow::Result<()> {
        let ec2 = ec2_client(&self.region).await;
        if !self.instance_ids.is_empty() {
            eprintln!(
                "[stdio] ec2 backend: terminating {}",
                self.instance_ids.join(", ")
            );
            ec2.terminate_instances()
                .set_instance_ids(Some(self.instance_ids.clone()))
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to terminate instances: {e:#?}"))?;
            wait_for_instance_state(&ec2, &self.instance_ids, "terminated").await?;
            self.instance_ids.clear();
        }
        if let Some(group_id) = self.security_group_id.take() {
            ec2.delete_security_group()
                .group_id(&group_id)
                .send()
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to delete security group {group_id}: {e:#?}")
                })?;
            eprintln!("[stdio] ec2 backend: deleted security group {group_id}");
        }
        Ok(())
    }
}

impl Drop for Ec2Cluster {
    fn drop(&mut self) {
        if self.instance_ids.is_empty() && self.security_group_id.is_none() {
            return;
        }
        let mut owned = Ec2Cluster {
            region: self.region.clone(),
            instance_ids: std::mem::take(&mut self.instance_ids),
            security_group_id: self.security_group_id.take(),
        };
        std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                eprintln!("[stdio] ec2 backend: failed to build tokio runtime for cleanup");
                return;
            };
            if let Err(e) = rt.block_on(owned.terminate()) {
                eprintln!("[stdio] ec2 backend: cleanup failed: {e}");
            }
        })
        .join()
        .ok();
    }
}

pub(crate) async fn provision_ec2_spiced_cluster(
    run_id: Uuid,
    ready_wait: Duration,
    setup_config: &SetupConfig,
    datasets: &HashMap<String, DatasetConfig>,
    args: &StdioArgs,
    config: &Ec2ClusterConfig,
) -> anyhow::Result<RunState> {
    let mut spicepod = generate_initial_spicepod(
        &run_id,
        setup_config,
        datasets,
        None,
        args,
        &ScpConfig::default(),
        None,
    )
    .await?;
    // `runtime.scheduler.state_location` is required before 2.4 and accepted
    // after it, so one setting serves every release under comparison.
    spicepod.runtime.scheduler = Some(Scheduler {
        state_location: Some(SCHEDULER_STATE_LOCATION.to_string()),
        params: None,
        partition_assignment_interval: default_partition_assignment_interval(),
        max_partition_assignments_per_interval: default_max_partition_assignments_per_interval(),
        max_partitions_per_executor: default_max_partitions_per_executor(),
        partition_discovery_timeout: default_partition_discovery_timeout(),
    });
    let spicepod_yaml = serialize_spicepod(&spicepod)?;

    let ca = generate_ca(run_id).await?;
    let runner_ip = runner_public_ip().await?;
    let ec2 = ec2_client(&config.region).await;
    let tags = RunTags::new(run_id);

    let mut cluster = Ec2Cluster {
        region: config.region.clone(),
        instance_ids: Vec::new(),
        security_group_id: None,
    };
    let group_id = create_security_group(&ec2, config, &tags, &runner_ip).await?;
    cluster.security_group_id = Some(group_id.clone());

    eprintln!(
        "[stdio] ec2 backend: launching scheduler ({}) and {} executor(s) ({}) with {}",
        config.scheduler_instance_type,
        config.executor_count,
        config.executor_instance_type,
        config.image
    );
    let scheduler_ids = launch_nodes(
        &ec2,
        config,
        &tags,
        &group_id,
        NodeRole::Scheduler,
        &config.scheduler_instance_type,
        1,
        &node_user_data(config, &ca, &NodeSetup::Scheduler { spicepod_yaml })?,
    )
    .await?;
    cluster.instance_ids.extend(scheduler_ids.iter().cloned());
    wait_for_instance_state(&ec2, &scheduler_ids, "running").await?;
    let scheduler = describe_addresses(&ec2, &scheduler_ids)
        .await?
        .pop()
        .ok_or_else(|| anyhow::anyhow!("Scheduler instance has no addresses"))?;

    let executor_ids = launch_nodes(
        &ec2,
        config,
        &tags,
        &group_id,
        NodeRole::Executor,
        &config.executor_instance_type,
        config.executor_count,
        &node_user_data(
            config,
            &ca,
            &NodeSetup::Executor {
                scheduler_private_ip: scheduler.private_ip.clone(),
            },
        )?,
    )
    .await?;
    cluster.instance_ids.extend(executor_ids.iter().cloned());
    wait_for_instance_state(&ec2, &executor_ids, "running").await?;

    let http_url = format!("http://{}:{HTTP_PORT}", scheduler.public_ip);
    let ready_wait = ready_wait.max(MIN_READY_WAIT);
    wait_for_http_ok(&format!("{http_url}/health"), ready_wait).await?;
    let metrics_url = format!("http://{}:{METRICS_PORT}/metrics", scheduler.public_ip);
    wait_for_executors(&metrics_url, config.executor_count, ready_wait).await?;
    wait_for_http_ok(&format!("{http_url}/v1/ready"), ready_wait).await?;
    eprintln!(
        "[stdio] ec2 backend: cluster ready (scheduler {}, executors {})",
        scheduler.public_ip,
        executor_ids.join(", ")
    );

    Ok(RunState::Local(Box::new(LocalRunState {
        processes: LocalProcesses::Ec2Cluster(cluster),
        flight_url: format!("grpc://{}:{FLIGHT_PORT}", scheduler.public_ip),
        flight_api_key: None,
        sql_url: format!("{http_url}/v1/sql"),
        working_dir: ca.dir,
        storage: setup_config.storage.clone(),
        ec2_guards: vec![],
        dynamodb_guard: None,
        mongodb_guard: None,
    })))
}

async fn ec2_client(region: &str) -> Ec2Client {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_string()))
        .load()
        .await;
    Ec2Client::new(&config)
}

struct RunTags {
    run_id: String,
    github_run_id: String,
}

impl RunTags {
    fn new(run_id: Uuid) -> Self {
        Self {
            run_id: run_id.to_string(),
            github_run_id: std::env::var("GITHUB_RUN_ID").unwrap_or_else(|_| "local".to_string()),
        }
    }

    fn short_id(&self) -> &str {
        self.run_id.split('-').next().unwrap_or_default()
    }

    fn specification(
        &self,
        resource_type: ResourceType,
        name: &str,
        role: Option<NodeRole>,
    ) -> TagSpecification {
        let builder = TagSpecification::builder()
            .resource_type(resource_type)
            .tags(tag("Name", name))
            .tags(tag(PURPOSE_TAG, PURPOSE))
            .tags(tag("spice:run-id", &self.run_id))
            .tags(tag("spice:github-run-id", &self.github_run_id));
        match role {
            Some(role) => builder.tags(tag("spice:role", role.as_str())),
            None => builder,
        }
        .build()
    }
}

fn tag(key: &str, value: &str) -> Tag {
    Tag::builder().key(key).value(value).build()
}

/// The adapter reaches the scheduler from this address; nothing else may.
async fn runner_public_ip() -> anyhow::Result<String> {
    let ip = reqwest::get("https://checkip.amazonaws.com")
        .await?
        .error_for_status()?
        .text()
        .await?;
    let ip = ip.trim().to_string();
    ip.parse::<std::net::Ipv4Addr>()
        .map_err(|e| anyhow::anyhow!("checkip.amazonaws.com returned '{ip}': {e}"))?;
    Ok(ip)
}

async fn create_security_group(
    ec2: &Ec2Client,
    config: &Ec2ClusterConfig,
    tags: &RunTags,
    runner_ip: &str,
) -> anyhow::Result<String> {
    let subnets = ec2
        .describe_subnets()
        .subnet_ids(&config.subnet_id)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to describe subnet {}: {e:#?}", config.subnet_id))?;
    let vpc_id = subnets
        .subnets()
        .first()
        .and_then(|s| s.vpc_id())
        .ok_or_else(|| anyhow::anyhow!("Subnet {} has no VPC", config.subnet_id))?
        .to_string();

    let name = format!("spidapter-ballista-{}", tags.short_id());
    let group_id = ec2
        .create_security_group()
        .group_name(&name)
        .description("spidapter temporary Ballista benchmark cluster")
        .vpc_id(&vpc_id)
        .tag_specifications(tags.specification(ResourceType::SecurityGroup, &name, None))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create security group: {e:#?}"))?
        .group_id()
        .ok_or_else(|| anyhow::anyhow!("CreateSecurityGroup returned no group ID"))?
        .to_string();

    let runner_cidr = IpRange::builder()
        .cidr_ip(format!("{runner_ip}/32"))
        .build();
    let from_runner = |port: u16| {
        IpPermission::builder()
            .ip_protocol("tcp")
            .from_port(i32::from(port))
            .to_port(i32::from(port))
            .ip_ranges(runner_cidr.clone())
            .build()
    };
    let within_cluster = IpPermission::builder()
        .ip_protocol("-1")
        .user_id_group_pairs(UserIdGroupPair::builder().group_id(&group_id).build())
        .build();
    ec2.authorize_security_group_ingress()
        .group_id(&group_id)
        .ip_permissions(from_runner(HTTP_PORT))
        .ip_permissions(from_runner(FLIGHT_PORT))
        .ip_permissions(from_runner(METRICS_PORT))
        .ip_permissions(within_cluster)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to authorize ingress on {group_id}: {e:#?}"))?;
    eprintln!("[stdio] ec2 backend: security group {group_id} admits {runner_ip}");
    Ok(group_id)
}

#[derive(Clone, Copy)]
enum NodeRole {
    Scheduler,
    Executor,
}

impl NodeRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Scheduler => "scheduler",
            Self::Executor => "executor",
        }
    }
}

#[expect(clippy::too_many_arguments)]
async fn launch_nodes(
    ec2: &Ec2Client,
    config: &Ec2ClusterConfig,
    tags: &RunTags,
    group_id: &str,
    role: NodeRole,
    instance_type: &str,
    count: usize,
    user_data: &str,
) -> anyhow::Result<Vec<String>> {
    let count = i32::try_from(count)?;
    let name = format!("spidapter-ballista-{}-{}", tags.short_id(), role.as_str());
    let response = ec2
        .run_instances()
        .image_id(&config.ami_id)
        .instance_type(InstanceType::from(instance_type))
        .min_count(count)
        .max_count(count)
        .user_data(base64::engine::general_purpose::STANDARD.encode(user_data))
        .instance_initiated_shutdown_behavior(ShutdownBehavior::Terminate)
        .metadata_options(
            InstanceMetadataOptionsRequest::builder()
                .http_tokens(HttpTokensState::Required)
                .build(),
        )
        .block_device_mappings(
            BlockDeviceMapping::builder()
                .device_name("/dev/sda1")
                .ebs(
                    EbsBlockDevice::builder()
                        .volume_size(config.disk_size_gb)
                        .volume_type(VolumeType::Gp3)
                        .delete_on_termination(true)
                        .build(),
                )
                .build(),
        )
        .network_interfaces(
            InstanceNetworkInterfaceSpecification::builder()
                .device_index(0)
                .subnet_id(&config.subnet_id)
                .groups(group_id)
                .associate_public_ip_address(true)
                .build(),
        )
        .tag_specifications(tags.specification(ResourceType::Instance, &name, Some(role)))
        .tag_specifications(tags.specification(ResourceType::Volume, &name, Some(role)))
        .tag_specifications(tags.specification(ResourceType::NetworkInterface, &name, Some(role)))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to launch {name}: {e:#?}"))?;

    let ids: Vec<String> = response
        .instances()
        .iter()
        .filter_map(|i| i.instance_id().map(str::to_string))
        .collect();
    eprintln!("[stdio] ec2 backend: launched {name}: {}", ids.join(", "));
    Ok(ids)
}

async fn wait_for_instance_state(
    ec2: &Ec2Client,
    instance_ids: &[String],
    target: &str,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_mins(10);
    loop {
        // EC2 is eventually consistent: a new instance can be missing from
        // DescribeInstances for a few seconds, which reads as "not yet".
        let states: Vec<String> = match ec2
            .describe_instances()
            .set_instance_ids(Some(instance_ids.to_vec()))
            .send()
            .await
        {
            Ok(desc) => desc
                .reservations()
                .iter()
                .flat_map(aws_sdk_ec2::types::Reservation::instances)
                .filter_map(|i| i.state().and_then(|s| s.name()))
                .map(|n| n.as_str().to_string())
                .collect(),
            Err(e)
                if e.as_service_error().and_then(|se| se.meta().code())
                    == Some("InvalidInstanceID.NotFound") =>
            {
                Vec::new()
            }
            Err(e) => anyhow::bail!("Failed to describe instances: {e:#?}"),
        };

        if states.len() == instance_ids.len() && states.iter().all(|s| s == target) {
            return Ok(());
        }
        if target == "running"
            && states
                .iter()
                .any(|s| s == "terminated" || s == "shutting-down")
        {
            anyhow::bail!("An instance terminated before it was running: {states:?}");
        }
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!("Timed out waiting for instances to be {target}: {states:?}");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

struct NodeAddresses {
    public_ip: String,
    private_ip: String,
}

async fn describe_addresses(
    ec2: &Ec2Client,
    instance_ids: &[String],
) -> anyhow::Result<Vec<NodeAddresses>> {
    let desc = ec2
        .describe_instances()
        .set_instance_ids(Some(instance_ids.to_vec()))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to describe instances: {e:#?}"))?;
    desc.reservations()
        .iter()
        .flat_map(aws_sdk_ec2::types::Reservation::instances)
        .map(|i| {
            let id = i.instance_id().unwrap_or_default();
            Ok(NodeAddresses {
                public_ip: i
                    .public_ip_address()
                    .ok_or_else(|| anyhow::anyhow!("Instance {id} has no public IP"))?
                    .to_string(),
                private_ip: i
                    .private_ip_address()
                    .ok_or_else(|| anyhow::anyhow!("Instance {id} has no private IP"))?
                    .to_string(),
            })
        })
        .collect()
}

async fn wait_for_http_ok(url: &str, timeout: Duration) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let started = tokio::time::Instant::now();
    loop {
        if let Ok(response) = client.get(url).send().await
            && response.status().is_success()
        {
            eprintln!(
                "[stdio] ec2 backend: {url} ok after {}s",
                started.elapsed().as_secs()
            );
            return Ok(());
        }
        if started.elapsed() > timeout {
            anyhow::bail!("Timed out after {}s waiting for {url}", timeout.as_secs());
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn wait_for_executors(
    metrics_url: &str,
    expected: usize,
    timeout: Duration,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let started = tokio::time::Instant::now();
    loop {
        if let Ok(response) = client.get(metrics_url).send().await
            && response.status().is_success()
            && let Ok(body) = response.text().await
            && super::spice_local::scheduler_active_executor_count(&body)
                .is_some_and(|count| count >= expected)
        {
            return Ok(());
        }
        if started.elapsed() > timeout {
            anyhow::bail!(
                "Timed out after {}s waiting for {expected} executors to register at {metrics_url}",
                timeout.as_secs()
            );
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

struct RunCa {
    dir: std::path::PathBuf,
    cert_pem: String,
    key_pem: String,
}

async fn generate_ca(run_id: Uuid) -> anyhow::Result<RunCa> {
    let dir = std::env::temp_dir().join(format!("spidapter-ec2-{run_id}"));
    tokio::fs::create_dir_all(&dir).await?;
    let key = dir.join("ca.key");
    let cert = dir.join("ca.crt");
    run_openssl(&[
        "ecparam",
        "-genkey",
        "-name",
        "prime256v1",
        "-noout",
        "-out",
        &key.display().to_string(),
    ])
    .await?;
    run_openssl(&[
        "req",
        "-x509",
        "-new",
        "-key",
        &key.display().to_string(),
        "-subj",
        &format!("/CN=spidapter-ballista-{run_id}"),
        "-days",
        "2",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-out",
        &cert.display().to_string(),
    ])
    .await?;
    Ok(RunCa {
        cert_pem: tokio::fs::read_to_string(&cert).await?,
        key_pem: tokio::fs::read_to_string(&key).await?,
        dir,
    })
}

async fn run_openssl(args: &[&str]) -> anyhow::Result<()> {
    let output = TokioCommand::new("openssl").args(args).output().await?;
    if !output.status.success() {
        anyhow::bail!(
            "openssl {} failed: {}",
            args.first().unwrap_or(&""),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

enum NodeSetup {
    Scheduler { spicepod_yaml: String },
    Executor { scheduler_private_ip: String },
}

fn node_user_data(
    config: &Ec2ClusterConfig,
    ca: &RunCa,
    setup: &NodeSetup,
) -> anyhow::Result<String> {
    let mtls = "--node-mtls-ca-certificate-file /etc/spice/pki/ca.crt \
                --node-mtls-certificate-file /etc/spice/pki/node.crt \
                --node-mtls-key-file /etc/spice/pki/node.key";
    let listen = format!(
        "--http 0.0.0.0:{HTTP_PORT} --flight 0.0.0.0:{FLIGHT_PORT} \
         --node-bind-address 0.0.0.0:{NODE_PORT} --node-advertise-address \"$PRIVATE_IP\""
    );
    let (spicepod_file, spiced_args) = match setup {
        NodeSetup::Scheduler { spicepod_yaml } => (
            format!("cat > /etc/spice/spicepod.yaml <<'SPICEPOD'\n{spicepod_yaml}\nSPICEPOD\n"),
            format!(
                "--role scheduler {listen} --metrics 0.0.0.0:{METRICS_PORT} {mtls} /etc/spice/spicepod.yaml"
            ),
        ),
        NodeSetup::Executor {
            scheduler_private_ip,
        } => (
            String::new(),
            format!(
                "--role executor --scheduler-address https://{scheduler_private_ip}:{NODE_PORT} {listen} {mtls}"
            ),
        ),
    };

    let script = format!(
        r#"#!/bin/bash
set -euxo pipefail
shutdown -h +{max_lifetime}

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y docker.io mdadm openssl

# Instance-store NVMe disks (striped when there are several) hold /data.
mapfile -t disks < <(lsblk -dpno NAME,MODEL | awk '/Instance Storage/ {{print $1}}')
mkdir -p /data
if [ "${{#disks[@]}}" -gt 1 ]; then
  mdadm --create /dev/md0 --level=0 --raid-devices="${{#disks[@]}}" "${{disks[@]}}" --run
  mkfs.ext4 -F /dev/md0
  mount /dev/md0 /data
elif [ "${{#disks[@]}}" -eq 1 ]; then
  mkfs.ext4 -F "${{disks[0]}}"
  mount "${{disks[0]}}" /data
fi
mkdir -p /data/tmp
chown -R 65534:65534 /data

TOKEN=$(curl -sfX PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')
PRIVATE_IP=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

mkdir -p /etc/spice/pki
cat > /etc/spice/pki/ca.crt <<'CACERT'
{ca_cert}
CACERT
cat > /etc/spice/pki/ca.key <<'CAKEY'
{ca_key}
CAKEY
cd /etc/spice/pki
openssl ecparam -genkey -name prime256v1 -noout -out node.key
openssl req -new -key node.key -subj "/CN=$PRIVATE_IP" -out node.csr
printf 'subjectAltName=IP:%s\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature,keyEncipherment\n' "$PRIVATE_IP" > node.ext
openssl x509 -req -in node.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 -extfile node.ext -out node.crt
rm -f ca.key node.csr
chmod 0444 /etc/spice/pki/*
{spicepod_file}
docker pull {image}
docker run -d --name spiced --network host --ulimit nofile=1048576:1048576 \
  -e TMPDIR=/data/tmp -v /etc/spice:/etc/spice:ro -v /data:/data \
  {image} {spiced_args}
# The serial console (EC2 GetConsoleOutput) shows why a node failed to join.
systemd-run --unit spiced-console sh -c 'docker logs -f spiced > /dev/console 2>&1'
"#,
        max_lifetime = config.max_lifetime_minutes,
        ca_cert = ca.cert_pem.trim(),
        ca_key = ca.key_pem.trim(),
        image = config.image,
    );

    if script.len() > MAX_USER_DATA_BYTES {
        anyhow::bail!(
            "EC2 user data is {} bytes; the limit is {MAX_USER_DATA_BYTES}. Use a smaller spicepod.",
            script.len()
        );
    }
    Ok(script)
}
