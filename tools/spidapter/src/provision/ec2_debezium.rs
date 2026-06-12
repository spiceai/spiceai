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

use std::time::Duration;

use aws_config::Region;
use aws_sdk_ec2::Client as Ec2Client;
use aws_sdk_ec2::types::{
    BlockDeviceMapping, EbsBlockDevice, IamInstanceProfileSpecification,
    InstanceNetworkInterfaceSpecification, InstanceType, ResourceType, Tag, TagSpecification,
    VolumeType,
};
use base64::Engine as _;

use crate::scenario::Ec2Spec;

const KAFKA_PORT: u16 = 9092;
const CONNECT_PORT: u16 = 8083;

/// A provisioned EC2 instance running Redpanda (Kafka-compatible) and Debezium Connect.
pub(crate) struct Ec2DebeziumInstance {
    pub(crate) instance_id: String,
    /// Bootstrap server string for Kafka clients: `"{host}:9092"`.
    pub(crate) kafka_brokers: String,
    /// Debezium Connect REST endpoint: `"http://{host}:8083"`.
    pub(crate) connect_url: String,
    pub(crate) region: String,
}

/// Launch an EC2 instance, install Redpanda + Debezium Connect, and wait until both are ready.
pub(crate) async fn launch_ec2_debezium(
    spec: &Ec2Spec,
    region: &str,
    run_id_short: &str,
) -> anyhow::Result<Ec2DebeziumInstance> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_string()))
        .load()
        .await;
    let ec2 = Ec2Client::new(&config);

    let instance_type = InstanceType::from(spec.instance_type.as_str());

    let user_data = debezium_user_data();
    let user_data_b64 = base64::engine::general_purpose::STANDARD.encode(user_data.as_bytes());

    let instance_name = format!("spidapter-debezium-{run_id_short}");
    eprintln!(
        "[stdio] EC2: launching Debezium instance \
         (name={instance_name}, ami={}, type={}, subnet={}, disk={}GB)",
        spec.ami_id, spec.instance_type, spec.subnet_id, spec.disk_size_gb
    );

    let mut run_req = ec2
        .run_instances()
        .image_id(&spec.ami_id)
        .instance_type(instance_type)
        .min_count(1)
        .max_count(1)
        .user_data(user_data_b64)
        .block_device_mappings(
            BlockDeviceMapping::builder()
                .device_name("/dev/sda1")
                .ebs(
                    EbsBlockDevice::builder()
                        .volume_size(spec.disk_size_gb)
                        .volume_type(VolumeType::Gp3)
                        .delete_on_termination(true)
                        .build(),
                )
                .build(),
        )
        .tag_specifications(
            TagSpecification::builder()
                .resource_type(ResourceType::Instance)
                .tags(Tag::builder().key("Name").value(&instance_name).build())
                .build(),
        );

    if !spec.iam_instance_profile.is_empty() {
        let profile = &spec.iam_instance_profile;
        let ispec = if profile.starts_with("arn:") {
            IamInstanceProfileSpecification::builder()
                .arn(profile)
                .build()
        } else {
            IamInstanceProfileSpecification::builder()
                .name(profile)
                .build()
        };
        run_req = run_req.iam_instance_profile(ispec);
    }

    if spec.associate_public_ip {
        run_req = run_req.network_interfaces(
            InstanceNetworkInterfaceSpecification::builder()
                .device_index(0)
                .subnet_id(&spec.subnet_id)
                .groups(&spec.security_group_id)
                .associate_public_ip_address(true)
                .build(),
        );
    } else {
        run_req = run_req
            .subnet_id(&spec.subnet_id)
            .security_group_ids(&spec.security_group_id);
    }

    let run_result = run_req
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to launch Debezium EC2 instance: {e:#?}"))?;

    let instance_id = run_result
        .instances()
        .first()
        .and_then(|i| i.instance_id())
        .ok_or_else(|| anyhow::anyhow!("EC2 RunInstances did not return an instance ID"))?
        .to_string();

    eprintln!(
        "[stdio] EC2 Debezium: instance {instance_id} launched, waiting for running state..."
    );
    wait_for_instance_running(&ec2, &instance_id).await?;

    let host = if spec.associate_public_ip {
        get_instance_public_ip(&ec2, &instance_id).await?
    } else {
        get_instance_private_ip(&ec2, &instance_id).await?
    };
    eprintln!("[stdio] EC2 Debezium: instance {instance_id} running at {host}");

    eprintln!("[stdio] EC2 Debezium: waiting for Debezium Connect at {host}:{CONNECT_PORT}...");
    wait_for_debezium_connect(&host, CONNECT_PORT).await?;
    eprintln!("[stdio] EC2 Debezium: Debezium Connect ready");

    Ok(Ec2DebeziumInstance {
        instance_id,
        kafka_brokers: format!("{host}:{KAFKA_PORT}"),
        connect_url: format!("http://{host}:{CONNECT_PORT}"),
        region: region.to_string(),
    })
}

/// Cloud-init user-data that installs Docker, Redpanda (Kafka-compatible broker),
/// and Debezium Connect on a fresh Ubuntu instance.
///
/// `$PUBLIC_IP` inside the heredoc is expanded by the bash shell at runtime using
/// the EC2 instance metadata API, so external Kafka clients can reach the broker.
fn debezium_user_data() -> String {
    // Language: bash — unquoted heredoc so $PUBLIC_IP is expanded at runtime
    r#"#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get install -y docker.io docker-compose curl

systemctl enable docker
systemctl start docker
until docker info >/dev/null 2>&1; do sleep 2; done

# Fetch the public IP so Redpanda can advertise it to external Kafka clients.
PUBLIC_IP=$(curl -s --max-time 10 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null \
    || hostname -I | awk '{print $1}')

mkdir -p /opt/debezium

# $PUBLIC_IP is expanded by the shell (unquoted YAML heredoc).
cat > /opt/debezium/docker-compose.yml <<YAML
version: "3.8"
services:
  redpanda:
    image: redpandadata/redpanda:v24.1.13
    command:
      - redpanda
      - start
      - --mode
      - dev-container
      - --kafka-addr
      - INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
      - --advertise-kafka-addr
      - INTERNAL://redpanda:29092,EXTERNAL://$PUBLIC_IP:9092
    ports:
      - "9092:9092"
      - "29092:29092"

  debezium:
    image: quay.io/debezium/connect:2.7
    depends_on:
      - redpanda
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: redpanda:29092
      GROUP_ID: spicebench
      CONFIG_STORAGE_TOPIC: debezium_config
      OFFSET_STORAGE_TOPIC: debezium_offsets
      STATUS_STORAGE_TOPIC: debezium_status
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
YAML

cd /opt/debezium
docker-compose up -d
"#
    .to_string()
}

async fn wait_for_debezium_connect(host: &str, port: u16) -> anyhow::Result<()> {
    let url = format!("http://{host}:{port}/connectors");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;

    let timeout = Duration::from_mins(10);
    let started = tokio::time::Instant::now();

    loop {
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                eprintln!(
                    "[stdio] EC2 Debezium: Connect not ready (status={}), retrying...",
                    resp.status()
                );
            }
            Err(e) => {
                eprintln!("[stdio] EC2 Debezium: Connect not reachable ({e}), retrying...");
            }
        }

        if started.elapsed() > timeout {
            anyhow::bail!(
                "Timed out after {}s waiting for Debezium Connect at {url}",
                timeout.as_secs()
            );
        }

        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}

async fn wait_for_instance_running(ec2: &Ec2Client, instance_id: &str) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_mins(5);

    loop {
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!(
                "Timed out waiting for EC2 instance {instance_id} to reach running state"
            );
        }

        let result = ec2
            .describe_instances()
            .instance_ids(instance_id)
            .send()
            .await;

        match result {
            Err(e) => {
                let is_not_found = e.as_service_error().and_then(|se| se.meta().code())
                    == Some("InvalidInstanceID.NotFound");
                if !is_not_found {
                    return Err(anyhow::anyhow!(
                        "Failed to describe EC2 instance {instance_id}: {e:#?}"
                    ));
                }
                eprintln!("[stdio] EC2: instance {instance_id} not yet visible, retrying...");
            }
            Ok(desc) => {
                let state_name = desc
                    .reservations()
                    .first()
                    .and_then(|r| r.instances().first())
                    .and_then(|i| i.state())
                    .and_then(|s| s.name())
                    .map(|n| n.as_str().to_string());

                match state_name.as_deref() {
                    Some("running") => return Ok(()),
                    Some("terminated" | "shutting-down") => {
                        anyhow::bail!(
                            "EC2 instance {instance_id} reached terminal state unexpectedly"
                        );
                    }
                    _ => {}
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn get_instance_public_ip(ec2: &Ec2Client, instance_id: &str) -> anyhow::Result<String> {
    let desc = ec2
        .describe_instances()
        .instance_ids(instance_id)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to describe EC2 instance {instance_id}: {e:#?}"))?;

    desc.reservations()
        .first()
        .and_then(|r| r.instances().first())
        .and_then(|i| i.public_ip_address())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("EC2 instance {instance_id} has no public IP address"))
}

async fn get_instance_private_ip(ec2: &Ec2Client, instance_id: &str) -> anyhow::Result<String> {
    let desc = ec2
        .describe_instances()
        .instance_ids(instance_id)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to describe EC2 instance {instance_id}: {e:#?}"))?;

    desc.reservations()
        .first()
        .and_then(|r| r.instances().first())
        .and_then(|i| i.private_ip_address())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("EC2 instance {instance_id} has no private IP address"))
}
