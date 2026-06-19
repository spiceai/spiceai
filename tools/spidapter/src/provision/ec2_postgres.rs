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
use tokio_postgres::NoTls;

use crate::scenario::Ec2Spec;

const DEFAULT_PG_USER: &str = "postgres";
const DEFAULT_PG_DATABASE: &str = "spicebench";
const PG_PORT: u16 = 5432;

/// A provisioned EC2 instance running `PostgreSQL`.
pub(crate) struct Ec2PostgresInstance {
    pub(crate) instance_id: String,
    pub(crate) host: String,
    pub(crate) pg_user: String,
    pub(crate) pg_password: String,
    pub(crate) pg_database: String,
    pub(crate) pg_port: u16,
    pub(crate) region: String,
}

/// Launch an EC2 instance, install `PostgreSQL`, and wait until it accepts connections.
pub(crate) async fn launch_postgres_ec2(
    spec: &Ec2Spec,
    region: &str,
    run_id_short: &str,
) -> anyhow::Result<Ec2PostgresInstance> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_string()))
        .load()
        .await;
    let ec2 = Ec2Client::new(&config);

    let instance_type = InstanceType::from(spec.instance_type.as_str());

    let pg_password = uuid::Uuid::new_v4().to_string().replace('-', "");

    let user_data = postgres_user_data(&pg_password);
    let user_data_b64 = base64::engine::general_purpose::STANDARD.encode(user_data.as_bytes());

    let instance_name = format!("spidapter-postgres-{run_id_short}");
    eprintln!(
        "[stdio] EC2: launching PostgreSQL instance \
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
        .map_err(|e| anyhow::anyhow!("Failed to launch EC2 instance: {e:#?}"))?;

    let instance_id = run_result
        .instances()
        .first()
        .and_then(|i| i.instance_id())
        .ok_or_else(|| anyhow::anyhow!("EC2 RunInstances did not return an instance ID"))?
        .to_string();

    eprintln!("[stdio] EC2: instance {instance_id} launched, waiting for running state...");
    wait_for_instance_running(&ec2, &instance_id).await?;

    let host = if spec.associate_public_ip {
        get_instance_public_ip(&ec2, &instance_id).await?
    } else {
        get_instance_private_ip(&ec2, &instance_id).await?
    };
    eprintln!("[stdio] EC2: instance {instance_id} running at {host}");

    if !spec.iam_instance_profile.is_empty() {
        eprintln!(
            "[stdio] EC2: Session Manager console: \
             https://{region}.console.aws.amazon.com/systems-manager/session-manager/{instance_id}?region={region}"
        );
    }

    eprintln!("[stdio] EC2: waiting for PostgreSQL at {host}:{PG_PORT}...");
    wait_for_postgres(
        &host,
        PG_PORT,
        DEFAULT_PG_USER,
        &pg_password,
        DEFAULT_PG_DATABASE,
    )
    .await?;
    eprintln!("[stdio] EC2: PostgreSQL ready");

    Ok(Ec2PostgresInstance {
        instance_id,
        host,
        pg_user: DEFAULT_PG_USER.to_string(),
        pg_password,
        pg_database: DEFAULT_PG_DATABASE.to_string(),
        pg_port: PG_PORT,
        region: region.to_string(),
    })
}

/// Terminate a previously provisioned EC2 instance.
pub(crate) async fn terminate_ec2_instance(region: &str, instance_id: &str) -> anyhow::Result<()> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.to_string()))
        .load()
        .await;
    let ec2 = Ec2Client::new(&config);

    eprintln!("[stdio] EC2: terminating instance {instance_id}...");
    ec2.terminate_instances()
        .instance_ids(instance_id)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to terminate EC2 instance {instance_id}: {e}"))?;

    eprintln!("[stdio] EC2: instance {instance_id} termination requested");
    Ok(())
}

/// Build the cloud-init user-data script that installs and configures `PostgreSQL`.
fn postgres_user_data(pg_password: &str) -> String {
    format!(
        r#"#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
PG_PASSWORD='{pg_password}'

apt-get update -y
apt-get install -y curl ca-certificates gnupg lsb-release
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg
echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] \
    https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list
apt-get update -y
apt-get install -y postgresql-15

PG_CONF=$(find /etc/postgresql -name postgresql.conf | head -1)
PG_HBA=$(find /etc/postgresql -name pg_hba.conf | head -1)

# Ensure wal_level = logical (remove any existing setting first for idempotency)
sed -i '/^[[:space:]]*wal_level/d' "$PG_CONF"
echo 'wal_level = logical' >> "$PG_CONF"

# Listen on all interfaces
sed -i '/^[[:space:]]*listen_addresses/d' "$PG_CONF"
echo "listen_addresses = '*'" >> "$PG_CONF"

# Generous replication slot / sender limits
sed -i '/^[[:space:]]*max_replication_slots/d' "$PG_CONF"
echo 'max_replication_slots = 100' >> "$PG_CONF"
sed -i '/^[[:space:]]*max_wal_senders/d' "$PG_CONF"
echo 'max_wal_senders = 100' >> "$PG_CONF"

# Memory tuning: use available RAM for caching hot pages.
# shared_buffers = 25% of RAM; work_mem per-sort/hash op; effective_cache_size
# as a planner hint. Values are sized for instances with >= 16 GB RAM (e.g. r5.xlarge).
TOTAL_MEM_KB=$(grep MemTotal /proc/meminfo | awk '{{print $2}}')
SHARED_BUFFERS_KB=$(( TOTAL_MEM_KB / 4 ))
SHARED_BUFFERS_MB=$(( SHARED_BUFFERS_KB / 1024 ))
EFFECTIVE_CACHE_KB=$(( TOTAL_MEM_KB * 3 / 4 ))
EFFECTIVE_CACHE_MB=$(( EFFECTIVE_CACHE_KB / 1024 ))
sed -i '/^[[:space:]]*shared_buffers/d' "$PG_CONF"
echo "shared_buffers = ${{SHARED_BUFFERS_MB}}MB" >> "$PG_CONF"
sed -i '/^[[:space:]]*work_mem/d' "$PG_CONF"
echo 'work_mem = 256MB' >> "$PG_CONF"
sed -i '/^[[:space:]]*effective_cache_size/d' "$PG_CONF"
echo "effective_cache_size = ${{EFFECTIVE_CACHE_MB}}MB" >> "$PG_CONF"

# Parallel query: allow planner to use multiple cores for large scans/joins.
sed -i '/^[[:space:]]*max_parallel_workers_per_gather/d' "$PG_CONF"
echo 'max_parallel_workers_per_gather = 4' >> "$PG_CONF"
sed -i '/^[[:space:]]*max_parallel_workers/d' "$PG_CONF"
echo 'max_parallel_workers = 8' >> "$PG_CONF"

# Allow remote connections for all users and replication
echo "host all             all        0.0.0.0/0 md5" >> "$PG_HBA"
echo "host replication     all        0.0.0.0/0 md5" >> "$PG_HBA"

systemctl restart postgresql

sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD '$PG_PASSWORD';"
sudo -u postgres psql -c "CREATE DATABASE {DEFAULT_PG_DATABASE};"
"#
    )
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
                // EC2 has eventual consistency — the instance may not be visible
                // immediately after RunInstances returns. Retry on NotFound.
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

async fn wait_for_postgres(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_mins(10);

    loop {
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!("Timed out waiting for PostgreSQL at {host}:{port}");
        }

        let config_str = format!(
            "host={host} port={port} user={user} password={password} dbname={database} connect_timeout=5"
        );

        match tokio_postgres::connect(&config_str, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    drop(conn);
                });
                drop(client);
                return Ok(());
            }
            Err(e) => {
                eprintln!("[stdio] EC2: PostgreSQL not ready yet ({e}), retrying...");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}
