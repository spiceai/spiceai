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

use crate::args::StdioArgs;

const MONGO_PORT: u16 = 27017;

/// A provisioned EC2 instance running `MongoDB`.
pub(crate) struct Ec2MongoInstance {
    pub(crate) instance_id: String,
    pub(crate) connection_string: String,
    pub(crate) database: String,
    pub(crate) region: String,
}

/// Launch an EC2 instance, install `MongoDB` 7.0, and wait until it accepts connections.
pub(crate) async fn launch_mongo_ec2(
    args: &StdioArgs,
    run_id_short: &str,
) -> anyhow::Result<Ec2MongoInstance> {
    let region = args
        .aws_region
        .clone()
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region.clone()))
        .load()
        .await;
    let ec2 = Ec2Client::new(&config);

    let subnet_id = args
        .ec2_subnet_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("EC2_SUBNET_ID is required for EC2 mode"))?;
    let sg_id = args
        .ec2_security_group_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("EC2_SECURITY_GROUP_ID is required for EC2 mode"))?;
    let ami_id = args
        .ec2_ami_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("EC2_AMI_ID is required for EC2 mode"))?;
    let instance_type = InstanceType::from(args.ec2_instance_type.as_str());

    let mongo_password = uuid::Uuid::new_v4().to_string().replace('-', "");

    let user_data = mongo_user_data(&mongo_password);
    let user_data_b64 = base64::engine::general_purpose::STANDARD.encode(user_data.as_bytes());

    let instance_name = format!("spidapter-mongo-{run_id_short}");
    eprintln!(
        "[stdio] EC2: launching MongoDB instance \
         (name={instance_name}, ami={ami_id}, type={}, subnet={subnet_id})",
        args.ec2_instance_type
    );

    let mut run_req = ec2
        .run_instances()
        .image_id(ami_id)
        .instance_type(instance_type)
        .min_count(1)
        .max_count(1)
        .user_data(user_data_b64)
        .block_device_mappings(
            BlockDeviceMapping::builder()
                .device_name("/dev/sda1")
                .ebs(
                    EbsBlockDevice::builder()
                        .volume_size(100)
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

    if let Some(profile) = &args.ec2_iam_instance_profile {
        let spec = if profile.starts_with("arn:") {
            IamInstanceProfileSpecification::builder()
                .arn(profile)
                .build()
        } else {
            IamInstanceProfileSpecification::builder()
                .name(profile)
                .build()
        };
        run_req = run_req.iam_instance_profile(spec);
    }

    if args.ec2_associate_public_ip {
        run_req = run_req.network_interfaces(
            InstanceNetworkInterfaceSpecification::builder()
                .device_index(0)
                .subnet_id(subnet_id)
                .groups(sg_id)
                .associate_public_ip_address(true)
                .build(),
        );
    } else {
        run_req = run_req.subnet_id(subnet_id).security_group_ids(sg_id);
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

    let host = if args.ec2_associate_public_ip {
        get_instance_public_ip(&ec2, &instance_id).await?
    } else {
        get_instance_private_ip(&ec2, &instance_id).await?
    };
    eprintln!("[stdio] EC2: instance {instance_id} running at {host}");

    if args.ec2_iam_instance_profile.is_some() {
        eprintln!(
            "[stdio] EC2: Session Manager console: \
             https://{region}.console.aws.amazon.com/systems-manager/session-manager/{instance_id}?region={region}"
        );
    }

    let connection_string = format!(
        "mongodb://spidapter:{mongo_password}@{host}:{MONGO_PORT}/spicebench?authSource=admin&replicaSet=rs0&directConnection=true"
    );

    eprintln!("[stdio] EC2: waiting for MongoDB at {host}:{MONGO_PORT}...");
    wait_for_mongo(&connection_string).await?;
    eprintln!("[stdio] EC2: MongoDB ready");

    Ok(Ec2MongoInstance {
        instance_id,
        connection_string,
        database: "spicebench".to_string(),
        region,
    })
}

/// Build the cloud-init user-data script that installs and configures `MongoDB` 7.0.
///
/// Replica set `rs0` is required for change streams.
fn mongo_user_data(mongo_password: &str) -> String {
    format!(
        r#"#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MONGO_PASSWORD='{mongo_password}'

# Install MongoDB 7.0 from official repo
apt-get update -y
apt-get install -y gnupg curl ca-certificates
curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc \
    | gpg --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" \
    > /etc/apt/sources.list.d/mongodb-org-7.0.list
apt-get update -y
apt-get install -y mongodb-org

# Configure: bind all interfaces, enable replication (required for change streams), no auth yet
cat > /etc/mongod.conf << 'MONGODCONF'
storage:
  dbPath: /var/lib/mongodb
systemLog:
  destination: file
  logAppend: true
  path: /var/log/mongodb/mongod.log
net:
  port: 27017
  bindIp: 0.0.0.0
replication:
  replSetName: "rs0"
MONGODCONF

systemctl start mongod
systemctl enable mongod

# Wait for mongod to start (up to 60s)
for i in $(seq 1 60); do
    mongosh --quiet --eval 'db.adminCommand({{ping: 1}})' > /dev/null 2>&1 && break || true
    sleep 2
done

# Initiate single-node replica set and wait for primary election
mongosh --quiet --eval 'rs.initiate({{_id: "rs0", members: [{{_id: 0, host: "localhost:27017"}}]}})'
sleep 15

# Create admin user (no auth enforced yet)
mongosh --quiet admin --eval 'db.createUser({{user: "admin", pwd: "{mongo_password}", roles: ["root"]}})'

# Enable authentication and restart
cat > /etc/mongod.conf << 'MONGODCONF'
storage:
  dbPath: /var/lib/mongodb
systemLog:
  destination: file
  logAppend: true
  path: /var/log/mongodb/mongod.log
net:
  port: 27017
  bindIp: 0.0.0.0
security:
  authorization: enabled
replication:
  replSetName: "rs0"
MONGODCONF

systemctl restart mongod
sleep 10

# Create application user in admin database (authSource=admin in connection string)
mongosh --quiet -u admin -p '{mongo_password}' --authenticationDatabase admin admin --eval '
db.createUser({{
  user: "spidapter",
  pwd: "{mongo_password}",
  roles: [{{role: "readWrite", db: "spicebench"}}]
}});
'
"#
    )
}

async fn wait_for_instance_running(ec2: &Ec2Client, instance_id: &str) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
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
                let is_not_found = e
                    .as_service_error()
                    .and_then(|se| se.meta().code()) == Some("InvalidInstanceID.NotFound");
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

async fn wait_for_mongo(connection_string: &str) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
    loop {
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!("Timed out waiting for MongoDB");
        }
        match mongodb::Client::with_uri_str(connection_string).await {
            Ok(client) => {
                match client
                    .database("admin")
                    .run_command(mongodb::bson::doc! {"ping": 1})
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(e) => eprintln!("[stdio] EC2: MongoDB not ready yet ({e}), retrying..."),
                }
            }
            Err(e) => eprintln!("[stdio] EC2: MongoDB client error ({e}), retrying..."),
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}
