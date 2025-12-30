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

use clap::{ArgAction, ValueEnum};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[cfg(feature = "cluster")]
use url::Url;

#[derive(Debug, Clone, clap::Parser)]
pub struct Config {
    /// Configure runtime HTTP address.
    #[arg(
        long = "http",
        value_name = "BIND_ADDRESS",
        default_value = "127.0.0.1:8090",
        action
    )]
    pub http_bind_address: SocketAddr,

    /// Configure runtime Flight address.
    #[arg(
        long = "flight",
        value_name = "FLIGHT_BIND_ADDRESS",
        default_value = "127.0.0.1:50051",
        action
    )]
    pub flight_bind_address: SocketAddr,

    /// All cluster related arguments
    #[cfg(feature = "cluster")]
    #[clap(flatten)]
    pub cluster: ClusterConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ClusterRole {
    Scheduler,
    Executor,
}

impl Config {
    #[must_use]
    pub fn new() -> Self {
        Self {
            http_bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8090),
            flight_bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 50051),
            #[cfg(feature = "cluster")]
            cluster: ClusterConfig::default(),
        }
    }

    #[must_use]
    pub fn with_http_bind_address(mut self, bind_addr: SocketAddr) -> Self {
        self.http_bind_address = bind_addr;
        self
    }

    #[must_use]
    pub fn with_flight_bind_address(mut self, bind_addr: SocketAddr) -> Self {
        self.flight_bind_address = bind_addr;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "cluster")]
#[derive(Debug, Clone, clap::Parser)]
pub struct ClusterConfig {
    /// Configure cluster node role: scheduler or executor
    #[arg(long = "role", value_name = "ROLE", required = false, action)]
    pub role: Option<ClusterRole>,

    /// The bind address for the internal cluster gRPC service.
    /// Used by both schedulers and executors.
    #[arg(
        long = "node-bind-address",
        value_name = "NODE_BIND_ADDRESS",
        default_value = "0.0.0.0:50052",
        action
    )]
    pub node_bind_address: SocketAddr,

    /// The path to the CA certificate used to validate cluster node identities.
    #[arg(
        long = "node-mtls-ca-certificate-file",
        value_name = "NODE_MTLS_CA_CERTIFICATE_FILE"
    )]
    pub node_mtls_ca_certificate_file: Option<String>,

    /// The path to the certificate file used for both server TLS and client mTLS.
    #[arg(
        long = "node-mtls-certificate-file",
        value_name = "NODE_MTLS_CERTIFICATE_FILE"
    )]
    pub node_mtls_certificate_file: Option<String>,

    /// The path to the private key file for the cluster certificate.
    #[arg(long = "node-mtls-key-file", value_name = "NODE_MTLS_KEY_FILE")]
    pub node_mtls_key_file: Option<String>,

    /// Allow insecure cluster communication without mTLS. WARNING: Only use this flag in development or testing environments and never in production.
    #[arg(long = "insecure", default_value_t = false, action = ArgAction::SetTrue)]
    pub insecure: bool,

    /// The URL of the scheduler service. Required for executors to join a cluster.
    /// If set, --role executor is implied and can be omitted.
    #[arg(long = "scheduler-address", value_name = "SCHEDULER_ADDRESS")]
    pub scheduler_address: Option<Url>,

    /// The hostname and port that this node advertises to other cluster nodes.
    /// For schedulers: used as the URL for distributed query planning.
    /// For executors: used during registration to tell the scheduler how to contact this node.
    #[arg(long = "node-advertise-address", value_name = "NODE_ADVERTISE_ADDRESS")]
    pub node_advertise_address: Option<String>,
}

#[cfg(feature = "cluster")]
impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            role: None,
            node_bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 50052),
            node_mtls_ca_certificate_file: None,
            node_mtls_certificate_file: None,
            node_mtls_key_file: None,
            insecure: false,
            scheduler_address: None,
            node_advertise_address: None,
        }
    }
}

#[cfg(feature = "cluster")]
impl ClusterConfig {
    #[must_use]
    pub fn with_role(mut self, role: ClusterRole) -> Self {
        self.role = Some(role);
        self
    }

    #[must_use]
    pub fn with_node_bind_address(mut self, addr: SocketAddr) -> Self {
        self.node_bind_address = addr;
        self
    }

    #[must_use]
    pub fn with_scheduler_address(mut self, url: Url) -> Self {
        self.scheduler_address = Some(url);
        self
    }
}
