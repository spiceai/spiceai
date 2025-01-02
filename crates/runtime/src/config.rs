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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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

    /// Configure runtime OpenTelemetry address.
    #[arg(
        long = "open_telemetry",
        value_name = "OPEN_TELEMETRY_BIND_ADDRESS",
        default_value = "127.0.0.1:50052",
        action
    )]
    pub open_telemetry_bind_address: SocketAddr,
}

impl Config {
    #[must_use]
    pub fn new() -> Self {
        Self {
            http_bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8090),
            flight_bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051),
            open_telemetry_bind_address: SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                50052,
            ),
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

    #[must_use]
    pub fn with_open_telemetry_bind_address(mut self, bind_addr: SocketAddr) -> Self {
        self.open_telemetry_bind_address = bind_addr;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}
