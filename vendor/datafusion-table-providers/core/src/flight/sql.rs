// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Default [FlightDriver] for Flight SQL

use std::collections::HashMap;

use arrow_flight::error::Result;
use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use tonic::transport::Channel;

use crate::flight::{FlightDriver, FlightMetadata, FlightProperties};

pub const QUERY: &str = "flight.sql.query";
pub const USERNAME: &str = "flight.sql.username";
pub const PASSWORD: &str = "flight.sql.password";
pub const HEADER_PREFIX: &str = "flight.sql.header.";

/// Default Flight SQL driver. Requires a [QUERY] to be passed as a table option.
/// If [USERNAME] (and optionally [PASSWORD]) are passed,
/// will perform the `Handshake` using basic authentication.
/// Any additional headers for the `GetFlightInfo` call can be passed as table options
/// using the [HEADER_PREFIX] prefix.
/// If a token is returned by the server with the handshake response, it will be
/// stored as a gRPC authorization header within the returned [FlightMetadata],
/// to be sent with the subsequent `DoGet` requests.
#[derive(Clone, Debug, Default)]
pub struct FlightSqlDriver {
    properties_template: FlightProperties,
    persistent_headers: bool,
}

impl FlightSqlDriver {
    pub fn new() -> Self {
        Default::default()
    }

    /// Custom flight properties to be returned from the metadata call instead of the default ones.
    /// The headers (if any) will only be used for the Handshake/GetFlightInfo calls by default.
    /// This behaviour can be changed by calling [Self::with_persistent_headers] below.
    /// Headers provided as options for the metadata call will overwrite the template ones.
    pub fn with_properties_template(mut self, properties_template: FlightProperties) -> Self {
        self.properties_template = properties_template;
        self
    }

    /// Propagate the static headers configured for Handshake/GetFlightInfo to the subsequent DoGet calls.
    pub fn with_persistent_headers(mut self, persistent_headers: bool) -> Self {
        self.persistent_headers = persistent_headers;
        self
    }
}

#[async_trait]
impl FlightDriver for FlightSqlDriver {
    async fn metadata(
        &self,
        channel: Channel,
        options: &HashMap<String, String>,
    ) -> Result<FlightMetadata> {
        let mut client = FlightSqlServiceClient::new(channel);
        let mut handshake_headers = self.properties_template.grpc_headers.clone();
        let headers_overlay = options.iter().filter_map(|(key, value)| {
            key.strip_prefix(HEADER_PREFIX)
                .map(|header_name| (header_name.to_owned(), value.to_owned()))
        });
        handshake_headers.extend(headers_overlay);
        for (name, value) in &handshake_headers {
            client.set_header(name, value)
        }
        if let Some(username) = options.get(USERNAME) {
            let default_password = "".to_string();
            let password = options.get(PASSWORD).unwrap_or(&default_password);
            client.handshake(username, password).await.ok();
        }
        let info = client.execute(options[QUERY].clone(), None).await?;
        let mut partition_headers = if self.persistent_headers {
            handshake_headers
        } else {
            HashMap::default()
        };
        if let Some(token) = client.token() {
            partition_headers.insert("authorization".into(), format!("Bearer {token}"));
        }
        let props = self
            .properties_template
            .clone()
            .with_grpc_headers(partition_headers);
        FlightMetadata::try_new(info, props)
    }
}
