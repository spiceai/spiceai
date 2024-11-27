/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow_flight::flight_service_server::FlightService;
use tonic::{
    metadata::{Ascii, MetadataValue},
    Request, Response,
};

use crate::flight::Service;

pub fn attach_cache_metadata(
    response: &mut Response<<Service as FlightService>::DoGetStream>,
    from_cache: Option<bool>,
) {
    if let Some(from_cache) = from_cache {
        let val: Result<MetadataValue<Ascii>, _> = if from_cache {
            "Hit from spiceai".parse()
        } else {
            "Miss from spiceai".parse()
        };
        match val {
            Ok(val) => {
                response.metadata_mut().insert("x-cache", val);
            }
            Err(e) => {
                tracing::error!("Failed to parse metadata value: {}", e);
            }
        }
    }
}

pub fn extract_flight_user_agent<T>(request: &Request<T>) -> SpiceUserAgent {
    let user_agent_string = request
        .metadata()
        .get("user-agent")
        .map(|ua| ua.to_str().unwrap_or(""))
        .unwrap_or_default()
        .to_string();

    let mut user_agent = SpiceUserAgent::try_from(user_agent_string).unwrap_or_else(|_| {
        SpiceUserAgent::default()
            .with_client_name("Flight")
            .with_client_version("1.0")
            .with_client_system("gRPC")
    });

    if user_agent.client_system.is_none() {
        user_agent = user_agent.with_client_system("gRPC");
    }
    user_agent
}
