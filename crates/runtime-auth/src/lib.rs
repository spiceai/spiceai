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

use std::sync::Arc;

use app::App;

pub mod api_key;
pub mod error;
mod traits;

pub use traits::*;

pub struct EndpointAuth {
    pub http_auth: Option<Arc<dyn HttpAuth + Send + Sync>>,
    pub flight_basic_auth: Option<Arc<dyn FlightBasicAuth + Send + Sync>>,
    pub grpc_auth: Option<Arc<dyn GrpcAuth + Send + Sync>>,
}

impl EndpointAuth {
    #[must_use]
    pub fn new(app: &App) -> Self {
        Self {
            http_auth: http_auth(app),
            flight_basic_auth: flight_basic_auth(app),
            grpc_auth: grpc_auth(app),
        }
    }

    #[must_use]
    pub fn no_auth() -> Self {
        Self {
            http_auth: None,
            flight_basic_auth: None,
            grpc_auth: None,
        }
    }
}

/// Gets the HTTP auth provider configured for the app, if any
#[must_use]
fn http_auth(_app: &App) -> Option<Arc<dyn HttpAuth + Send + Sync>> {
    None
}

#[must_use]
fn flight_basic_auth(_app: &App) -> Option<Arc<dyn FlightBasicAuth + Send + Sync>> {
    None
}

#[must_use]
fn grpc_auth(_app: &App) -> Option<Arc<dyn GrpcAuth + Send + Sync>> {
    None
}
