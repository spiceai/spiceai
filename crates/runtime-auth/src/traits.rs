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

use crate::error::Error;
use axum::http;

pub enum AuthVerdict {
    Allow,
    Deny,
}

pub trait HttpAuth {
    /// Receive the entire HTTP request object and return a verdict on whether to allow/deny it
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the request.
    fn http(&self, request: &http::request::Parts) -> Result<AuthVerdict, Error>;
}

pub trait FlightBasicAuth {
    // Receive the username/password for Flight basic auth and return a verdict
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the request.
    fn flight_basic(&self, username: String, password: String) -> Result<AuthVerdict, Error>;
}

pub trait GrpcAuth {
    // Receive the entire gRPC request object and return a verdict
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the request.
    fn grpc(&self, req: &http::request::Parts) -> Result<AuthVerdict, Error>;
}
