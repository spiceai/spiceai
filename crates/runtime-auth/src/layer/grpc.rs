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

use crate::{AuthVerdict, GrpcAuth};
use std::sync::Arc;
use tonic::service::Interceptor;
use tonic::Status;

#[must_use]
pub fn make_interceptor(
    auth_verifier: Option<Arc<dyn GrpcAuth + Send + Sync>>,
) -> impl Interceptor + Send + Sync + Clone {
    move |req: tonic::Request<()>| {
        if let Some(auth_verifier) = &auth_verifier {
            match auth_verifier.grpc_verify(&req) {
                Ok(AuthVerdict::Allow(_)) => Ok(req),
                Ok(AuthVerdict::Deny) => Err(Status::unauthenticated("Invalid credentials")),
                Err(e) => {
                    tracing::error!("Error verifying credentials: {e}");
                    Err(tonic::Status::internal("Internal server error"))
                }
            }
        } else {
            Ok(req)
        }
    }
}
