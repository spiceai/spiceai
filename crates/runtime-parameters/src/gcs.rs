/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The GCS authentication rule shared by the GCS data connector and the
//! object-store locations that take its parameter names.

use snafu::prelude::*;

use crate::Parameters;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Multiple authentication methods were provided. Specify only one of the following: gcs_service_account_path, gcs_service_account_key, gcs_application_default_credentials, or gcs_skip_signature. For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#auth"
    ))]
    MultipleAuthMethods,
}

/// Ensures at most one GCS authentication method is set in `parameters`. Their keys
/// carry no `gcs_` prefix (`service_account_path`, `skip_signature`, ...), as
/// [`Parameters`] built for the `gcs` prefix stores them.
///
/// # Errors
///
/// Returns [`Error::MultipleAuthMethods`] when more than one method is set.
pub fn validate_auth(parameters: &Parameters) -> Result<(), Error> {
    // Check for each authentication method
    let has_service_account_path = parameters
        .get("service_account_path")
        .expose()
        .ok()
        .is_some();
    let has_service_account_key = parameters
        .get("service_account_key")
        .expose()
        .ok()
        .is_some();

    // skip_signature must be explicitly "true" to count as an auth method
    let has_skip_signature = parameters
        .get("skip_signature")
        .expose()
        .ok()
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    // application_default_credentials must be explicitly "true" to count as an auth method
    let has_application_default_credentials = parameters
        .get("application_default_credentials")
        .expose()
        .ok()
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    // Count active authentication methods
    let auth_method_count = [
        has_service_account_path,
        has_service_account_key,
        has_skip_signature,
        has_application_default_credentials,
    ]
    .iter()
    .filter(|&&b| b)
    .count();

    if auth_method_count > 1 {
        return Err(Error::MultipleAuthMethods);
    }

    Ok(())
}
