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

//! The AWS identity of a Spice workload: the SDK configuration every AWS component starts from,
//! the credential chain it uses ([`named_chain`]), and that identity as the identity token
//! another provider's federation exchanges ([`identity_token`]).

use std::sync::LazyLock;

use aws_config::{AppName, BehaviorVersion};

pub mod identity_token;
pub mod named_chain;

/// The APN user-agent string for Spice.
///
/// This is set on all AWS SDK configurations to identify Spice as an AWS Partner Network (APN)
/// application in the user-agent header of AWS API requests.
///
/// The `AppName::new` call is infallible for this input: the name contains only alphanumeric
/// characters plus `.` and `-`, all of which are permitted. The name is truncated to 50
/// characters to stay within the AWS SDK's recommended limit.
static APN_APP_NAME: LazyLock<AppName> = LazyLock::new(|| {
    let version = env!("CARGO_PKG_VERSION");
    let mut name = format!("Spice-{version}");
    name.truncate(50);
    match AppName::new(name) {
        Ok(name) => name,
        Err(_) => unreachable!("Spice version string should always be a valid AppName"),
    }
});

/// Returns the APN [`AppName`] for Spice, suitable for use in AWS SDK configurations.
#[must_use]
pub fn apn_app_name() -> &'static AppName {
    &APN_APP_NAME
}

/// Returns a default AWS SDK configuration with the latest behavior version and
/// the Spice APN user-agent.
///
/// This is a convenience function to ensure all AWS SDK configuration uses
/// the same behavior version and APN identification consistently across the codebase.
#[must_use]
pub fn default_aws_config() -> aws_config::ConfigLoader {
    aws_config::defaults(BehaviorVersion::v2026_01_12()).app_name(APN_APP_NAME.clone())
}
