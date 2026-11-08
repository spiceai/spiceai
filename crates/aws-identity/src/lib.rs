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

use aws_config::meta::region::ProvideRegion;
use aws_config::timeout::TimeoutConfig;
use aws_config::{AppName, BehaviorVersion, ConfigLoader, SdkConfig};
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};

pub mod identity_token;
pub mod named_chain;

/// The region STS is called in when none is configured. `GetWebIdentityToken` is served only by
/// regional endpoints, and any of them assumes a role of any region.
pub(crate) const DEFAULT_REGION: &str = "us-east-1";

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

/// The AWS SDK configuration every AWS component starts from: the behavior version Spice is
/// tested with, the Spice APN user-agent, and the credential chain every AWS component uses
/// ([`named_chain::default_named_chain`]) unless the component sets credentials of its own.
pub fn default_aws_config() -> AwsConfigLoader {
    AwsConfigLoader {
        loader: aws_config::defaults(BehaviorVersion::v2026_01_12()).app_name(APN_APP_NAME.clone()),
        credentials: CredentialsFrom::NamedChain { profile: None },
    }
}

/// An [`aws_config::ConfigLoader`] that loads the credential chain every AWS component uses,
/// unless the component sets credentials of its own. The chain is built from the configuration
/// the loader resolves, so it calls AWS as the component's client does: in the region the
/// component names (or the environment, or the AWS profile), with its profile, its FIPS and
/// dual-stack endpoints, and its HTTP client.
#[must_use = "a loader does nothing until it is loaded"]
pub struct AwsConfigLoader {
    loader: ConfigLoader,
    credentials: CredentialsFrom,
}

/// Where the credentials of an [`AwsConfigLoader`] come from.
enum CredentialsFrom {
    /// The named chain, whose profile link reads `profile`, or the default profile.
    NamedChain { profile: Option<String> },
    /// A provider the component set.
    Own,
}

impl AwsConfigLoader {
    /// The region the component calls, and where the credential chain calls STS.
    pub fn region(mut self, region: impl ProvideRegion + 'static) -> Self {
        self.loader = self.loader.region(region);
        self
    }

    /// The AWS profile the configuration is read from, and the credential chain's profile link.
    pub fn profile_name(mut self, profile: impl Into<String>) -> Self {
        let profile = profile.into();
        if let CredentialsFrom::NamedChain { profile: chain } = &mut self.credentials {
            *chain = Some(profile.clone());
        }
        self.loader = self.loader.profile_name(profile);
        self
    }

    /// Credentials of the component's own, in place of the credential chain.
    pub fn credentials_provider(mut self, provider: impl ProvideCredentials + 'static) -> Self {
        self.loader = self.loader.credentials_provider(provider);
        self.credentials = CredentialsFrom::Own;
        self
    }

    /// Timeouts of the component's requests.
    pub fn timeout_config(mut self, timeout_config: TimeoutConfig) -> Self {
        self.loader = self.loader.timeout_config(timeout_config);
        self
    }

    /// The endpoint of the component's service. The credential chain keeps the endpoints of its
    /// own services, as the AWS SDK's providers do.
    pub fn endpoint_url(mut self, endpoint_url: impl Into<String>) -> Self {
        self.loader = self.loader.endpoint_url(endpoint_url);
        self
    }

    /// Loads the configuration, with the credential chain built from what it resolved.
    pub async fn load(self) -> SdkConfig {
        match self.credentials {
            CredentialsFrom::Own => self.loader.load().await,
            CredentialsFrom::NamedChain { profile } => {
                let config = self.loader.no_credentials().load().await;
                let chain = named_chain::default_named_chain(&config, profile.as_deref());
                config
                    .into_builder()
                    .credentials_provider(SharedCredentialsProvider::new(chain))
                    .build()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use aws_config::Region;
    use aws_credential_types::Credentials;

    use super::*;

    /// The credential chain is built from the component's configuration: STS in its region, and
    /// the profile it names.
    #[tokio::test]
    async fn the_credential_chain_calls_sts_as_the_component_does() {
        let config = default_aws_config()
            .region(Region::new("eu-west-1"))
            .profile_name("spice-ci")
            .endpoint_url("http://localhost:4566")
            .load()
            .await;
        let chain = format!("{:?}", config.credentials_provider());
        assert!(
            chain.contains("GitHubActionsRole { role_arn: ")
                && chain.contains(r#"sts_region: Some(Region("eu-west-1""#)
                && chain.contains("sts_endpoint_url: None")
                && chain.contains(r#"profile_name_override: Some("spice-ci")"#),
            "{chain}"
        );
    }

    /// A component's own credentials replace the chain, wherever the profile is set.
    #[tokio::test]
    async fn the_components_own_credentials_replace_the_chain() {
        let config = default_aws_config()
            .profile_name("spice-ci")
            .credentials_provider(Credentials::new("AKID", "secret", None, None, "static"))
            .region(Region::new("eu-west-1"))
            .load()
            .await;
        let provider = format!("{:?}", config.credentials_provider());
        assert!(
            provider.contains(r#"provider_name: "static""#)
                && !provider.contains("GitHubActionsRole"),
            "{provider}"
        );
    }
}
