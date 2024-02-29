use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::BehaviorVersion;
use aws_config::default_provider::credentials::DefaultCredentialsChain;

use crate::auth::AuthProvider;


fn from_auth_provider(auth: Optional<AuthProvider>) -> Option<SharedCredentialsProvider> {
    match cfg {
        Ok(cfg) => {
            // ProfileFileCredentialsProvider::builder().build();
            // CredentialsProviderChain::first_try(name, provider)
            // .with_custom_credential_source().profile_name(name).region(region).;
            let cred_builder = DefaultCredentialsChain::builder().build(); 
            cred_builder.build()
        }
        None() => None
    }
}
