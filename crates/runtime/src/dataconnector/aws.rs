
use snafu::prelude::*;
use crate::auth::AuthProvider;
use object_store::aws::AwsCredential;


#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No AWS access secret provided for credentials"))]
    NoAccessSecret ,

    #[snafu(display("No AWS access key provided for credentials"))]
    NoAccessKey ,
}

pub async fn from_auth_provider(auth: AuthProvider) -> Result<AwsCredential, Error> {
    Ok(AwsCredential {
        key_id: auth.get_param("key").context(NoAccessKeySnafu)?.to_string(),
        secret_key: auth.get_param("secret").context(NoAccessSecretSnafu)?.to_string(),
        token: None
    })
}
