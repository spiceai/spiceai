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

//! Renew browser sign-in access tokens before a command uses them.
//!
//! A browser sign-in stores a short-lived access token and a refresh token.
//! Spice Cloud replaces the refresh token on every use and revokes the whole
//! sign-in when an old one is presented again, so two commands must never
//! renew the same sign-in at once.

use snafu::ResultExt as _;

use super::org;
use crate::commands::login::{self, LoginOutput, session};
use crate::error::{ConfigIoSnafu, Result};

/// Renew an access token with less than this left, so a command that waits,
/// such as `spice cloud deploy --wait` (10 minutes by default), does not see
/// it expire part way through.
const RENEW_BEFORE_EXPIRY_SECS: i64 = 15 * 60;

/// A credential store that `spice login` writes and the CLI reads back.
#[derive(Clone, Copy, Debug)]
enum Store {
    Keychain,
    EnvFile,
}

impl Store {
    fn read(self, var: &str) -> Option<String> {
        match self {
            Self::Keychain => login::keychain::read(var),
            Self::EnvFile => login::read_env_var(var),
        }
    }

    /// Replace the values of `SPICE_SPICEAI_*` variables this store holds.
    /// The env file keeps every other line as it is.
    fn write(self, values: &[(&str, &str)]) -> Result<()> {
        match self {
            Self::Keychain => {
                let keys: Vec<(&str, &str)> = values
                    .iter()
                    .map(|(var, value)| (var.strip_prefix("SPICE_SPICEAI_").unwrap_or(var), *value))
                    .collect();
                login::save_credentials(LoginOutput::Keychain, "SPICEAI", &keys)
            }
            Self::EnvFile => super::replace_env_file_values(values),
        }
    }
}

/// Renew every stored browser sign-in whose access token is about to expire.
///
/// Best effort: a sign-in that cannot be renewed stays as it is, and the
/// command reports the rejected token in the usual way.
pub(crate) async fn renew_expiring_sign_ins() {
    renew_expiring_sign_ins_at(&super::client::portal_base_url()).await;
}

async fn renew_expiring_sign_ins_at(portal: &str) {
    let orgs = match org::orgs_with_stored_tokens() {
        Ok(orgs) => orgs,
        Err(err) => {
            tracing::debug!("Could not list stored Spice Cloud sign-ins to renew: {err}");
            return;
        }
    };
    let pairs: Vec<(String, String)> = std::iter::once((
        org::DEFAULT_TOKEN_VAR.to_string(),
        org::DEFAULT_REFRESH_TOKEN_VAR.to_string(),
    ))
    .chain(
        orgs.iter()
            .map(|org| (org::org_token_var(org), org::org_refresh_token_var(org))),
    )
    .collect();

    for store in [Store::Keychain, Store::EnvFile] {
        for (token_var, refresh_var) in &pairs {
            if let Err(err) = renew(portal, store, token_var, refresh_var, &pairs).await {
                tracing::debug!("Could not renew the Spice Cloud sign-in in `{token_var}`: {err}");
            }
        }
    }
}

/// A browser sign-in that `store` holds in `token_var` and that expires soon.
struct Expiring {
    access: String,
    refresh: String,
    client_id: String,
}

/// The sign-in in `token_var` when it expires soon and was granted to the
/// client this CLI registered. A refresh token presented with another client
/// ID revokes the whole sign-in, so a token from any other client is skipped.
fn expiring(store: Store, token_var: &str, refresh_var: &str) -> Option<Expiring> {
    let access = store.read(token_var)?;
    let refresh = store.read(refresh_var)?;
    let claims = session::access_claims(&access)?;
    let registered = org::read_credential(org::CLIENT_ID_VAR)?;
    (claims.seconds_left < RENEW_BEFORE_EXPIRY_SECS && claims.client_id == registered).then_some(
        Expiring {
            access,
            refresh,
            client_id: claims.client_id,
        },
    )
}

async fn renew(
    portal: &str,
    store: Store,
    token_var: &str,
    refresh_var: &str,
    pairs: &[(String, String)],
) -> Result<()> {
    if expiring(store, token_var, refresh_var).is_none() {
        return Ok(());
    }

    let _lock = lock().await?;
    // Read again under the lock: another command may have renewed it already.
    let Some(Expiring {
        access,
        refresh,
        client_id,
    }) = expiring(store, token_var, refresh_var)
    else {
        return Ok(());
    };

    let grant = session::refresh(portal, &client_id, &refresh).await?;

    // One sign-in is stored under the default name and under each org name,
    // and every copy holds the same tokens. Renew them all together.
    let mut values = Vec::new();
    for (token_var, refresh_var) in pairs {
        if store.read(token_var).as_deref() == Some(access.as_str()) {
            values.push((token_var.as_str(), grant.access_token.as_str()));
        }
        if store.read(refresh_var).as_deref() == Some(refresh.as_str()) {
            values.push((refresh_var.as_str(), grant.refresh_token.as_str()));
        }
    }
    store.write(&values)
}

/// Hold the machine-wide renewal lock until the returned file is dropped.
async fn lock() -> Result<std::fs::File> {
    let path = org::renewal_lock_path()?;
    tokio::task::spawn_blocking(move || {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context(ConfigIoSnafu {
                operation: "create",
                path: parent.to_path_buf(),
            })?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .context(ConfigIoSnafu {
                operation: "open",
                path: path.clone(),
            })?;
        file.lock().context(ConfigIoSnafu {
            operation: "lock",
            path,
        })?;
        Ok(file)
    })
    .await
    .map_err(|e| crate::error::Error::InvalidArgument {
        message: format!("The Spice Cloud sign-in renewal lock task failed: {e}"),
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct Stores;

    impl Stores {
        fn new() -> Self {
            login::keychain::test_store::reset();
            login::test_env_file::reset();
            org::test_context_file::reset();
            Self
        }
    }

    impl Drop for Stores {
        fn drop(&mut self) {
            login::keychain::test_store::reset();
            login::test_env_file::reset();
            org::test_context_file::reset();
        }
    }

    fn jwt_expiring_in(seconds: i64) -> String {
        jwt_for("client-1", seconds)
    }

    fn jwt_for(client_id: &str, seconds: i64) -> String {
        let exp = chrono::Utc::now().timestamp() + seconds;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!(r#"{{"exp":{exp},"client_id":"{client_id}"}}"#));
        format!("e30.{payload}.c2ln")
    }

    async fn token_endpoint(expected_calls: u64) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/oauth/token"))
            .and(body_partial_json(serde_json::json!({
                "grant_type": "refresh_token",
                "client_id": "client-1",
                "refresh_token": "refresh-1",
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "access-2",
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": "refresh-2",
            })))
            .expect(expected_calls)
            .mount(&server)
            .await;
        server
    }

    fn store_sign_in(access: &str) {
        login::merge_auth_config(
            "SPICEAI",
            &[
                ("TOKEN", access),
                ("TOKEN_ACME", access),
                ("REFRESH_TOKEN", "refresh-1"),
                ("REFRESH_TOKEN_ACME", "refresh-1"),
                ("CLIENT_ID", "client-1"),
                ("TOKEN_OTHER", "other-access"),
                ("REFRESH_TOKEN_OTHER", "other-refresh"),
            ],
        )
        .expect("store the sign-in");
    }

    #[tokio::test]
    async fn an_expiring_sign_in_is_renewed_in_every_copy() {
        let _stores = Stores::new();
        let server = token_endpoint(1).await;
        store_sign_in(&jwt_expiring_in(60));

        renew_expiring_sign_ins_at(&server.uri()).await;

        for (var, expected) in [
            ("SPICE_SPICEAI_TOKEN", "access-2"),
            ("SPICE_SPICEAI_TOKEN_ACME", "access-2"),
            ("SPICE_SPICEAI_REFRESH_TOKEN", "refresh-2"),
            ("SPICE_SPICEAI_REFRESH_TOKEN_ACME", "refresh-2"),
            ("SPICE_SPICEAI_TOKEN_OTHER", "other-access"),
            ("SPICE_SPICEAI_REFRESH_TOKEN_OTHER", "other-refresh"),
        ] {
            assert_eq!(login::read_env_var(var).as_deref(), Some(expected), "{var}");
        }
    }

    #[tokio::test]
    async fn renewal_keeps_every_other_line_of_the_env_file() {
        let _stores = Stores::new();
        let server = token_endpoint(1).await;
        store_sign_in(&jwt_expiring_in(60));
        let path = login::test_env_file::path();
        let original = std::fs::read_to_string(&path).expect("read env file");
        let edited = format!("# keep me\n\nexport OTHER='a b'\n{original}");
        std::fs::write(&path, &edited).expect("write env file");

        renew_expiring_sign_ins_at(&server.uri()).await;

        let renewed = std::fs::read_to_string(&path).expect("read env file");
        assert!(
            renewed.starts_with("# keep me\n\nexport OTHER='a b'\n"),
            "{renewed}"
        );
        assert_eq!(renewed.lines().count(), edited.lines().count());
        assert_eq!(
            login::read_env_var("SPICE_SPICEAI_TOKEN").as_deref(),
            Some("access-2")
        );
    }

    #[tokio::test]
    async fn a_token_from_another_client_is_never_renewed() {
        let _stores = Stores::new();
        // Presenting the refresh token with another client ID revokes the
        // sign-in, so no token request may be sent at all.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/oauth/token"))
            .respond_with(ResponseTemplate::new(400))
            .expect(0)
            .mount(&server)
            .await;
        // A machine token beside a refresh token left by a browser sign-in.
        store_sign_in(&jwt_for("service-client", 60));

        renew_expiring_sign_ins_at(&server.uri()).await;

        assert_eq!(
            login::read_env_var("SPICE_SPICEAI_REFRESH_TOKEN").as_deref(),
            Some("refresh-1")
        );
    }

    #[tokio::test]
    async fn a_sign_in_that_does_not_expire_soon_is_left_alone() {
        let _stores = Stores::new();
        let server = token_endpoint(0).await;
        let access = jwt_expiring_in(3600);
        store_sign_in(&access);

        renew_expiring_sign_ins_at(&server.uri()).await;

        assert_eq!(login::read_env_var("SPICE_SPICEAI_TOKEN"), Some(access));
    }

    #[tokio::test]
    async fn concurrent_commands_use_the_refresh_token_once() {
        let _stores = Stores::new();
        let server = token_endpoint(1).await;
        store_sign_in(&jwt_expiring_in(60));

        let portal = server.uri();
        tokio::join!(
            renew_expiring_sign_ins_at(&portal),
            renew_expiring_sign_ins_at(&portal),
        );

        assert_eq!(
            login::read_env_var("SPICE_SPICEAI_TOKEN").as_deref(),
            Some("access-2")
        );
    }
}
