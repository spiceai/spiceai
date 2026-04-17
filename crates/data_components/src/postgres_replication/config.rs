/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Replication parameters derived from connector params + environment.

use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};

/// Parameters for a single dataset's replication stream.
///
/// Built by the connector from spicepod params; see
/// `connector-postgres::lib::replication_params_from_connector_params`.
#[derive(Clone)]
pub struct ReplicationParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
    pub database: String,
    pub sslmode: SslMode,

    pub slot_name: String,
    pub publication_name: String,
    pub initial_snapshot: bool,
    pub temporary_slot: bool,
    pub status_interval: Duration,
}

impl std::fmt::Debug for ReplicationParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationParams")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("sslmode", &self.sslmode)
            .field("slot_name", &self.slot_name)
            .field("publication_name", &self.publication_name)
            .field("initial_snapshot", &self.initial_snapshot)
            .field("temporary_slot", &self.temporary_slot)
            .field("status_interval", &self.status_interval)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
}

impl SslMode {
    #[must_use]
    pub fn from_str_or_default(s: Option<&str>) -> Self {
        match s.map(str::to_ascii_lowercase).as_deref() {
            Some("disable") => Self::Disable,
            Some("require") | Some("verify-ca") | Some("verify-full") => Self::Require,
            _ => Self::Prefer,
        }
    }
}

/// Build a default slot name: `spice_{sanitized_dataset}_{instance_suffix}`.
///
/// `instance_suffix` is an 8-char blake3-ish hash (actually `twox-hash` xxh3 for
/// zero-dep reuse) of `SPICE_INSTANCE_ID` falling back to the machine hostname,
/// so each replica gets a distinct, deterministic slot across restarts.
#[must_use]
pub fn default_slot_name(dataset_name: &str) -> String {
    let instance = std::env::var("SPICE_INSTANCE_ID")
        .ok()
        .or_else(|| hostname::get().ok().and_then(|h| h.into_string().ok()))
        .unwrap_or_else(|| "unknown".to_string());
    let hash = xxh3_short_hash(&instance);
    format!("spice_{}_{hash}", sanitize(dataset_name))
}

/// Default publication is shared across replicas: `spice_{dataset}_pub`.
#[must_use]
pub fn default_publication_name(dataset_name: &str) -> String {
    format!("spice_{}_pub", sanitize(dataset_name))
}

/// Postgres identifiers must match `[a-z_][a-z0-9_]*` to avoid quoting.
fn sanitize(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() || !out.starts_with(|c: char| c == '_' || c.is_ascii_alphabetic()) {
        out.insert(0, 'd');
    }
    out
}

fn xxh3_short_hash(s: &str) -> String {
    use std::hash::Hasher;
    let mut h = twox_hash::XxHash3_64::with_seed(0);
    h.write(s.as_bytes());
    let v = h.finish();
    // 8 hex chars is enough to disambiguate replicas at human scale.
    format!("{:08x}", v as u32)
}

// `hostname` is a tiny crate; we avoid adding it by using libc/unistd only where needed.
// For cross-platform simplicity, reuse `std::env::var("HOSTNAME")` as a fallback.
mod hostname {
    pub fn get() -> std::io::Result<std::ffi::OsString> {
        if let Ok(h) = std::env::var("HOSTNAME") {
            return Ok(h.into());
        }
        if let Ok(h) = std::env::var("COMPUTERNAME") {
            return Ok(h.into());
        }
        // Last resort: uname(2)-like fallback for unix via /etc/hostname.
        if let Ok(contents) = std::fs::read_to_string("/etc/hostname") {
            return Ok(contents.trim().to_string().into());
        }
        Err(std::io::Error::other("hostname unavailable"))
    }
}

impl ReplicationParams {
    /// Build a tokio-postgres config for setup queries (not replication).
    #[must_use]
    pub fn setup_pg_config(&self) -> tokio_postgres::Config {
        let mut cfg = tokio_postgres::Config::new();
        cfg.host(&self.host)
            .port(self.port)
            .user(&self.user)
            .password(self.password.expose_secret())
            .dbname(&self.database)
            .application_name(&format!("spice-replication-setup/{}", self.slot_name));
        match self.sslmode {
            SslMode::Disable => cfg.ssl_mode(tokio_postgres::config::SslMode::Disable),
            SslMode::Prefer => cfg.ssl_mode(tokio_postgres::config::SslMode::Prefer),
            SslMode::Require => cfg.ssl_mode(tokio_postgres::config::SslMode::Require),
        };
        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_replaces_non_alnum() {
        assert_eq!(sanitize("public.users"), "public_users");
        assert_eq!(sanitize("my-dataset"), "my_dataset");
        assert_eq!(sanitize(""), "d");
        assert_eq!(sanitize("9leading"), "d9leading");
    }

    #[test]
    fn default_names_are_deterministic_for_fixed_env() {
        // SAFETY: single-threaded test; set-get-unset within one test.
        // We guard by restoring previous value.
        let prev = std::env::var("SPICE_INSTANCE_ID").ok();
        // SAFETY: no concurrent reads of this env var during this test.
        unsafe {
            std::env::set_var("SPICE_INSTANCE_ID", "replica-a");
        }
        let a1 = default_slot_name("users");
        let a2 = default_slot_name("users");
        assert_eq!(a1, a2);
        assert!(a1.starts_with("spice_users_"));
        // SAFETY: reverting to the value from before this test.
        unsafe {
            match prev {
                Some(v) => std::env::set_var("SPICE_INSTANCE_ID", v),
                None => std::env::remove_var("SPICE_INSTANCE_ID"),
            }
        }
    }

    #[test]
    fn publication_default() {
        assert_eq!(default_publication_name("users"), "spice_users_pub");
        assert_eq!(
            default_publication_name("public.orders"),
            "spice_public_orders_pub"
        );
    }
}
