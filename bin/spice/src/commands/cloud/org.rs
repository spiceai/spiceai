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

//! Organization context for `spice cloud`.
//!
//! Spice Cloud user access tokens follow the user's memberships, while machine
//! credentials remain bound to one organization. The CLI keeps an explicit
//! organization selection and optional per-org credential copies so both token
//! kinds can be selected without guessing. This module owns both halves:
//!
//! - the **active org**, a machine-wide setting in `~/.spice/cloud-context.json`
//!   so a new shell keeps operating on the org the last `org use` selected;
//! - the **per-org credential**, stored under `SPICE_SPICEAI_TOKEN_<ORG>`
//!   alongside the default `SPICE_SPICEAI_TOKEN`, so authenticating against a
//!   second org never overwrites the first.
//! - the **keychain org index**, a non-secret list in the same context file so
//!   `logout --scope all` can enumerate credentials that have no env-file key.
//!
//! Selecting an org is a statement of intent only. Every request still carries
//! the org to the server, which is the sole authority on membership.

use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{CloudErrorCode, ConfigIoSnafu, CreateDirectorySnafu, Error, Result};
use snafu::ResultExt;

/// Environment variable holding the default (personal-org) Spice Cloud token.
pub const DEFAULT_TOKEN_VAR: &str = "SPICE_SPICEAI_TOKEN";

/// Environment variable holding the default app (data-plane) API key.
pub const DEFAULT_API_KEY_VAR: &str = "SPICE_SPICEAI_API_KEY";

/// Environment variable that overrides the persisted active org for one process.
pub const ACTIVE_ORG_VAR: &str = "SPICE_CLOUD_ORG";

const DOT_SPICE: &str = ".spice";
const CONTEXT_FILE: &str = "cloud-context.json";
const MAX_ORG_NAME_LEN: usize = 64;

/// Machine-wide `spice cloud` state that is not a secret.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CloudContext {
    /// Organization subsequent commands act on. `None` means the credential's
    /// default organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_org: Option<String>,

    /// When the active org was last changed, for `whoami` and support triage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,

    /// Organizations whose credentials were written to the platform keychain.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub keychain_orgs: BTreeSet<String>,
}

/// Reject org names that are not safe to put in a URL, a header, or an
/// environment variable name, before they reach the API or the credential store.
///
/// # Errors
///
/// Returns an error when `org` is empty, over-long, or contains anything other
/// than ASCII alphanumerics, `-`, `_`, or `.`.
pub fn validate_org_name(org: &str) -> Result<()> {
    if org.is_empty() {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::InvalidRequest,
            "Organization name cannot be empty.",
            "Run 'spice cloud orgs' to list the organizations you can access.",
        ));
    }

    if org.len() > MAX_ORG_NAME_LEN {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::InvalidRequest,
            format!(
                "Organization name is too long ({} characters, maximum {MAX_ORG_NAME_LEN}).",
                org.len()
            ),
            "Run 'spice cloud orgs' to list the organizations you can access.",
        ));
    }

    if !org
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(Error::cloud_with_hint(
            CloudErrorCode::InvalidRequest,
            format!(
                "Invalid organization name '{org}': expected letters, digits, '-', '_', or '.'."
            ),
            "Run 'spice cloud orgs' to list the organizations you can access.",
        ));
    }

    Ok(())
}

/// Path to the machine-wide cloud context file (`~/.spice/cloud-context.json`).
///
/// # Errors
///
/// Returns an error if the home directory cannot be determined.
fn context_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| crate::error::HomeDirectoryNotFoundSnafu.build())?;
    Ok(home.join(DOT_SPICE).join(CONTEXT_FILE))
}

/// Load the persisted cloud context, or the default when none has been written.
///
/// A context file that cannot be parsed is reported rather than silently reset,
/// so a corrupted file never causes a command to act on an unexpected org.
///
/// # Errors
///
/// Returns an error if the file exists but cannot be read or parsed.
pub fn load_context() -> Result<CloudContext> {
    let path = context_path()?;
    if !path.exists() {
        return Ok(CloudContext::default());
    }

    let content = std::fs::read_to_string(&path).context(ConfigIoSnafu {
        operation: "read",
        path: path.clone(),
    })?;

    serde_json::from_str(&content).map_err(|e| Error::ConfigParse {
        message: format!(
            "Failed to parse cloud context at {}: {e}. Delete the file to reset the active org.",
            path.display()
        ),
    })
}

/// Persist the cloud context to `~/.spice/cloud-context.json`.
///
/// # Errors
///
/// Returns an error if the file cannot be created or written.
pub fn save_context(context: &CloudContext) -> Result<()> {
    let path = context_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context(CreateDirectorySnafu { path: parent })?;
    }

    let serialized = serde_json::to_string_pretty(context).map_err(|e| Error::ConfigParse {
        message: format!("Failed to serialize cloud context: {e}"),
    })?;

    std::fs::write(&path, serialized).context(ConfigIoSnafu {
        operation: "write",
        path,
    })
}

/// Set the active org for subsequent commands.
///
/// # Errors
///
/// Returns an error if `org` is not a valid name or the context cannot be saved.
pub fn set_active_org(org: &str) -> Result<()> {
    validate_org_name(org)?;
    let mut context = load_context()?;
    context.active_org = Some(org.to_string());
    context.updated_at = Some(chrono::Utc::now().to_rfc3339());
    save_context(&context)
}

/// Clear the active org, returning commands to the credential's own org.
///
/// # Errors
///
/// Returns an error if the context cannot be saved.
pub fn clear_active_org() -> Result<()> {
    let mut context = load_context()?;
    context.active_org = None;
    context.updated_at = Some(chrono::Utc::now().to_rfc3339());
    save_context(&context)
}

/// Record organizations whose credentials are being written to the keychain.
///
/// The index is written before the credentials. A failed credential write can
/// therefore leave a harmless stale name, but never an unenumerable secret.
///
/// # Errors
///
/// Returns an error if an organization name is invalid or the context cannot
/// be loaded or saved.
pub(super) fn remember_keychain_orgs(orgs: &BTreeSet<String>) -> Result<()> {
    if orgs.is_empty() {
        return Ok(());
    }

    let mut context = load_context()?;
    for org in orgs {
        validate_org_name(org)?;
        context.keychain_orgs.insert(org.clone());
    }
    save_context(&context)
}

/// Forget an organization after its keychain credential has been removed.
///
/// # Errors
///
/// Returns an error if the context cannot be loaded or saved.
pub(super) fn forget_keychain_org(org: &str) -> Result<()> {
    let mut context = load_context()?;
    if context.keychain_orgs.remove(org) {
        save_context(&context)?;
    }
    Ok(())
}

/// Clear the keychain organization index after all indexed credentials have
/// been removed.
///
/// # Errors
///
/// Returns an error if the context cannot be loaded or saved.
pub(super) fn clear_keychain_orgs() -> Result<()> {
    let mut context = load_context()?;
    if !context.keychain_orgs.is_empty() {
        context.keychain_orgs.clear();
        save_context(&context)?;
    }
    Ok(())
}

/// The active org for this process: `SPICE_CLOUD_ORG` if set, else the
/// persisted selection.
///
/// # Errors
///
/// Returns an error if the persisted context cannot be read, or if either
/// source holds an invalid org name.
pub fn active_org() -> Result<Option<String>> {
    if let Ok(org) = std::env::var(ACTIVE_ORG_VAR)
        && !org.is_empty()
    {
        validate_org_name(&org)?;
        return Ok(Some(org));
    }

    let Some(org) = load_context()?.active_org else {
        return Ok(None);
    };
    validate_org_name(&org)?;
    Ok(Some(org))
}

/// Encode an org name into the `[A-Z0-9_]` alphabet an environment variable
/// name allows, reversibly.
///
/// ASCII alphanumerics uppercase directly — org names compare
/// case-insensitively everywhere else in the CLI, so folding case here is
/// deliberate. Every other legal character becomes `_XX` with its hex code, so
/// names that differ only by punctuation get **different** variables:
/// `spice-hq` → `SPICE_2DHQ`, `spice.hq` → `SPICE_2EHQ`, `spice_hq` →
/// `SPICE_5FHQ`. A lossy mapping here would let one org's credential overwrite
/// another's and then be sent to the wrong organization.
fn encode_org(org: &str) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(org.len());
    for c in org.chars() {
        if c.is_ascii_alphanumeric() {
            encoded.push(c.to_ascii_uppercase());
        } else {
            // `validate_org_name` restricts the alphabet to ASCII, so one byte
            // is always enough.
            let _ = write!(encoded, "_{:02X}", c as u32);
        }
    }
    encoded
}

/// Reverse [`encode_org`], returning `None` for anything this CLI did not write.
fn decode_org(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut org = String::with_capacity(encoded.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'_' {
            let hex = encoded.get(i + 1..i + 3)?;
            let byte = u8::from_str_radix(hex, 16).ok()?;
            org.push(char::from(byte));
            i += 3;
        } else {
            org.push(char::from(bytes[i]).to_ascii_lowercase());
            i += 1;
        }
    }

    (!org.is_empty()).then_some(org)
}

/// Recover an organization name from a `save_credentials("SPICEAI", ...)`
/// token key. Default and API-key entries do not identify an organization.
pub(super) fn org_from_credential_key(key: &str) -> Option<String> {
    decode_org(key.strip_prefix("TOKEN_")?)
}

/// Environment variable holding the management credential for `org`.
///
/// `spicehq` → `SPICE_SPICEAI_TOKEN_SPICEHQ`.
#[must_use]
pub fn org_token_var(org: &str) -> String {
    format!("{DEFAULT_TOKEN_VAR}_{}", encode_org(org))
}

/// Environment variable holding the app (data-plane) API key for `org`.
///
/// Kept separate from the default `SPICE_SPICEAI_API_KEY` so authenticating to
/// a second org cannot silently replace the first org's data-plane key.
#[must_use]
pub fn org_api_key_var(org: &str) -> String {
    format!("{DEFAULT_API_KEY_VAR}_{}", encode_org(org))
}

/// Read a credential by environment variable name, in the CLI's standard
/// priority order: process environment, platform keychain, then env file.
#[must_use]
pub fn read_credential(var: &str) -> Option<String> {
    if let Ok(value) = std::env::var(var)
        && !value.is_empty()
    {
        return Some(value);
    }

    if let Ok(entry) = keyring::Entry::new(var, "spice")
        && let Ok(value) = entry.get_password()
        && !value.is_empty()
    {
        return Some(value);
    }

    read_credential_from_env_file(var)
}

/// Read a credential from the env file `spice cloud login` writes to.
///
/// Delegates to the writer's own parser so quoting and `KEY = value` spacing
/// are handled identically on both sides — a hand-rolled reader here could
/// find a credential the remover cannot match, leaving it live after logout.
fn read_credential_from_env_file(var: &str) -> Option<String> {
    crate::commands::login::read_env_var(var)
}

/// The credential to use when no organization was named.
///
/// A single-org user has exactly one token and it is the right one.
#[must_use]
pub fn default_token() -> Option<String> {
    read_credential(DEFAULT_TOKEN_VAR)
}

/// The credential bound to `org`.
///
/// Deliberately does **not** decide whether the default credential may fall
/// back. Callers first prefer this explicit copy, then may probe the default
/// credential: a user token is valid for every member organization, while a
/// machine token has no user membership and must stay organization-bound.
#[must_use]
pub fn token_for_org(org: &str) -> Option<String> {
    read_credential(&org_token_var(org))
}

/// The app API key bound to `org`, falling back to the default key only when no
/// org was named.
///
/// Unlike the management token this may fall back: the data-plane key is
/// per-app, and a single-org user's default key is the only one they have.
#[must_use]
pub fn api_key_for_org(org: Option<&str>) -> Option<String> {
    if let Some(org) = org {
        return read_credential(&org_api_key_var(org));
    }
    read_credential(DEFAULT_API_KEY_VAR)
}

/// Whether a credential is stored specifically for `org`, as opposed to `org`
/// falling back to the default credential.
#[must_use]
pub fn has_org_token(org: &str) -> bool {
    read_credential(&org_token_var(org)).is_some()
}

/// Orgs with a credential of their own, discovered from the env file and the
/// process environment, plus the non-secret index of keychain-only credentials.
/// Used to list known orgs when the API cannot enumerate them, and to log out of
/// every stored session.
///
/// # Errors
///
/// Returns an error if the persisted context cannot be read or contains an
/// invalid indexed organization.
pub fn orgs_with_stored_tokens() -> Result<BTreeSet<String>> {
    stored_orgs_from_sources(
        load_context()?,
        std::env::vars().chain(crate::commands::login::env_file_vars()),
    )
}

fn stored_orgs_from_sources(
    context: CloudContext,
    vars: impl IntoIterator<Item = (String, String)>,
) -> Result<BTreeSet<String>> {
    for org in &context.keychain_orgs {
        validate_org_name(org)?;
    }
    let mut names = context.keychain_orgs;
    let prefix = format!("{DEFAULT_TOKEN_VAR}_");

    let mut collect = |key: &str| {
        if let Some(suffix) = key.strip_prefix(prefix.as_str())
            && !suffix.is_empty()
            // Decode rather than lowercase: the encoded form is not the org
            // name, and reporting `spice_hq` for `spice-hq` would send the user
            // to a `--org` value that resolves to a different credential.
            && let Some(org) = decode_org(suffix)
        {
            names.insert(org);
        }
    };

    for (key, value) in vars {
        if !value.is_empty() {
            collect(&key);
        }
    }

    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn org_names_accept_the_shapes_spice_cloud_issues() {
        for org in [
            "lukekim", "spicehq", "spice-hq", "spice_hq", "acme.co", "a1",
        ] {
            validate_org_name(org).unwrap_or_else(|e| panic!("'{org}' should be valid: {e}"));
        }
    }

    #[test]
    fn org_names_reject_values_unsafe_for_urls_and_headers() {
        for org in [
            "",
            "spice hq",
            "spice/hq",
            "spice\nhq",
            "spice:hq",
            "../etc",
        ] {
            let err = validate_org_name(org)
                .expect_err(&format!("'{}' should be rejected", org.escape_debug()));
            assert_eq!(err.cloud_code(), Some(CloudErrorCode::InvalidRequest));
        }
    }

    #[test]
    fn over_long_org_names_are_rejected() {
        let org = "a".repeat(MAX_ORG_NAME_LEN + 1);
        let err = validate_org_name(&org).expect_err("over-long org should be rejected");
        assert!(
            err.to_string().contains("too long"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn org_token_var_is_derived_from_the_org_name() {
        assert_eq!(org_token_var("spicehq"), "SPICE_SPICEAI_TOKEN_SPICEHQ");
        assert_eq!(org_token_var("SpiceHQ"), "SPICE_SPICEAI_TOKEN_SPICEHQ");
    }

    #[test]
    fn org_names_differing_only_by_punctuation_get_different_credentials() {
        // Regression guard: a lossy mapping made `spice-hq`, `spice_hq`, and
        // `spice.hq` share one variable, so logging into one overwrote the
        // others and its token was then sent to the wrong organization.
        let vars: Vec<String> = ["spice-hq", "spice_hq", "spice.hq"]
            .iter()
            .map(|org| org_token_var(org))
            .collect();

        let unique: BTreeSet<&String> = vars.iter().collect();
        assert_eq!(
            unique.len(),
            vars.len(),
            "credential keys collided: {vars:?}"
        );
    }

    #[test]
    fn every_legal_org_character_round_trips() {
        // Any name `validate_org_name` accepts must survive encode → decode, or
        // `spice cloud orgs` and `logout --scope all` report the wrong org.
        for org in [
            "spicehq", "spice-hq", "spice_hq", "spice.hq", "a1", "a-b_c.d", "0-9",
        ] {
            validate_org_name(org).unwrap_or_else(|e| panic!("'{org}' should be valid: {e}"));
            let encoded = encode_org(org);
            assert_eq!(
                decode_org(&encoded).as_deref(),
                Some(org),
                "'{org}' did not round-trip (encoded as '{encoded}')"
            );
        }
    }

    #[test]
    fn decode_org_rejects_malformed_suffixes() {
        assert!(decode_org("").is_none());
        assert!(decode_org("SPICE_").is_none(), "truncated escape");
        assert!(decode_org("SPICE_ZZ").is_none(), "non-hex escape");
    }

    #[test]
    fn credential_keys_recover_only_per_org_tokens() {
        assert_eq!(
            org_from_credential_key("TOKEN_SPICE_2DHQ").as_deref(),
            Some("spice-hq")
        );
        assert!(org_from_credential_key("TOKEN").is_none());
        assert!(org_from_credential_key("API_KEY_SPICEHQ").is_none());
    }

    #[test]
    fn keychain_only_orgs_are_enumerated_for_logout() {
        let context = CloudContext {
            keychain_orgs: BTreeSet::from(["keychain-org".to_string()]),
            ..CloudContext::default()
        };
        let stored = stored_orgs_from_sources(
            context,
            [
                (
                    "SPICE_SPICEAI_TOKEN_ENV_2DORG".to_string(),
                    "token".to_string(),
                ),
                ("SPICE_SPICEAI_TOKEN_EMPTY".to_string(), String::new()),
            ],
        )
        .expect("stored organization sources should be valid");

        assert_eq!(
            stored,
            BTreeSet::from(["env-org".to_string(), "keychain-org".to_string()])
        );
    }

    #[test]
    fn old_context_files_default_to_no_keychain_orgs() {
        let context: CloudContext =
            serde_json::from_str(r#"{"active_org":"acme","updated_at":"2026-08-20T00:00:00Z"}"#)
                .expect("the prior context schema should remain readable");

        assert!(context.keychain_orgs.is_empty());
    }

    #[test]
    fn per_org_variables_never_collide_with_the_defaults() {
        // A per-org variable must not shadow the personal-org credential, or
        // authenticating to a second org would silently replace it.
        assert_ne!(org_token_var("spicehq"), DEFAULT_TOKEN_VAR);
        assert_ne!(org_api_key_var("spicehq"), DEFAULT_API_KEY_VAR);
        assert_ne!(org_token_var("spicehq"), org_api_key_var("spicehq"));
    }
}
