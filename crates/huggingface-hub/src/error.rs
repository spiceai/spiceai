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

//! The error a component's download fails with, worded for the person who has to fix it:
//!
//! ```text
//! Failed to download '<file>' for model '<name>' from Hugging Face repo '<repo>' (revision '<rev>'): <cause>. <fix>. See: <docs>
//! ```
//!
//! The cause names the token the Hub rejected by where it came from, because each source has
//! its own fix.

use std::io;
use std::path::PathBuf;

use snafu::prelude::*;

use crate::client;
use crate::download::FetchError;
use crate::location::{RepoId, RepoType};
use crate::settings::{self, Settings, TokenSource, display_path};
use crate::snapshot::Requester;

/// What a download was for: every message names it.
#[derive(Debug, Clone)]
pub(crate) struct DownloadContext {
    requester: Requester,
    /// The repository as the message names it, `<owner>/<name>`.
    repo: String,
    revision: String,
    token: Option<TokenSource>,
    cache_dir: PathBuf,
}

impl DownloadContext {
    pub(crate) fn new(
        requester: &Requester,
        repo: &RepoId,
        revision: &str,
        settings: &Settings,
    ) -> Self {
        Self {
            requester: requester.clone(),
            repo: repo.to_string(),
            revision: revision.to_string(),
            token: settings.token.as_ref().map(|token| token.source().clone()),
            cache_dir: settings.cache_dir.clone(),
        }
    }

    /// A context for a download that failed before its settings resolved.
    fn unresolved(requester: &Requester, repo: &str, revision: Option<&str>) -> Self {
        Self {
            requester: requester.clone(),
            repo: repo.to_string(),
            revision: revision
                .unwrap_or(crate::snapshot::DEFAULT_REVISION)
                .to_string(),
            token: None,
            cache_dir: PathBuf::new(),
        }
    }

    pub(crate) fn error(&self, file: Option<&str>, cause: Cause) -> DownloadError {
        DownloadError {
            context: Box::new(self.clone()),
            file: file.map(ToString::to_string),
            cause,
        }
    }
}

/// Why a download failed.
#[derive(Debug)]
pub(crate) enum Cause {
    Hub(client::Error),
    Fetch(FetchError),
    Settings(settings::Error),
    /// `HF_HUB_OFFLINE` is set and the revision, or `file`, is not in the cache.
    OfflineNotCached {
        file: Option<String>,
    },
    /// The Hub failed (`reason`), and the cached snapshot does not hold the file.
    UnreachableNotCached {
        reason: String,
    },
    /// The file is not in the repository at `commit`.
    NotInRepo {
        commit: String,
    },
    /// The repository does not hold what the component needs: `message` says what is missing.
    Selection {
        message: String,
    },
    /// The repository id is not one the Hub allows.
    InvalidRepo,
    /// The Hub listed a path that would leave the snapshot directory.
    InvalidPath,
}

/// A download for a model, embedding model or reranker failed.
#[derive(Debug, Snafu)]
#[snafu(display("{}", self.message()))]
pub struct DownloadError {
    context: Box<DownloadContext>,
    file: Option<String>,
    cause: Cause,
}

impl DownloadError {
    /// A download of `repo` at `revision` for `requester` that could not start: one of its
    /// settings is not valid.
    #[must_use]
    pub fn settings(
        requester: &Requester,
        repo: &RepoId,
        revision: Option<&str>,
        source: settings::Error,
    ) -> Self {
        DownloadError {
            context: Box::new(DownloadContext::unresolved(
                requester,
                &repo.to_string(),
                revision,
            )),
            file: None,
            cause: Cause::Settings(source),
        }
    }

    /// `repo`, `<owner>/<name>`, as a repository of `kind`.
    ///
    /// # Errors
    ///
    /// Returns an error naming `requester` if `repo` is not a repository id the Hub allows.
    pub fn parse_repo(
        requester: &Requester,
        kind: RepoType,
        repo: &str,
        revision: Option<&str>,
    ) -> Result<RepoId, Self> {
        let invalid = || DownloadError {
            context: Box::new(DownloadContext::unresolved(requester, repo, revision)),
            file: None,
            cause: Cause::InvalidRepo,
        };
        let (owner, name) = repo.split_once('/').ok_or_else(invalid)?;
        RepoId::new(kind, owner, name).map_err(|_| invalid())
    }

    /// The repository lacks a file `requester` needs; `message` names what is missing.
    #[must_use]
    pub fn missing_files(snapshot: &crate::snapshot::Snapshot, message: impl Into<String>) -> Self {
        snapshot.error(
            None,
            Cause::Selection {
                message: message.into(),
            },
        )
    }

    /// Whether the download cannot succeed until the Spicepod changes: a setting that is not
    /// valid, or a repository that does not hold what the component loads. A refusal from the
    /// Hub is not one: a missing or private repository, a rejected token and a gated
    /// repository can all be fixed on the Hub's side, and a network failure, a full disk or a
    /// cold offline cache can clear while Spice runs.
    #[must_use]
    pub fn is_configuration_error(&self) -> bool {
        match &self.cause {
            Cause::Settings(_)
            | Cause::NotInRepo { .. }
            | Cause::Selection { .. }
            | Cause::InvalidPath
            | Cause::InvalidRepo => true,
            Cause::Hub(error) | Cause::Fetch(FetchError::Hub { source: error }) => matches!(
                error,
                client::Error::InvalidEndpoint { .. } | client::Error::InvalidToken
            ),
            Cause::Fetch(_)
            | Cause::OfflineNotCached { .. }
            | Cause::UnreachableNotCached { .. } => false,
        }
    }

    fn message(&self) -> String {
        let DownloadContext {
            requester,
            repo,
            revision,
            ..
        } = self.context.as_ref();
        let what = match &self.file {
            Some(file) => format!("'{file}' for {requester}"),
            None => requester.to_string(),
        };
        let (cause, fix) = self.cause_and_fix();
        let fix = fix.map_or_else(String::new, |fix| format!(" {fix}."));
        format!(
            "Failed to download {what} from Hugging Face repo '{repo}' (revision '{revision}'): {cause}.{fix} See: {}",
            requester.docs_url
        )
    }

    /// The clause after the colon, and the sentence that says what to do.
    fn cause_and_fix(&self) -> (String, Option<String>) {
        let context = self.context.as_ref();
        let token = context.token.as_ref();
        let no_token_fix = "set `hf_token`, set `HF_TOKEN`, or run `hf auth login`";
        match &self.cause {
            Cause::Hub(error)
            | Cause::Fetch(FetchError::Hub {
                source: error,
            }) => hub_cause_and_fix(error, token, no_token_fix, &context.revision),
            Cause::Fetch(FetchError::Write { path, source }) => {
                let cache = display_path(&context.cache_dir);
                let fix = match source.kind() {
                    io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded => Some(format!(
                        "Free space on the disk that holds '{cache}', or set `HF_HUB_CACHE` to a directory on a larger one"
                    )),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::ReadOnlyFilesystem => {
                        Some(format!(
                            "Make '{cache}' writable, or set `HF_HUB_CACHE` to a writable directory"
                        ))
                    }
                    _ => None,
                };
                (
                    format!("writing '{}' failed: {source}", display_path(path)),
                    fix,
                )
            }
            Cause::Fetch(error @ (FetchError::Checksum { .. } | FetchError::Size { .. })) => (
                error.to_string(),
                Some(
                    "Spice downloads it again on the next attempt; if this repeats, a proxy between Spice and the Hub may be changing the bytes"
                        .to_string(),
                ),
            ),
            Cause::Fetch(error @ FetchError::InvalidEntry { .. }) => (error.to_string(), None),
            // Worded as a cause already, with its own fix.
            Cause::Settings(error) => (error.to_string(), None),
            Cause::OfflineNotCached { file } => {
                let cache = display_path(&context.cache_dir);
                let missing = match file {
                    Some(file) => format!("'{file}' is"),
                    None => format!("revision '{}' of the repo is", context.revision),
                };
                (
                    format!(
                        "`HF_HUB_OFFLINE` is set and {missing} not in the Hugging Face cache at '{cache}'"
                    ),
                    Some(format!(
                        "Run `hf download {} --revision {}` while online, or unset `HF_HUB_OFFLINE`",
                        context.repo, context.revision
                    )),
                )
            }
            Cause::UnreachableNotCached { reason } => (
                format!(
                    "the Hugging Face Hub could not be reached ({reason}), and the cached snapshot does not hold this file"
                ),
                Some("Spice retries the download; check the network connection".to_string()),
            ),
            Cause::NotInRepo { commit } => (
                format!(
                    "the repo has no such file at commit {}",
                    commit.get(..7).unwrap_or(commit)
                ),
                None,
            ),
            Cause::Selection { message } => (message.clone(), None),
            Cause::InvalidRepo => (
                "it is not a Hugging Face repo id: use '<owner>/<name>', each 1 to 96 letters, digits, '-', '_' or '.', starting and ending with a letter or digit, without '--' or '..'"
                    .to_string(),
                Some("Correct the repo in `from`".to_string()),
            ),
            Cause::InvalidPath => (
                "the Hub listed this path, which would leave the repository's snapshot directory, so it was refused"
                    .to_string(),
                None,
            ),
        }
    }
}

/// The cause and fix for an error the Hub returned.
fn hub_cause_and_fix(
    error: &client::Error,
    token: Option<&TokenSource>,
    no_token_fix: &str,
    revision: &str,
) -> (String, Option<String>) {
    match (error, token) {
        (client::Error::TokenRejected { status, .. }, Some(source)) => {
            let expired = if matches!(source, TokenSource::File(_)) {
                ", which may have expired"
            } else {
                ""
            };
            let fix = match source {
                TokenSource::Param => {
                    "Set `hf_token` to a token with access to this repo".to_string()
                }
                TokenSource::Env(name) => {
                    format!("Set `{name}` to a token with access to this repo, or set `hf_token`")
                }
                TokenSource::File(_) => "Run `hf auth login`, or set `hf_token`".to_string(),
            };
            (
                format!("the Hub rejected the token from {source} (HTTP {status}){expired}"),
                Some(fix),
            )
        }
        (client::Error::Gated { repo_url, .. }, Some(source)) => (
            "the repo is gated".to_string(),
            Some(format!(
                "Accept its access conditions at {repo_url} with the account the token from {source} belongs to"
            )),
        ),
        (client::Error::Gated { repo_url, .. }, None) => (
            "the repo is gated and no token is set".to_string(),
            Some(format!(
                "Accept its access conditions at {repo_url}, then {no_token_fix}"
            )),
        ),
        (client::Error::RepoNotFound { .. }, Some(source)) => (
            format!(
                "the repo does not exist, or the account the token from {source} belongs to cannot read it"
            ),
            Some("Check the repo name in `from`".to_string()),
        ),
        (client::Error::RepoNotFound { .. } | client::Error::TokenRejected { .. }, None) => (
            "the repo does not exist, or it is private and no token is set".to_string(),
            Some(format!("Check the repo name in `from`, or {no_token_fix}")),
        ),
        (client::Error::RevisionNotFound { .. }, _) => (
            format!("the repo has no revision '{revision}'"),
            Some("Use an existing branch, tag or commit".to_string()),
        ),
        (client::Error::EntryNotFound { path, .. }, _) => {
            (format!("the repo has no '{path}'"), None)
        }
        (client::Error::RateLimited { reset_hint, .. }, None) => (
            format!("the Hub kept rate-limiting requests{reset_hint}"),
            Some(format!(
                "Spice retries the download; to raise the limit, {no_token_fix}"
            )),
        ),
        (client::Error::RateLimited { reset_hint, .. }, Some(_)) => (
            format!("the Hub kept rate-limiting requests{reset_hint}"),
            Some("Spice retries the download".to_string()),
        ),
        (client::Error::Request { .. }, _) => (
            format!("the Hugging Face Hub {}", unreachable_reason_clause(error)),
            Some(RETRY_FIX.to_string()),
        ),
        (client::Error::Status { status, .. }, _) => (
            format!("the Hugging Face Hub {}", unreachable_reason_clause(error)),
            (*status >= 500).then(|| RETRY_FIX.to_string()),
        ),
        (error, _) => (lowercase_first(&error.to_string()), None),
    }
}

/// What a user can do while the Hub cannot be reached.
const RETRY_FIX: &str = "Spice retries the download; check the network connection, or set `HF_HUB_OFFLINE=1` to load from the cache";

/// Why the Hub could not answer, as a short phrase for a warning: `connection timed out`,
/// `HTTP 503`.
#[must_use]
pub(crate) fn unreachable_reason(error: &client::Error) -> String {
    match error {
        client::Error::Request { source, .. } => request_failure(source),
        client::Error::Status { status, .. } => format!("HTTP {status}"),
        client::Error::RateLimited { .. } => "rate limited".to_string(),
        client::Error::TokenRejected { status, .. } => format!("token rejected, HTTP {status}"),
        client::Error::Gated { .. } => "the repo is gated".to_string(),
        error => lowercase_first(&error.to_string()),
    }
}

/// `could not be reached at <endpoint> (connection timed out)`, `returned HTTP 503 …`.
fn unreachable_reason_clause(error: &client::Error) -> String {
    match error {
        client::Error::Request {
            endpoint,
            attempts,
            source,
            ..
        } => format!(
            "could not be reached at {endpoint} after {attempts} attempt(s) ({})",
            request_failure(source)
        ),
        client::Error::Status {
            status,
            attempts,
            message,
            ..
        } => format!("returned HTTP {status} after {attempts} attempt(s){message}"),
        error => lowercase_first(&error.to_string()),
    }
}

fn request_failure(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        "connection timed out".to_string()
    } else if error.is_connect() {
        "connection failed".to_string()
    } else {
        error.to_string()
    }
}

fn lowercase_first(message: &str) -> String {
    let message = message.trim_end_matches('.');
    let mut chars = message.chars();
    match chars.next() {
        // An identifier such as `hf_endpoint` or a quoted value keeps its case.
        Some(first) if first.is_uppercase() && !message.starts_with("Hugging Face") => {
            first.to_lowercase().chain(chars).collect()
        }
        _ => message.to_string(),
    }
}
