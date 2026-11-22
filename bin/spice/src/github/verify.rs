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

//! Checks a downloaded release asset against the sha256 its release records, before anything
//! is installed from it.
//!
//! This applies the rules of `verifyDownload` in `install/install.sh` (and its copies in
//! `install/install-spiced.sh` and `install/Install.ps1`), so a release the installers refuse
//! is refused by `spice install` and `spice upgrade` too:
//!
//! - The expected sha256 comes from the release's `SHA256SUMS`. An asset `SHA256SUMS` does not
//!   list is refused, and so is every asset of a release whose `SHA256SUMS` cannot be
//!   downloaded.
//! - Only a release whose asset list has no `SHA256SUMS` (one published before it) falls back
//!   to the sha256 GitHub recorded when the asset was uploaded.
//! - When neither exists the asset is refused, unless `SPICE_INSTALL_ALLOW_UNVERIFIED=1` is
//!   set. That setting never installs an asset whose check fails.
//! - A release with `SHA256SUMS` also has build attestations, which are checked with
//!   `gh attestation verify` when the GitHub CLI is installed and logged in.

use super::{GitHubClient, GitHubError, ReleaseAsset, RepoRelease};
use sha2::{Digest as _, Sha256};
use std::ffi::OsString;
use std::time::Duration;

/// The release asset listing the sha256 of every other asset, as `sha256sum` writes it.
pub(crate) const SHA256SUMS: &str = "SHA256SUMS";

/// Setting this to `1` installs an asset whose release records no sha256 for it.
pub(crate) const ALLOW_UNVERIFIED_ENV: &str = "SPICE_INSTALL_ALLOW_UNVERIFIED";

/// Release attestations are signed by `build_release.yml` running on a release branch.
const ATTESTATION_SIGNER: &str = r"^https://github\.com/spiceai/spiceai/\.github/workflows/build_release\.yml@refs/heads/release/";

/// Further attempts at downloading `SHA256SUMS` after a transient failure.
const SUMS_RETRIES: u32 = 3;
const SUMS_RETRY_DELAY: Duration = Duration::from_secs(1);

/// How a download is verified: the settings that come from the environment.
#[derive(Debug, Clone)]
pub(crate) struct VerifyOptions {
    /// `SPICE_INSTALL_ALLOW_UNVERIFIED=1`.
    pub allow_unverified: bool,
    /// The GitHub CLI program that checks build attestations.
    pub gh: OsString,
}

impl VerifyOptions {
    /// The options this process runs with.
    pub(crate) fn from_env() -> Self {
        Self {
            allow_unverified: std::env::var(ALLOW_UNVERIFIED_ENV).is_ok_and(|v| v == "1"),
            gh: OsString::from("gh"),
        }
    }
}

/// The sha256 a release records for one of its assets.
#[derive(Debug, PartialEq, Eq)]
enum Expected {
    Recorded {
        /// Lowercase hex.
        sha256: String,
        /// Where the sha256 came from, for messages.
        source: String,
        /// The release publishes `SHA256SUMS`, and with it build attestations.
        attested: bool,
    },
    /// The release records no sha256 for the asset.
    Unrecorded { reason: String },
}

/// Checks `data`, the downloaded `asset` of `release`, against the sha256 the release records
/// for it, and its build attestation when the release has one and `gh` can check it. Returns
/// `data` once it may be installed.
///
/// # Errors
///
/// Returns an error when the release records no sha256 for the asset (unless
/// [`VerifyOptions::allow_unverified`]), when the sha256 does not match, or when the build
/// attestation does not verify.
pub(crate) async fn verify_release_asset(
    client: &GitHubClient,
    release: &RepoRelease,
    asset: &ReleaseAsset,
    data: Vec<u8>,
    options: &VerifyOptions,
) -> Result<Vec<u8>, GitHubError> {
    let (expected, source, attested) = match expected_sha256(client, release, asset).await? {
        Expected::Recorded {
            sha256,
            source,
            attested,
        } => (sha256, source, attested),
        Expected::Unrecorded { reason } => {
            if options.allow_unverified {
                tracing::warn!(
                    "Installing {} unverified, because {ALLOW_UNVERIFIED_ENV}=1 is set: {reason}.",
                    asset.name
                );
                return Ok(data);
            }
            return Err(GitHubError::Unverifiable {
                asset: asset.name.clone(),
                reason,
            });
        }
    };

    // Hashing a few hundred megabytes is CPU work, so it runs off the async threads.
    let (data, actual) = tokio::task::spawn_blocking(move || {
        let actual = format!("{:x}", Sha256::digest(&data));
        (data, actual)
    })
    .await
    .map_err(|e| GitHubError::Io {
        message: format!("Failed to compute the sha256 of {}: {e}", asset.name),
    })?;

    if actual != expected {
        return Err(GitHubError::ChecksumMismatch {
            asset: asset.name.clone(),
            expected,
            actual,
            source,
        });
    }
    tracing::info!(
        "Verified the sha256 of {} against the {source}.",
        asset.name
    );

    if attested {
        verify_attestation(client, asset, data, options).await
    } else {
        Ok(data)
    }
}

/// The sha256 `release` records for `asset`: its `SHA256SUMS` entry, or for a release with no
/// `SHA256SUMS`, the digest GitHub recorded at upload.
async fn expected_sha256(
    client: &GitHubClient,
    release: &RepoRelease,
    asset: &ReleaseAsset,
) -> Result<Expected, GitHubError> {
    let tag = &release.tag_name;
    if let Some(sums) = release.get_asset(SHA256SUMS) {
        // The release lists SHA256SUMS, so the GitHub digest is never a fallback here: it
        // would skip the attestation check.
        let body =
            download_sums(client, sums)
                .await
                .map_err(|e| GitHubError::ChecksumsUnavailable {
                    asset: asset.name.clone(),
                    tag: tag.clone(),
                    cause: e.to_string(),
                })?;
        let sha256 =
            sha256_listed_for(&String::from_utf8_lossy(&body), &asset.name).ok_or_else(|| {
                GitHubError::ChecksumNotListed {
                    asset: asset.name.clone(),
                    tag: tag.clone(),
                }
            })?;
        return Ok(Expected::Recorded {
            sha256,
            source: format!("SHA256SUMS of release {tag}"),
            attested: true,
        });
    }

    Ok(match asset.digest.as_deref().and_then(sha256_of_digest) {
        Some(sha256) => Expected::Recorded {
            sha256,
            source: format!("sha256 GitHub recorded for {} in release {tag}", asset.name),
            attested: false,
        },
        None => Expected::Unrecorded {
            reason: format!(
                "release {tag} has no SHA256SUMS, and GitHub recorded no sha256 for {}",
                asset.name
            ),
        },
    })
}

/// Downloads `SHA256SUMS`, trying again after a failure a retry can fix: a dropped or timed
/// out connection, a truncated body, or a 408, 429 or 5xx.
async fn download_sums(client: &GitHubClient, sums: &ReleaseAsset) -> Result<Vec<u8>, GitHubError> {
    let mut retries = 0;
    loop {
        match client.download_asset(sums, |_| {}).await {
            Ok(body) => return Ok(body),
            Err(e) if retries < SUMS_RETRIES && is_transient(&e) => {
                retries += 1;
                tracing::debug!("Downloading {SHA256SUMS} failed, trying again: {e}");
                tokio::time::sleep(SUMS_RETRY_DELAY).await;
            }
            Err(e) => return Err(e),
        }
    }
}

fn is_transient(error: &GitHubError) -> bool {
    match error {
        GitHubError::Request { .. } | GitHubError::IncompleteDownload { .. } => true,
        GitHubError::Api { status, .. } => matches!(status, 408 | 429 | 500 | 502 | 503 | 504),
        _ => false,
    }
}

/// The sha256 `sums`, in `sha256sum` output format, lists for `asset`, in lowercase. A
/// binary-mode entry names the file with a leading `*`.
fn sha256_listed_for(sums: &str, asset: &str) -> Option<String> {
    sums.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let sha256 = fields.next()?;
        let name = fields.next()?;
        (name == asset || name.strip_prefix('*') == Some(asset)).then(|| sha256.to_lowercase())
    })
}

/// The hex of a GitHub asset digest, `sha256:<64 hex digits>`, in lowercase.
fn sha256_of_digest(digest: &str) -> Option<String> {
    let hex = digest.strip_prefix("sha256:")?;
    (hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit())).then(|| hex.to_lowercase())
}

/// Runs `gh attestation verify` on `data` when `gh` is installed and logged in.
async fn verify_attestation(
    client: &GitHubClient,
    asset: &ReleaseAsset,
    data: Vec<u8>,
    options: &VerifyOptions,
) -> Result<Vec<u8>, GitHubError> {
    let logged_in = tokio::process::Command::new(&options.gh)
        .args(["auth", "status"])
        .output()
        .await
        .is_ok_and(|output| output.status.success());
    if !logged_in {
        tracing::info!(
            "Skipped the build attestation check of {}: the GitHub CLI (gh) is not installed or not logged in.",
            asset.name
        );
        return Ok(data);
    }

    // `gh attestation verify` reads a file, so the download is written to one first.
    let io_error = |action: &str, e: &dyn std::fmt::Display| GitHubError::Io {
        message: format!(
            "Failed to {action} to check the build attestation of {}: {e}",
            asset.name
        ),
    };
    let dir = tempfile::tempdir().map_err(|e| io_error("create a temporary directory", &e))?;
    let file = dir.path().join(&asset.name);
    let data = {
        let file = file.clone();
        tokio::task::spawn_blocking(move || std::fs::write(&file, &data).map(|()| data))
            .await
            .map_err(|e| io_error("write the download", &e))?
            .map_err(|e| io_error("write the download", &e))?
    };

    let output = tokio::process::Command::new(&options.gh)
        .arg("attestation")
        .arg("verify")
        .arg(&file)
        .args(["--repo", &format!("{}/{}", client.owner, client.repo)])
        .args(["--cert-identity-regex", ATTESTATION_SIGNER])
        .arg("--deny-self-hosted-runners")
        .output()
        .await
        .map_err(|e| io_error("run `gh attestation verify`", &e))?;
    if !output.status.success() {
        // One line, so the error stays a single log line.
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let log = stdout
            .lines()
            .chain(stderr.lines())
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        return Err(GitHubError::AttestationFailed {
            asset: asset.name.clone(),
            log,
        });
    }
    tracing::info!("Verified the build attestation of {}.", asset.name);
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path as path_matcher};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ASSET: &str = "spiced_linux_x86_64.tar.gz";
    const TAG: &str = "v9.9.9";
    const BODY: &[u8] = b"the runtime archive";

    fn sha256_hex(data: &[u8]) -> String {
        format!("{:x}", Sha256::digest(data))
    }

    fn asset(server: &MockServer, name: &str, size: u64, digest: Option<String>) -> ReleaseAsset {
        ReleaseAsset {
            url: String::new(),
            id: 1,
            name: name.to_string(),
            content_type: "application/octet-stream".to_string(),
            size,
            download_count: 0,
            browser_download_url: format!("{}/{name}", server.uri()),
            digest,
        }
    }

    fn release(assets: Vec<ReleaseAsset>) -> RepoRelease {
        RepoRelease {
            url: String::new(),
            html_url: String::new(),
            assets_url: String::new(),
            tag_name: TAG.to_string(),
            name: None,
            draft: false,
            prerelease: false,
            created_at: String::new(),
            published_at: None,
            assets,
        }
    }

    /// A release whose asset `ASSET` carries `digest`, and which publishes `sums` (served by
    /// the mock) when given.
    async fn release_with(
        server: &MockServer,
        digest: Option<String>,
        sums: Option<&str>,
    ) -> (RepoRelease, ReleaseAsset) {
        let mut assets = vec![];
        if let Some(sums) = sums {
            Mock::given(method("GET"))
                .and(path_matcher(format!("/{SHA256SUMS}")))
                .respond_with(ResponseTemplate::new(200).set_body_string(sums))
                .mount(server)
                .await;
            assets.push(asset(server, SHA256SUMS, sums.len() as u64, None));
        }
        assets.push(asset(server, ASSET, BODY.len() as u64, digest.clone()));
        (
            release(assets),
            asset(server, ASSET, BODY.len() as u64, digest),
        )
    }

    fn options(allow_unverified: bool) -> VerifyOptions {
        VerifyOptions {
            allow_unverified,
            // No such program: the attestation check is skipped.
            gh: OsString::from("/nonexistent/gh"),
        }
    }

    async fn verify(
        release: &RepoRelease,
        asset: &ReleaseAsset,
        options: &VerifyOptions,
    ) -> Result<Vec<u8>, GitHubError> {
        let client = GitHubClient::new("spiceai", "spiceai");
        verify_release_asset(&client, release, asset, BODY.to_vec(), options).await
    }

    #[tokio::test]
    async fn an_asset_matching_its_sha256sums_entry_is_accepted() {
        let server = MockServer::start().await;
        let sums = format!(
            "{}  spice_linux_x86_64.tar.gz\n{}  {ASSET}\n",
            sha256_hex(b"other"),
            sha256_hex(BODY)
        );
        // A wrong GitHub digest proves SHA256SUMS, not the digest, decided.
        let (release, asset) = release_with(
            &server,
            Some(format!("sha256:{}", "0".repeat(64))),
            Some(&sums),
        )
        .await;

        let data = verify(&release, &asset, &options(false))
            .await
            .expect("an asset matching SHA256SUMS is accepted");
        assert_eq!(data, BODY);
    }

    #[tokio::test]
    async fn binary_mode_and_uppercase_sha256sums_entries_are_read() {
        let server = MockServer::start().await;
        let sums = format!("{} *{ASSET}\n", sha256_hex(BODY).to_uppercase());
        let (release, asset) = release_with(&server, None, Some(&sums)).await;

        let data = verify(&release, &asset, &options(false))
            .await
            .expect("a `*name` entry in uppercase hex matches");
        assert_eq!(data, BODY);
    }

    #[tokio::test]
    async fn an_asset_not_matching_its_sha256sums_entry_is_refused() {
        let server = MockServer::start().await;
        let wrong = sha256_hex(b"something else");
        let sums = format!("{wrong}  {ASSET}\n");
        // The GitHub digest matches, and must not rescue a SHA256SUMS mismatch.
        let (release, asset) = release_with(
            &server,
            Some(format!("sha256:{}", sha256_hex(BODY))),
            Some(&sums),
        )
        .await;

        let Err(error) = verify(&release, &asset, &options(true)).await else {
            panic!("a mismatch must be refused, even with SPICE_INSTALL_ALLOW_UNVERIFIED=1");
        };
        assert_eq!(
            error.to_string(),
            format!(
                "Checksum mismatch for {ASSET}: expected sha256 {wrong} (SHA256SUMS of release {TAG}), downloaded {}. The download is corrupt or was altered, so it was not installed.",
                sha256_hex(BODY)
            )
        );
    }

    #[tokio::test]
    async fn an_asset_sha256sums_does_not_list_is_refused() {
        let server = MockServer::start().await;
        // A name that only ends with the asset's must not count as listing it.
        let sums = format!("{}  x{ASSET}\n", sha256_hex(BODY));
        let (release, asset) = release_with(
            &server,
            Some(format!("sha256:{}", sha256_hex(BODY))),
            Some(&sums),
        )
        .await;

        let Err(error) = verify(&release, &asset, &options(true)).await else {
            panic!("an unlisted asset must be refused");
        };
        assert_eq!(
            error.to_string(),
            format!(
                "Cannot verify {ASSET}: the SHA256SUMS of release {TAG} does not list it, so it was not installed."
            )
        );
    }

    /// A release that lists SHA256SUMS but fails to serve it must not fall back to the GitHub
    /// digest, which would skip the attestation check.
    #[tokio::test]
    async fn a_listed_sha256sums_that_cannot_be_downloaded_stops_the_install() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_matcher(format!("/{SHA256SUMS}")))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let release = release(vec![
            asset(&server, SHA256SUMS, 100, None),
            asset(
                &server,
                ASSET,
                BODY.len() as u64,
                Some(format!("sha256:{}", sha256_hex(BODY))),
            ),
        ]);
        let asset = &release.assets[1];

        let Err(error) = verify(&release, asset, &options(true)).await else {
            panic!("an unreachable SHA256SUMS must stop the install");
        };
        assert_eq!(
            error.to_string(),
            format!(
                "Cannot verify {ASSET}: release {TAG} publishes SHA256SUMS, but downloading it failed (GitHub API error (status 404): Failed to download asset), so it was not installed. Run the command again later."
            )
        );
    }

    #[tokio::test]
    async fn a_sha256sums_download_is_retried_after_a_server_error() {
        let server = MockServer::start().await;
        let sums = format!("{}  {ASSET}\n", sha256_hex(BODY));
        Mock::given(method("GET"))
            .and(path_matcher(format!("/{SHA256SUMS}")))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .expect(1)
            .with_priority(1)
            .mount(&server)
            .await;
        let (release, asset) = release_with(&server, None, Some(&sums)).await;

        let data = verify(&release, &asset, &options(false))
            .await
            .expect("a 503 followed by the file is accepted");
        assert_eq!(data, BODY);
    }

    #[tokio::test]
    async fn without_sha256sums_the_github_digest_is_checked() {
        let server = MockServer::start().await;
        let (release, asset) = release_with(
            &server,
            Some(format!("sha256:{}", sha256_hex(BODY).to_uppercase())),
            None,
        )
        .await;
        let data = verify(&release, &asset, &options(false))
            .await
            .expect("an asset matching its GitHub digest is accepted");
        assert_eq!(data, BODY);

        let wrong = sha256_hex(b"something else");
        let (release, asset) = release_with(&server, Some(format!("sha256:{wrong}")), None).await;
        let Err(error) = verify(&release, &asset, &options(true)).await else {
            panic!("an asset not matching its GitHub digest must be refused");
        };
        assert_eq!(
            error.to_string(),
            format!(
                "Checksum mismatch for {ASSET}: expected sha256 {wrong} (sha256 GitHub recorded for {ASSET} in release {TAG}), downloaded {}. The download is corrupt or was altered, so it was not installed.",
                sha256_hex(BODY)
            )
        );
    }

    #[tokio::test]
    async fn an_asset_with_no_recorded_sha256_needs_the_opt_out() {
        let server = MockServer::start().await;
        for digest in [
            None,
            Some("sha256:abc".to_string()),
            Some("md5:00".to_string()),
        ] {
            let (release, asset) = release_with(&server, digest.clone(), None).await;

            let Err(error) = verify(&release, &asset, &options(false)).await else {
                panic!("{digest:?}: an unverifiable asset must be refused by default");
            };
            assert_eq!(
                error.to_string(),
                format!(
                    "Cannot verify {ASSET}: release {TAG} has no SHA256SUMS, and GitHub recorded no sha256 for {ASSET}, so it was not installed. To install it without verification, set SPICE_INSTALL_ALLOW_UNVERIFIED=1 and run the command again."
                ),
                "{digest:?}"
            );

            let data = verify(&release, &asset, &options(true))
                .await
                .expect("SPICE_INSTALL_ALLOW_UNVERIFIED=1 installs an unverifiable asset");
            assert_eq!(data, BODY, "{digest:?}");
        }
    }

    /// A fake `gh` that logs its arguments to `args.log`, answers `auth status` with
    /// `auth_status`, and `attestation verify` with `verify_status` and a two-line message.
    #[cfg(unix)]
    fn fake_gh(dir: &std::path::Path, auth_status: u8, verify_status: u8) -> OsString {
        use std::os::unix::fs::PermissionsExt;
        let log = dir.join("args.log");
        let script = dir.join("gh");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\ncase \"$1\" in\n  auth) exit {auth_status} ;;\n  attestation) [ -f \"$3\" ] || exit 99; echo 'Loaded digest'; echo 'no matching attestations found' >&2; exit {verify_status} ;;\nesac\nexit 98\n",
                log = log.display()
            ),
        )
        .expect("fake gh is written");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("fake gh is executable");
        script.into_os_string()
    }

    #[cfg(unix)]
    async fn verify_with_gh(
        auth_status: u8,
        verify_status: u8,
    ) -> (Result<Vec<u8>, GitHubError>, String) {
        let server = MockServer::start().await;
        let sums = format!("{}  {ASSET}\n", sha256_hex(BODY));
        let (release, asset) = release_with(&server, None, Some(&sums)).await;
        let dir = tempfile::tempdir().expect("temp dir is created");
        let options = VerifyOptions {
            allow_unverified: false,
            gh: fake_gh(dir.path(), auth_status, verify_status),
        };
        let result = verify(&release, &asset, &options).await;
        let log = std::fs::read_to_string(dir.path().join("args.log")).unwrap_or_default();
        (result, log)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_logged_in_gh_checks_the_build_attestation() {
        let (result, log) = verify_with_gh(0, 0).await;

        assert_eq!(result.expect("a verified attestation is accepted"), BODY);
        let calls: Vec<_> = log.lines().collect();
        assert_eq!(calls.len(), 2, "{log}");
        assert_eq!(calls[0], "auth status");
        assert!(
            calls[1].starts_with("attestation verify ")
                && calls[1].ends_with(&format!(
                    "/{ASSET} --repo spiceai/spiceai --cert-identity-regex {ATTESTATION_SIGNER} --deny-self-hosted-runners"
                )),
            "{}",
            calls[1]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_build_attestation_is_refused() {
        let (result, _) = verify_with_gh(0, 1).await;

        let Err(error) = result else {
            panic!("a failed attestation must be refused");
        };
        assert_eq!(
            error.to_string(),
            format!(
                "The build attestation of {ASSET} did not verify, so it was not installed: Loaded digest no matching attestations found"
            )
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_gh_that_is_not_logged_in_skips_the_attestation_check() {
        let (result, log) = verify_with_gh(1, 1).await;

        assert_eq!(result.expect("the sha256 alone is checked"), BODY);
        assert_eq!(log, "auth status\n");
    }

    /// A release without SHA256SUMS has no attestations, so `gh` is never asked.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_release_without_sha256sums_does_not_check_attestations() {
        let server = MockServer::start().await;
        let (release, asset) =
            release_with(&server, Some(format!("sha256:{}", sha256_hex(BODY))), None).await;
        let dir = tempfile::tempdir().expect("temp dir is created");
        let options = VerifyOptions {
            allow_unverified: false,
            gh: fake_gh(dir.path(), 0, 1),
        };

        assert_eq!(
            verify(&release, &asset, &options)
                .await
                .expect("the digest matches"),
            BODY
        );
        assert!(!dir.path().join("args.log").exists(), "gh must not run");
    }
}
