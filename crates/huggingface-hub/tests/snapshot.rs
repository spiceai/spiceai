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

//! Downloads into the Hub's cache over real HTTP, against the mock Hub: the cache layout
//! `huggingface_hub` reads, resume, checksums, concurrent writers, offline mode, and loading
//! from the cache when the Hub is down.

use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use huggingface_hub::settings::{Environment, Settings};
use huggingface_hub::snapshot::{Requester, Snapshot};
use huggingface_hub::{RepoId, RepoType};

use crate::mock_hub::{C1, C2, MockFile, MockHub};

const MODEL: &str = "models/o/m";

/// The environment a test resolves its settings in: the mock's endpoint, a cache in a
/// temporary directory, and nothing else.
struct TestEnvironment {
    vars: Vec<(&'static str, String)>,
    home: PathBuf,
}

impl Environment for TestEnvironment {
    fn var(&self, name: &str) -> Option<String> {
        self.vars
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.clone())
    }

    fn home_dir(&self) -> Option<PathBuf> {
        Some(self.home.clone())
    }
}

struct Fixture {
    mock: MockHub,
    home: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let mock = MockHub::start().await;
        Self {
            mock,
            home: tempfile::tempdir().expect("a temporary home"),
        }
    }

    fn cache(&self) -> PathBuf {
        self.home.path().join("hf").join("hub")
    }

    async fn settings(&self, extra: &[(&'static str, &str)]) -> Settings {
        let mut vars = vec![
            ("HF_HOME", self.home.path().join("hf").display().to_string()),
            ("HF_ENDPOINT", self.mock.endpoint()),
        ];
        vars.extend(
            extra
                .iter()
                .map(|(key, value)| (*key, (*value).to_string())),
        );
        Settings::resolve_in(
            &TestEnvironment {
                vars,
                home: self.home.path().to_path_buf(),
            },
            None,
            None,
        )
        .await
        .expect("valid settings")
    }

    async fn open(&self, revision: Option<&str>, extra: &[(&'static str, &str)]) -> Snapshot {
        self.try_open(revision, extra)
            .await
            .unwrap_or_else(|e| panic!("the snapshot opens: {e}"))
    }

    async fn try_open(
        &self,
        revision: Option<&str>,
        extra: &[(&'static str, &str)],
    ) -> Result<Snapshot, huggingface_hub::DownloadError> {
        Snapshot::open(
            self.settings(extra).await,
            requester(),
            RepoId::new(RepoType::Model, "o", "m").expect("a valid repository"),
            revision,
        )
        .await
    }
}

fn requester() -> Requester {
    Requester {
        kind: "model",
        name: "test".to_string(),
        docs_url: "https://spiceai.org/docs/components/models/huggingface",
    }
}

fn weights(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i * 31 % 251) as u8).collect::<Vec<u8>>())
}

fn model_files() -> Vec<(&'static str, MockFile)> {
    vec![
        ("config.json", MockFile::text("{\"model_type\":\"llama\"}")),
        ("tokenizer.json", MockFile::text("{\"version\":\"1.0\"}")),
        ("model.safetensors", MockFile::lfs(weights(300_000))),
        ("onnx/model.onnx", MockFile::lfs(weights(10_000))),
    ]
}

#[tokio::test]
async fn downloads_into_the_layout_huggingface_hub_reads() {
    let fixture = Fixture::new().await;
    let files = model_files();
    fixture.mock.commit(MODEL, C1, files.clone());
    fixture.mock.branch(MODEL, "main", C1);

    let snapshot = fixture.open(None, &[]).await;
    assert_eq!(snapshot.commit(), C1);
    let paths = snapshot
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the files download");

    let repo = fixture.cache().join("models--o--m");
    assert_eq!(
        std::fs::read_to_string(repo.join("refs").join("main")).expect("refs/main"),
        C1
    );
    for (path, (name, file)) in paths.iter().zip([&files[0], &files[2]]) {
        let link = repo.join("snapshots").join(C1).join(name);
        assert_eq!(path, &link);
        // A relative symlink into `blobs/`, named by the file's etag.
        assert_eq!(
            std::fs::read_link(&link).expect("a symlink"),
            Path::new("../../blobs").join(file.etag())
        );
        assert_eq!(
            std::fs::read(&link).expect("the file reads"),
            file.bytes.as_ref()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let blob = repo.join("blobs").join(file.etag());
            let mode = std::fs::metadata(blob)
                .expect("the blob")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o644, "{name}");
        }
    }
    // Only the files asked for are downloaded.
    assert!(
        !repo
            .join("snapshots")
            .join(C1)
            .join("tokenizer.json")
            .exists()
    );
    assert!(!repo.join("snapshots").join(C1).join("onnx").exists());
}

#[tokio::test]
async fn a_second_load_downloads_nothing() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    let snapshot = fixture.open(None, &[]).await;
    snapshot
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the files download");
    assert_eq!(
        fixture.mock.downloads().len(),
        3,
        "{:?}",
        fixture.mock.downloads()
    );

    fixture.mock.clear_seen();
    let snapshot = fixture.open(None, &[]).await;
    snapshot
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the files load from the cache");
    assert!(
        fixture.mock.downloads().is_empty(),
        "{:?}",
        fixture.mock.downloads()
    );
}

#[tokio::test]
async fn files_the_hf_cli_cached_are_reused() {
    let fixture = Fixture::new().await;
    let files = model_files();
    fixture.mock.commit(MODEL, C1, files.clone());
    fixture.mock.branch(MODEL, "main", C1);
    // The layout `hf download` writes, made by hand.
    let repo = fixture.cache().join("models--o--m");
    std::fs::create_dir_all(repo.join("blobs")).expect("blobs");
    std::fs::create_dir_all(repo.join("snapshots").join(C1)).expect("snapshot");
    let weights = &files[2].1;
    std::fs::write(repo.join("blobs").join(weights.etag()), &weights.bytes).expect("a blob");
    #[cfg(unix)]
    std::os::unix::fs::symlink(
        Path::new("../../blobs").join(weights.etag()),
        repo.join("snapshots").join(C1).join("model.safetensors"),
    )
    .expect("a link");

    let snapshot = fixture.open(None, &[]).await;
    snapshot
        .download(&["model.safetensors"])
        .await
        .expect("the cached file loads");
    assert!(fixture.mock.downloads().is_empty());
}

#[tokio::test]
async fn a_blob_another_commit_downloaded_is_linked_not_downloaded() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    let mut changed = model_files();
    changed[0].1 = MockFile::text("{\"model_type\":\"mistral\"}");
    fixture.mock.commit(MODEL, C2, changed);
    fixture.mock.branch(MODEL, "main", C1);
    fixture
        .open(None, &[])
        .await
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the first commit downloads");

    fixture.mock.branch(MODEL, "main", C2);
    fixture.mock.clear_seen();
    let snapshot = fixture.open(Some(C2), &[]).await;
    snapshot
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the second commit downloads");
    // The weights are the same blob: only the changed config is fetched.
    let downloads = fixture.mock.downloads();
    assert_eq!(downloads.len(), 1, "{downloads:?}");
    assert!(downloads[0].path.ends_with("/config.json"), "{downloads:?}");
}

#[tokio::test]
async fn an_interrupted_download_resumes_where_it_stopped() {
    let fixture = Fixture::new().await;
    let files = model_files();
    let weights = files[2].1.clone();
    fixture.mock.commit(MODEL, C1, files);
    fixture.mock.branch(MODEL, "main", C1);
    // A previous run wrote the first 1000 bytes.
    let blobs = fixture.cache().join("models--o--m").join("blobs");
    std::fs::create_dir_all(&blobs).expect("blobs");
    std::fs::write(
        blobs.join(format!("{}.incomplete", weights.etag())),
        &weights.bytes[..1000],
    )
    .expect("a partial file");

    let snapshot = fixture.open(None, &[]).await;
    let paths = snapshot
        .download(&["model.safetensors"])
        .await
        .expect("the download resumes");
    assert_eq!(
        std::fs::read(&paths[0]).expect("the file"),
        weights.bytes.as_ref()
    );
    let ranges: Vec<_> = fixture
        .mock
        .downloads()
        .into_iter()
        .filter(|seen| seen.cdn)
        .map(|seen| seen.range)
        .collect();
    assert_eq!(ranges, vec![Some("bytes=1000-".to_string())]);
    assert!(
        !blobs
            .join(format!("{}.incomplete", weights.etag()))
            .exists()
    );
}

#[tokio::test]
async fn a_connection_reset_mid_file_resumes_byte_identical() {
    let fixture = Fixture::new().await;
    let files = model_files();
    let weights = files[2].1.clone();
    fixture.mock.commit(MODEL, C1, files);
    fixture.mock.branch(MODEL, "main", C1);
    fixture.mock.faults(|faults| faults.truncated = 1);

    let snapshot = fixture.open(None, &[]).await;
    let paths = snapshot
        .download(&["model.safetensors"])
        .await
        .expect("the download resumes after the reset");
    assert_eq!(
        std::fs::read(&paths[0]).expect("the file"),
        weights.bytes.as_ref()
    );
    let cdn: Vec<_> = fixture
        .mock
        .downloads()
        .into_iter()
        .filter(|seen| seen.cdn)
        .collect();
    assert_eq!(cdn.len(), 2, "{cdn:?}");
    assert_eq!(cdn[0].range, None);
    assert_eq!(
        cdn[1].range.as_deref(),
        Some(
            format!(
                "bytes={}-{}",
                weights.bytes.len() / 2,
                weights.bytes.len() - 1
            )
            .as_str()
        )
    );
}

#[tokio::test]
async fn a_corrupted_download_fails_its_checksum_and_is_discarded() {
    let fixture = Fixture::new().await;
    let files = model_files();
    let weights = files[2].1.clone();
    fixture.mock.commit(MODEL, C1, files);
    fixture.mock.branch(MODEL, "main", C1);
    fixture.mock.faults(|faults| faults.corrupted = 1);

    let snapshot = fixture.open(None, &[]).await;
    let error = snapshot
        .download(&["model.safetensors"])
        .await
        .expect_err("altered bytes fail the checksum");
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to download 'model.safetensors' for model 'test' from Hugging Face repo 'o/m' (revision 'main'): the downloaded file does not match the Hub's SHA-256 ({}), so it was discarded. Spice downloads it again on the next attempt; if this repeats, a proxy between Spice and the Hub may be changing the bytes. See: https://spiceai.org/docs/components/models/huggingface",
            weights.sha256()
        )
    );
    let blobs = fixture.cache().join("models--o--m").join("blobs");
    assert!(!blobs.join(weights.etag()).exists());
    assert!(
        !blobs
            .join(format!("{}.incomplete", weights.etag()))
            .exists()
    );

    // The next attempt downloads it whole.
    let paths = snapshot
        .download(&["model.safetensors"])
        .await
        .expect("the retry downloads it");
    assert_eq!(
        std::fs::read(&paths[0]).expect("the file"),
        weights.bytes.as_ref()
    );
}

#[tokio::test]
async fn a_server_that_ignores_range_restarts_from_the_first_byte() {
    let fixture = Fixture::new().await;
    let files = model_files();
    let weights = files[2].1.clone();
    fixture.mock.commit(MODEL, C1, files);
    fixture.mock.branch(MODEL, "main", C1);
    let blobs = fixture.cache().join("models--o--m").join("blobs");
    std::fs::create_dir_all(&blobs).expect("blobs");
    std::fs::write(
        blobs.join(format!("{}.incomplete", weights.etag())),
        &weights.bytes[..1000],
    )
    .expect("a partial file");
    fixture.mock.faults(|faults| faults.ignore_range = true);

    let snapshot = fixture.open(None, &[]).await;
    let paths = snapshot
        .download(&["model.safetensors"])
        .await
        .expect("the download restarts");
    assert_eq!(
        std::fs::read(&paths[0]).expect("the file"),
        weights.bytes.as_ref()
    );
}

#[tokio::test]
async fn rate_limiting_is_waited_out() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture.mock.faults(|faults| faults.rate_limited = 1);

    let snapshot = fixture.open(None, &[]).await;
    snapshot
        .download(&["config.json"])
        .await
        .expect("the download succeeds after the wait");
    let revision_lookups = fixture
        .mock
        .seen()
        .iter()
        .filter(|seen| seen.path.contains("/revision/"))
        .count();
    assert_eq!(revision_lookups, 2);
}

#[tokio::test]
async fn concurrent_downloads_of_one_file_download_it_once() {
    let fixture = Fixture::new().await;
    let files = model_files();
    let weights = files[2].1.clone();
    fixture.mock.commit(MODEL, C1, files);
    fixture.mock.branch(MODEL, "main", C1);
    fixture
        .mock
        .faults(|faults| faults.delay = Some(Duration::from_millis(300)));

    let (one, two) = tokio::join!(
        async {
            fixture
                .open(None, &[])
                .await
                .download(&["model.safetensors"])
                .await
        },
        async {
            fixture
                .open(None, &[])
                .await
                .download(&["model.safetensors"])
                .await
        }
    );
    for paths in [one, two] {
        let paths = paths.expect("both downloads succeed");
        assert_eq!(
            std::fs::read(&paths[0]).expect("the file"),
            weights.bytes.as_ref()
        );
    }
    let cdn = fixture
        .mock
        .downloads()
        .into_iter()
        .filter(|seen| seen.cdn)
        .count();
    assert_eq!(
        cdn, 1,
        "the second download waited for the first and reused its blob"
    );
}

#[tokio::test]
async fn offline_mode_reads_only_the_cache() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture
        .open(None, &[])
        .await
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the files download");

    fixture.mock.clear_seen();
    let snapshot = fixture.open(None, &[("HF_HUB_OFFLINE", "1")]).await;
    assert_eq!(snapshot.commit(), C1);
    snapshot
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the cached files load offline");
    assert!(fixture.mock.seen().is_empty(), "{:?}", fixture.mock.seen());
}

#[tokio::test]
async fn offline_mode_with_a_cold_cache_says_how_to_fill_it() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);

    let error = fixture
        .try_open(None, &[("HF_HUB_OFFLINE", "true")])
        .await
        .expect_err("nothing is cached");
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to download model 'test' from Hugging Face repo 'o/m' (revision 'main'): `HF_HUB_OFFLINE` is set and revision 'main' of the repo is not in the Hugging Face cache at '{}'. Run `hf download o/m --revision main` while online, or unset `HF_HUB_OFFLINE`. See: https://spiceai.org/docs/components/models/huggingface",
            fixture.cache().display()
        )
    );
    assert!(!error.is_configuration_error());
    assert!(fixture.mock.seen().is_empty());
}

#[tokio::test]
async fn a_hub_outage_loads_the_cached_snapshot() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture
        .open(None, &[])
        .await
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the files download");

    fixture.mock.faults(|faults| faults.hub_down = true);
    let snapshot = fixture.open(None, &[]).await;
    assert_eq!(snapshot.commit(), C1);
    snapshot
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the cached files load");

    // A file the cache does not hold cannot be loaded.
    let error = snapshot
        .download(&["tokenizer.json"])
        .await
        .expect_err("the tokenizer was never downloaded");
    assert_eq!(
        error.to_string(),
        "Failed to download model 'test' from Hugging Face repo 'o/m' (revision 'main'): the Hugging Face Hub could not be reached (HTTP 503), and the Hugging Face cache does not hold 'tokenizer.json'. Spice retries the download; check the network connection. See: https://spiceai.org/docs/components/models/huggingface"
    );
    assert!(!error.is_configuration_error());
}

#[tokio::test]
async fn a_hub_outage_with_a_cold_cache_fails_with_the_hub_error() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture.mock.faults(|faults| faults.hub_down = true);

    let error = fixture
        .try_open(None, &[])
        .await
        .expect_err("nothing is cached");
    // `HF_HUB_OFFLINE` is no way out: the cache holds no snapshot to load.
    assert_eq!(
        error.to_string(),
        "Failed to download model 'test' from Hugging Face repo 'o/m' (revision 'main'): the Hugging Face Hub returned HTTP 503 after 5 attempt(s): The Hub is down. Spice retries the download; check the network connection. See: https://spiceai.org/docs/components/models/huggingface"
    );
    assert!(!error.is_configuration_error());
}

#[tokio::test]
async fn a_missing_revision_does_not_fall_back_to_the_cache() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    let error = fixture
        .try_open(Some("v9"), &[])
        .await
        .expect_err("the revision does not exist");
    assert_eq!(
        error.to_string(),
        "Failed to download model 'test' from Hugging Face repo 'o/m' (revision 'v9'): the repo has no revision 'v9'. Use an existing branch, tag or commit. See: https://spiceai.org/docs/components/models/huggingface"
    );
}

#[tokio::test]
async fn a_rejected_token_is_named_by_its_source() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture.mock.require_token("hf_right");

    let error = fixture
        .try_open(None, &[("HF_TOKEN", "hf_wrong")])
        .await
        .expect_err("the token is rejected");
    assert_eq!(
        error.to_string(),
        "Failed to download model 'test' from Hugging Face repo 'o/m' (revision 'main'): the Hub rejected the token from `HF_TOKEN` (HTTP 401). Set `HF_TOKEN` to a token with access to this repo, or set `hf_token`. See: https://spiceai.org/docs/components/models/huggingface"
    );

    let token_file = fixture.home.path().join("hf").join("token");
    std::fs::create_dir_all(token_file.parent().expect("a parent")).expect("HF_HOME");
    std::fs::write(&token_file, "hf_stale\n").expect("a token file");
    let error = fixture
        .try_open(None, &[])
        .await
        .expect_err("the token is rejected");
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to download model 'test' from Hugging Face repo 'o/m' (revision 'main'): the Hub rejected the token from '{}' (HTTP 401), which may have expired. Run `hf auth login`, or set `hf_token`. See: https://spiceai.org/docs/components/models/huggingface",
            token_file.display()
        )
    );

    std::fs::write(&token_file, "hf_right").expect("a refreshed token file");
    fixture
        .open(None, &[])
        .await
        .download(&["config.json"])
        .await
        .expect("the token file's token is accepted");
}

#[tokio::test]
async fn a_path_that_leaves_the_snapshot_is_refused() {
    let fixture = Fixture::new().await;
    fixture
        .mock
        .commit(MODEL, C1, vec![("a/../../evil.json", MockFile::text("{}"))]);
    fixture.mock.branch(MODEL, "main", C1);
    let snapshot = fixture.open(None, &[]).await;
    let error = snapshot
        .download(&["a/../../evil.json"])
        .await
        .expect_err("the path escapes the snapshot");
    assert!(error.to_string().contains("'a/../../evil.json'"), "{error}");
    // Refused before a byte of it was fetched.
    assert!(
        fixture.mock.downloads().is_empty(),
        "{:?}",
        fixture.mock.downloads()
    );
    assert!(
        !fixture
            .cache()
            .join("models--o--m")
            .join("evil.json")
            .exists()
    );
    assert!(!fixture.cache().join("evil.json").exists());
}

#[tokio::test]
async fn a_gated_repo_names_the_page_that_grants_access() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture.mock.gate(MODEL);

    let snapshot = fixture.open(None, &[("HF_TOKEN", "hf_token")]).await;
    // A file in git, and an LFS file whose content id the Hub masks from an account without
    // access: both report the gate, not a malformed listing.
    for file in ["config.json", "model.safetensors"] {
        let error = snapshot
            .download(&[file])
            .await
            .expect_err("the repo is gated");
        assert_eq!(
            error.to_string(),
            format!(
                "Failed to download '{file}' for model 'test' from Hugging Face repo 'o/m' (revision 'main'): the repo is gated. Accept its access conditions at {}/o/m with the account the token from `HF_TOKEN` belongs to. See: https://spiceai.org/docs/components/models/huggingface",
                fixture.mock.endpoint()
            )
        );
        assert!(!error.is_configuration_error());
    }

    let snapshot = fixture.open(None, &[]).await;
    let error = snapshot
        .download(&["model.safetensors"])
        .await
        .expect_err("the repo is gated");
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to download 'model.safetensors' for model 'test' from Hugging Face repo 'o/m' (revision 'main'): the repo is gated and no token is set. Accept its access conditions at {}/o/m, then set `hf_token`, set `HF_TOKEN`, or run `hf auth login`. See: https://spiceai.org/docs/components/models/huggingface",
            fixture.mock.endpoint()
        )
    );
}

#[tokio::test]
async fn offline_mode_names_a_needed_file_the_cache_lacks() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture
        .open(None, &[])
        .await
        .download(&["config.json"])
        .await
        .expect("the config downloads");

    let snapshot = fixture.open(None, &[("HF_HUB_OFFLINE", "1")]).await;
    let error = snapshot
        .download(&["config.json", "tokenizer.json"])
        .await
        .expect_err("the tokenizer was never downloaded");
    assert_eq!(
        error.to_string(),
        format!(
            "Failed to download model 'test' from Hugging Face repo 'o/m' (revision 'main'): `HF_HUB_OFFLINE` is set and the Hugging Face cache at '{}' does not hold 'tokenizer.json'. Run `hf download o/m --revision main` while online, or unset `HF_HUB_OFFLINE`. See: https://spiceai.org/docs/components/models/huggingface",
            fixture.cache().display()
        )
    );
    // `hf download` can fill the cache while Spice runs: a retry may succeed.
    assert!(!error.is_configuration_error());
}

#[tokio::test]
async fn a_connection_failure_keeps_its_kind_through_the_revision_cache() {
    // A port nothing listens on.
    let closed = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a loopback port");
    let hub = std::sync::Arc::new(
        huggingface_hub::Hub::new(
            huggingface_hub::HubConfig {
                endpoint: url::Url::parse(&format!("http://{closed}")).expect("a URL"),
                token: None,
            },
            tokio::runtime::Handle::current(),
        )
        .expect("a client"),
    );
    let repo = RepoId::new(RepoType::Model, "o", "m").expect("a valid repository");
    let error = hub
        .commit(&repo, "main")
        .await
        .expect_err("nothing listens");
    // The lookup is shared between concurrent callers through a cache; the error each gets
    // still says the connection failed, which is what a load that falls back reports.
    assert!(
        matches!(&error, huggingface_hub::Error::Request { source, .. } if source.is_connect()),
        "{error:?}"
    );
}

#[tokio::test]
async fn the_token_goes_to_the_hub_and_never_to_the_cdn() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture.mock.require_token("hf_secret");

    fixture
        .open(None, &[("HF_TOKEN", "hf_secret")])
        .await
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the files download");
    let seen = fixture.mock.seen();
    assert!(seen.iter().any(|request| request.cdn), "{seen:?}");
    for request in seen {
        let expected = (!request.cdn).then(|| "Bearer hf_secret".to_string());
        assert_eq!(request.authorization, expected, "{}", request.path);
    }
}
