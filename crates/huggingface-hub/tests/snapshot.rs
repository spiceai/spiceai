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

use crate::mock_hub::{C1, C2, MockFile, MockHub, RevisionFiles};

const MODEL: &str = "models/o/m";
/// The request that resolves `main` of the test model and describes its files.
const REVISION_DESCRIBED: &str = "/api/models/o/m/revision/main?blobs=true&expand%5B%5D=sha&expand%5B%5D=lastModified&expand%5B%5D=siblings";

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
        self.try_open_for(requester(), revision, extra).await
    }

    async fn open_paths(&self, paths: &[&str]) -> Snapshot {
        Snapshot::open_paths(
            self.settings(&[]).await,
            requester(),
            RepoId::new(RepoType::Model, "o", "m").expect("a valid repository"),
            None,
            paths,
        )
        .await
        .unwrap_or_else(|e| panic!("the snapshot opens: {e}"))
    }

    async fn try_open_for(
        &self,
        requester: Requester,
        revision: Option<&str>,
        extra: &[(&'static str, &str)],
    ) -> Result<Snapshot, huggingface_hub::DownloadError> {
        Snapshot::open(
            self.settings(extra).await,
            requester,
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
        retried: true,
    }
}

fn weights(len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|i| u8::try_from(i * 31 % 251).expect("a remainder of 251 fits in a byte"))
            .collect::<Vec<u8>>(),
    )
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
    // The request that resolves the revision describes every file too: a cold load lists
    // nothing more, and a warm one makes that request alone.
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
    assert_eq!(fixture.mock.api_paths(), vec![REVISION_DESCRIBED]);

    fixture.mock.clear_seen();
    let snapshot = fixture.open(None, &[]).await;
    snapshot
        .download(&["config.json", "model.safetensors"])
        .await
        .expect("the files load from the cache");
    assert_eq!(fixture.mock.seen_paths(), vec![REVISION_DESCRIBED]);
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
async fn a_server_error_is_retried() {
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture.mock.faults(|faults| faults.unavailable = 2);

    let snapshot = fixture.open(None, &[]).await;
    snapshot
        .download(&["config.json"])
        .await
        .expect("the download succeeds after the retries");
    let revision_lookups = fixture
        .mock
        .seen()
        .iter()
        .filter(|seen| seen.path.contains("/revision/"))
        .count();
    assert_eq!(
        revision_lookups, 3,
        "two answers of HTTP 503, then the revision"
    );
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
async fn a_renamed_repo_is_read_through_the_hubs_redirect() {
    // The Hub redirects every request that names a repository's old name to its new one, so
    // a Spicepod written before a rename keeps loading, as it does with `huggingface_hub`.
    let fixture = Fixture::new().await;
    let files = model_files();
    let weights = files[2].1.clone();
    fixture.mock.commit(MODEL, C1, files);
    fixture.mock.branch(MODEL, "main", C1);
    fixture.mock.rename("models/o/old", MODEL);

    let snapshot = Snapshot::open(
        fixture.settings(&[]).await,
        requester(),
        RepoId::new(RepoType::Model, "o", "old").expect("a valid repository"),
        None,
    )
    .await
    .unwrap_or_else(|e| panic!("the renamed repository opens: {e}"));
    assert_eq!(snapshot.commit(), C1);
    let paths = snapshot
        .download(&["config.json", "model.safetensors"])
        .await
        .unwrap_or_else(|e| panic!("the renamed repository's files download: {e}"));
    assert_eq!(
        std::fs::read(&paths[1]).expect("the weights"),
        weights.bytes.as_ref()
    );
    // The cache keeps the name the Spicepod gave, as `huggingface_hub` does.
    assert_eq!(
        paths[1],
        fixture
            .cache()
            .join("models--o--old")
            .join("snapshots")
            .join(C1)
            .join("model.safetensors")
    );
    // Every request went to the old name first and was redirected.
    let seen: Vec<String> = fixture
        .mock
        .seen()
        .into_iter()
        .filter(|seen| !seen.cdn)
        .map(|seen| seen.path)
        .collect();
    assert!(
        seen.iter()
            .any(|path| path.starts_with("/api/models/o/old/revision/main"))
            && seen
                .iter()
                .any(|path| path.starts_with("/api/models/o/m/revision/main"))
            && seen.iter().any(|path| path.starts_with("/o/old/resolve/"))
            && seen.iter().any(|path| path.starts_with("/o/m/resolve/")),
        "{seen:#?}"
    );
}

#[tokio::test]
async fn a_component_spice_loads_once_says_to_restart() {
    // Spice loads an embedding model or a reranker once, so only a restart downloads it again.
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture.mock.faults(|faults| faults.hub_down = true);

    let embedding_model = Requester {
        kind: "embedding model",
        name: "e5".to_string(),
        docs_url: "https://spiceai.org/docs/components/embeddings/huggingface",
        retried: false,
    };
    let error = fixture
        .try_open_for(embedding_model, None, &[])
        .await
        .expect_err("nothing is cached");
    assert_eq!(
        error.to_string(),
        "Failed to download embedding model 'e5' from Hugging Face repo 'o/m' (revision 'main'): the Hugging Face Hub returned HTTP 503 after 5 attempt(s): The Hub is down. Check the network connection, then restart Spice. See: https://spiceai.org/docs/components/embeddings/huggingface"
    );
}

#[tokio::test]
async fn open_paths_describes_only_the_files_named() {
    // A component that reads one file of a repository describes only that file. Warm, it
    // costs one request: the answer that names the commit.
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);

    let snapshot = fixture.open_paths(&["tokenizer.json", "absent.json"]).await;
    assert_eq!(snapshot.commit(), C1);
    assert_eq!(
        snapshot
            .files()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        vec!["tokenizer.json"]
    );
    let paths = snapshot
        .download(&["tokenizer.json"])
        .await
        .unwrap_or_else(|e| panic!("the file downloads: {e}"));
    assert_eq!(
        std::fs::read_to_string(&paths[0]).expect("the file"),
        "{\"version\":\"1.0\"}"
    );
    // Cold: the answer that names the commit describes the file, which is then downloaded.
    assert_eq!(
        fixture.mock.seen_paths(),
        vec![
            REVISION_DESCRIBED.to_string(),
            format!("/o/m/resolve/{C1}/tokenizer.json"),
        ]
    );
    // A file the repository does not have is named when it is downloaded.
    let error = snapshot
        .download(&["absent.json"])
        .await
        .expect_err("the repository has no such file");
    assert_eq!(
        error.to_string(),
        "Failed to download model 'test' from Hugging Face repo 'o/m' (revision 'main'): the repo has no 'absent.json'. See: https://spiceai.org/docs/components/models/huggingface"
    );

    // Warm: a `HEAD` confirms the commit `main` names is the one the cache holds the file at,
    // and the cached file is used as it is.
    fixture.mock.clear_seen();
    let warm = fixture.open_paths(&["tokenizer.json"]).await;
    let located = warm
        .download(&["tokenizer.json"])
        .await
        .unwrap_or_else(|e| panic!("the cached file loads: {e}"));
    assert_eq!(located, paths);
    assert_eq!(
        fixture.mock.seen_paths(),
        vec!["/o/m/resolve/main/tokenizer.json"]
    );
}

#[tokio::test]
async fn open_paths_follows_a_branch_that_moved() {
    // The cache holds the file at the commit `main` named before; `main` has moved since, so
    // the `HEAD` names another commit, at which the file is described and downloaded.
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture
        .open_paths(&["tokenizer.json"])
        .await
        .download(&["tokenizer.json"])
        .await
        .unwrap_or_else(|e| panic!("the first commit's file downloads: {e}"));
    fixture.mock.commit(
        MODEL,
        C2,
        vec![("tokenizer.json", MockFile::text("{\"version\":\"2.0\"}"))],
    );
    fixture.mock.branch(MODEL, "main", C2);
    fixture.mock.clear_seen();

    let snapshot = fixture.open_paths(&["tokenizer.json"]).await;
    assert_eq!(snapshot.commit(), C2);
    let paths = snapshot
        .download(&["tokenizer.json"])
        .await
        .unwrap_or_else(|e| panic!("the new commit's file downloads: {e}"));
    assert_eq!(
        std::fs::read_to_string(&paths[0]).expect("the file"),
        "{\"version\":\"2.0\"}"
    );
    assert_eq!(
        fixture.mock.seen_paths(),
        vec![
            "/o/m/resolve/main/tokenizer.json".to_string(),
            REVISION_DESCRIBED.to_string(),
            format!("/o/m/resolve/{C2}/tokenizer.json"),
        ]
    );
}

#[tokio::test]
async fn an_endpoint_that_refuses_to_describe_files_is_asked_for_the_revision_alone() {
    // An endpoint that answers HTTP 400 to the request for the revision with its files'
    // blobs, a combination `huggingface_hub` never sends: the revision is asked for alone, the
    // files are listed, and the load downloads as from the Hub.
    let fixture = Fixture::new().await;
    let files = model_files();
    fixture.mock.commit(MODEL, C1, files.clone());
    fixture.mock.branch(MODEL, "main", C1);
    fixture
        .mock
        .faults(|faults| faults.revision_files = RevisionFiles::Refused);

    let paths = fixture
        .open(None, &[])
        .await
        .download(&["model.safetensors"])
        .await
        .unwrap_or_else(|e| panic!("the file downloads: {e}"));
    assert_eq!(
        std::fs::read(&paths[0]).expect("the weights"),
        files[2].1.bytes.as_ref()
    );
    assert_eq!(
        fixture.mock.api_paths(),
        vec![
            REVISION_DESCRIBED.to_string(),
            "/api/models/o/m/revision/main?expand%5B%5D=sha&expand%5B%5D=lastModified".to_string(),
            format!("/api/models/o/m/tree/{C1}?recursive=true&expand=false"),
        ]
    );
}

#[tokio::test]
async fn an_endpoint_that_does_not_describe_files_is_listed() {
    // A mirror that answers the revision without its files: they are listed, or described one
    // path at a time, and download as from the Hub.
    let fixture = Fixture::new().await;
    fixture.mock.commit(MODEL, C1, model_files());
    fixture.mock.branch(MODEL, "main", C1);
    fixture
        .mock
        .faults(|faults| faults.revision_files = RevisionFiles::Omitted);

    let snapshot = fixture.open(None, &[]).await;
    let files: Vec<&str> = snapshot.files().map(|file| file.path.as_str()).collect();
    assert_eq!(
        files,
        vec![
            "config.json",
            "model.safetensors",
            "onnx/model.onnx",
            "tokenizer.json"
        ]
    );
    snapshot
        .download(&["config.json", "model.safetensors"])
        .await
        .unwrap_or_else(|e| panic!("the files download: {e}"));
    let seen = fixture.mock.seen_paths();
    assert!(
        seen.iter()
            .any(|path| path.starts_with(&format!("/api/models/o/m/tree/{C1}"))),
        "{seen:#?}"
    );

    fixture.mock.clear_seen();
    let path = fixture
        .open_paths(&["tokenizer.json"])
        .await
        .download(&["tokenizer.json"])
        .await
        .unwrap_or_else(|e| panic!("the file downloads: {e}"));
    assert_eq!(
        std::fs::read_to_string(&path[0]).expect("the file"),
        "{\"version\":\"1.0\"}"
    );
    assert_eq!(
        fixture.mock.seen_paths(),
        vec![
            REVISION_DESCRIBED.to_string(),
            format!("/api/models/o/m/paths-info/{C1}"),
            format!("/o/m/resolve/{C1}/tokenizer.json"),
        ]
    );
}

#[tokio::test]
async fn files_xet_cannot_serve_download_through_the_bridge() {
    // Spice reads Xet only from the public Hub; here Xet is turned on for the mock, whose Xet
    // storage refuses the read token, so every file of the load downloads through the bridge.
    let fixture = Fixture::new().await;
    let files = model_files();
    fixture.mock.commit(MODEL, C1, files.clone());
    fixture.mock.branch(MODEL, "main", C1);
    let mut settings = fixture.settings(&[]).await;
    assert_eq!(settings.xet, None, "the mock is not the public Hub");
    settings.xet = Some(fixture.cache().join("xet"));
    let snapshot = Snapshot::open(
        settings,
        requester(),
        RepoId::new(RepoType::Model, "o", "m").expect("a valid repository"),
        None,
    )
    .await
    .unwrap_or_else(|e| panic!("the snapshot opens: {e}"));

    let paths = snapshot
        .download(&["config.json", "model.safetensors", "onnx/model.onnx"])
        .await
        .unwrap_or_else(|e| panic!("the files download through the bridge: {e}"));
    for (path, (_, file)) in paths.iter().zip([&files[0], &files[2], &files[3]]) {
        assert_eq!(
            std::fs::read(path).expect("a downloaded file"),
            file.bytes.as_ref()
        );
    }
    // The revision, one description of both LFS files, which names their Xet hashes, and the
    // refused read token. Each file then comes through the bridge: one request for
    // `config.json`, and a `resolve` and its CDN redirect for each LFS file.
    assert_eq!(
        fixture.mock.api_paths(),
        vec![
            REVISION_DESCRIBED.to_string(),
            format!("/api/models/o/m/paths-info/{C1}"),
            format!("/api/models/o/m/xet-read-token/{C1}"),
        ]
    );
    assert_eq!(
        fixture.mock.downloads().len(),
        5,
        "{:?}",
        fixture.mock.downloads()
    );
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
                token_scope: huggingface_hub::settings::TokenScope::Any,
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
