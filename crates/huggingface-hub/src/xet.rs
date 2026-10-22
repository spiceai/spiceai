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

//! Reads of files the Hub stores in Xet, through `hf-xet`.
//!
//! The Hub serves a Xet file to any client through its LFS bridge, which reconstructs the
//! whole file on the server. `hf-xet` instead fetches the file's chunks from the CAS server
//! itself, many at once. The tree and `paths-info` endpoints name a Xet file's hash.
//!
//! One download group serves every file of a load: it asks
//! `/api/{type}s/{repo}/xet-read-token/{commit}` for the CAS read token, with the Hub token,
//! and asks again before the token expires. That request goes only to the Hub's endpoint; the
//! read token goes only to the CAS server the Hub names.
//!
//! Every read runs on one runtime Spice owns, sized by `cpu_budget`, and keeps its shard cache
//! in the directory [`Settings::xet`](crate::Settings::xet) names, with no chunk cache. The
//! `HF_XET_*` variables apply as `xet-core` defines them. Telemetry is off unless
//! `HF_XET_TELEMETRY_ENABLED=1`.

use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, OnceLock};

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::GetRange;
use parking_lot::Mutex;
use runtime_async::ManagedTokioRuntime;
use tokio::sync::mpsc;
use xet::XetError;
use xet::xet_session::{
    HeaderMap, XetConfig, XetDownloadStreamGroup, XetFileInfo, XetSession, XetSessionBuilder,
    header::AUTHORIZATION,
};

use crate::client::{self, Hub, TreeEntry};
use crate::location::RepoId;
use crate::snapshot::AbortOnDrop;

/// The most of a Xet read handed to the download writer at once. `hf-xet` delivers a file in
/// terms of up to 64 MiB, and the writer's queue holds several items.
const MAX_CHUNK: usize = 1 << 20;

/// The runtime every Xet read runs on, owned by Spice so its threads count against the CPU
/// budget, rather than a pool `hf-xet` would size from the host.
static RUNTIME: OnceLock<Result<ManagedTokioRuntime, String>> = OnceLock::new();

/// One session per shard cache directory. Sessions share the runtime, and every component
/// that resolves the same settings shares a directory.
static SESSIONS: LazyLock<Mutex<HashMap<PathBuf, XetSession>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn runtime() -> Result<&'static ManagedTokioRuntime, XetError> {
    RUNTIME
        .get_or_init(|| {
            ManagedTokioRuntime::builder()
                .with_thread_name("hf-xet")
                .with_worker_threads(cpu_budget::cpu_budget().hf_xet_runtime_worker_threads())
                .build()
                .map_err(|error| format!("the Xet runtime could not start: {error}"))
        })
        .as_ref()
        .map_err(|message| XetError::Configuration(message.clone()))
}

/// The settings Spice gives `hf-xet`: no chunk cache and no telemetry, then the `HF_XET_*`
/// variables over both.
fn config() -> Result<XetConfig, XetError> {
    let config = XetConfig::default()
        .with_config("chunk_cache.size_bytes", 0)
        .and_then(|config| config.with_config("telemetry.enabled", false))
        .map_err(|error| XetError::Configuration(error.to_string()))?;
    Ok(config.with_env_overrides())
}

fn session(cache_dir: &Path) -> Result<XetSession, XetError> {
    let mut sessions = SESSIONS.lock();
    if let Some(session) = sessions.get(cache_dir) {
        return Ok(session.clone());
    }
    let session = XetSessionBuilder::new_with_config(config()?)
        .with_cache_dir(cache_dir)
        .with_tokio_handle(runtime()?.handle().clone())
        .build()?;
    sessions.insert(cache_dir.to_path_buf(), session.clone());
    Ok(session)
}

/// The Xet files of one load: one download group for them all, and each one's Xet hash.
pub(crate) struct XetFiles {
    group: XetDownloadStreamGroup,
    hashes: HashMap<String, String>,
}

impl XetFiles {
    /// The Xet files among `entries`, the files a load of `repo` at `commit` downloads, with
    /// the shard cache in `cache_dir`. Files whose description does not name a Xet hash are
    /// described again, together in one request. `None` when no file is in Xet, or Xet cannot
    /// start; every file is then read through the Hub's LFS bridge.
    pub(crate) async fn of(
        hub: &Hub,
        cache_dir: &Path,
        repo: &RepoId,
        commit: &str,
        entries: &[TreeEntry],
    ) -> Option<Self> {
        let mut hashes: HashMap<String, String> = entries
            .iter()
            .filter_map(|entry| Some((entry.path.clone(), entry.xet_hash.clone()?)))
            .collect();
        let unnamed: Vec<&str> = entries
            .iter()
            .filter(|entry| entry.lfs_oid().is_some() && !hashes.contains_key(&entry.path))
            .map(|entry| entry.path.as_str())
            .collect();
        if !unnamed.is_empty() {
            match hub.entries(repo, commit, &unnamed).await {
                Ok(described) => hashes.extend(
                    described
                        .into_iter()
                        .filter_map(|entry| Some((entry.path, entry.xet_hash?))),
                ),
                // The bridge's own requests report what went wrong.
                Err(error) => {
                    tracing::debug!(
                        "Reading Hugging Face {} '{repo}' through the Hub's LFS bridge: its Xet files could not be described: {error}",
                        repo.kind()
                    );
                    return None;
                }
            }
        }
        if hashes.is_empty() {
            return None;
        }
        match group(hub, cache_dir, repo, commit).await {
            Ok(group) => Some(Self { group, hashes }),
            Err(error) => {
                tracing::warn!(
                    "Failed to start reading Hugging Face {} '{repo}' through Xet, so its files download through the Hub's LFS bridge. Cause: {error}",
                    repo.kind()
                );
                None
            }
        }
    }

    /// Whether the file at `path` is one of them.
    pub(crate) fn contains(&self, path: &str) -> bool {
        self.hashes.contains_key(path)
    }

    /// Reads `entry` of `repo` at `commit` from byte `start` to its end, when it is a Xet file:
    /// through Xet, and through the Hub's LFS bridge from the first byte Xet did not deliver if
    /// Xet fails or ends early. `None` for a file not in Xet.
    pub(crate) fn read(
        &self,
        hub: &Arc<Hub>,
        repo: &RepoId,
        commit: &str,
        entry: &TreeEntry,
        start: u64,
    ) -> Option<BoxStream<'static, client::Result<Bytes>>> {
        let hash = self.hashes.get(&entry.path)?;
        let reading = Reading {
            hub: Arc::clone(hub),
            group: self.group.clone(),
            repo: repo.clone(),
            commit: commit.to_string(),
            info: XetFileInfo {
                hash: hash.clone(),
                file_size: Some(entry.size),
                sha256: entry.lfs_oid().map(ToString::to_string),
            },
            path: entry.path.clone(),
            size: entry.size,
            next: start,
            state: State::Start,
        };
        Some(
            futures::stream::unfold(reading, |mut reading| async move {
                let chunk = reading.next_chunk().await?;
                Some((chunk, reading))
            })
            .boxed(),
        )
    }
}

/// One download group for the Xet files of `repo` at `commit`: its streams share the CAS read
/// token and the connection pool.
async fn group(
    hub: &Hub,
    cache_dir: &Path,
    repo: &RepoId,
    commit: &str,
) -> Result<XetDownloadStreamGroup, XetError> {
    let mut headers = HeaderMap::new();
    if let Some(authorization) = hub.authorization() {
        headers.insert(AUTHORIZATION, authorization.clone());
    }
    session(cache_dir)?
        .new_download_stream_group()?
        .with_token_refresh_url(hub.xet_read_token_url(repo, commit).to_string(), headers)
        .build()
        .await
}

/// A read of one Xet file, from `next` on.
struct Reading {
    hub: Arc<Hub>,
    group: XetDownloadStreamGroup,
    repo: RepoId,
    commit: String,
    info: XetFileInfo,
    path: String,
    size: u64,
    /// The first byte not yet delivered.
    next: u64,
    state: State,
}

enum State {
    /// Xet has not been asked yet.
    Start,
    /// Reading through Xet: a task on the Xet runtime sends each term `hf-xet` delivers, and
    /// `pending` is what is left of the last one.
    Xet {
        terms: mpsc::Receiver<Result<Bytes, XetError>>,
        pending: Bytes,
        _reader: AbortOnDrop,
    },
    /// Xet failed or ended early: the rest is read through the bridge.
    BridgeStart,
    Bridge(BoxStream<'static, client::Result<Bytes>>),
    Done,
}

impl Reading {
    async fn next_chunk(&mut self) -> Option<client::Result<Bytes>> {
        loop {
            let cause = match &mut self.state {
                State::Start => match runtime() {
                    Ok(runtime) => {
                        // One term at a time: each can be 64 MiB.
                        let (sender, terms) = mpsc::channel(1);
                        let reader = runtime.handle().spawn(read_terms(
                            self.group.clone(),
                            self.info.clone(),
                            self.next..self.size,
                            sender,
                        ));
                        self.state = State::Xet {
                            terms,
                            pending: Bytes::new(),
                            _reader: AbortOnDrop(reader),
                        };
                        continue;
                    }
                    Err(error) => error.to_string(),
                },
                State::Xet { terms, pending, .. } => {
                    if !pending.is_empty() {
                        let chunk = pending.split_to(pending.len().min(MAX_CHUNK));
                        self.next += chunk.len() as u64;
                        return Some(Ok(chunk));
                    }
                    match terms.recv().await {
                        Some(Ok(term)) => {
                            *pending = term;
                            continue;
                        }
                        None if self.next >= self.size => {
                            self.state = State::Done;
                            continue;
                        }
                        None => format!(
                            "Xet ended the file after {} of its {} bytes",
                            self.next, self.size
                        ),
                        Some(Err(error)) => error.to_string(),
                    }
                }
                State::BridgeStart => {
                    let range = (self.next > 0).then_some(GetRange::Offset(self.next));
                    match self
                        .hub
                        .read(&self.repo, &self.commit, &self.path, range)
                        .await
                    {
                        Ok(read) => {
                            self.state = State::Bridge(read.stream);
                            continue;
                        }
                        Err(error) => {
                            self.state = State::Done;
                            return Some(Err(error));
                        }
                    }
                }
                State::Bridge(stream) => {
                    if let Some(chunk) = stream.next().await {
                        return Some(chunk);
                    }
                    self.state = State::Done;
                    continue;
                }
                State::Done => return None,
            };
            tracing::warn!(
                "Failed to download '{}' of Hugging Face {} '{}' through Xet, so the rest of it downloads through the Hub's LFS bridge. Cause: {cause}",
                self.path,
                self.repo.kind(),
                self.repo
            );
            self.state = State::BridgeStart;
        }
    }
}

/// Reads `range` of the Xet file `info` through `group` and sends each term to `terms`, then
/// the error that ends the read, if one does. It runs on the Xet runtime, so the chunk fetches
/// and decompression `hf-xet` spawns run there too.
async fn read_terms(
    group: XetDownloadStreamGroup,
    info: XetFileInfo,
    range: Range<u64>,
    terms: mpsc::Sender<Result<Bytes, XetError>>,
) {
    let mut stream = match group.download_stream(info, Some(range)).await {
        Ok(stream) => stream,
        Err(error) => {
            let _ = terms.send(Err(error)).await;
            return;
        }
    };
    loop {
        match stream.next().await {
            Ok(Some(term)) => {
                if terms.send(Ok(term)).await.is_err() {
                    return;
                }
            }
            Ok(None) => return,
            Err(error) => {
                let _ = terms.send(Err(error)).await;
                return;
            }
        }
    }
}
