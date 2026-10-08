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

//! The object store Hugging Face dataset files are read through.
//!
//! One store, registered under `hf://datasets`, serves every dataset. An object's path names
//! its repository and commit as well as the file, `{owner}/{name}@{commit}/{path}`, so the URL
//! of an object is the `hf://datasets/{owner}/{name}@{commit}/{path}` location `HfFileSystem`
//! and `DuckDB` read. The Hub client (endpoint and token) each repository is read with is the
//! one its dataset registered.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use parking_lot::RwLock;
use url::Url;

use crate::hub::{self, EntryKind, Hub, TreeEntry};
use crate::location::RepoId;

const STORE_NAME: &str = "HuggingFace";

/// The URL the store is registered under.
pub static STORE_URL: LazyLock<Url> = LazyLock::new(|| {
    Url::parse("hf://datasets/").unwrap_or_else(|_| unreachable!("a valid URL literal"))
});

static STORE: LazyLock<Arc<HuggingFaceStore>> =
    LazyLock::new(|| Arc::new(HuggingFaceStore::default()));

/// The process-wide store.
#[must_use]
pub fn store() -> Arc<HuggingFaceStore> {
    Arc::clone(&STORE)
}

/// A read-only [`ObjectStore`] over Hugging Face dataset repositories.
#[derive(Debug, Default)]
pub struct HuggingFaceStore {
    hubs: RwLock<HashMap<RepoId, Arc<Hub>>>,
}

impl fmt::Display for HuggingFaceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{STORE_NAME}")
    }
}

impl HuggingFaceStore {
    /// Reads `repo` with `hub` from now on.
    ///
    /// Objects are addressed by repository and commit alone, so every dataset reading a
    /// repository reads it with one client: the latest registered, so that a reloaded dataset's
    /// new token takes effect — except that a client with a token is never replaced by one
    /// without, for the same endpoint: a public read of a gated dataset must not take away the
    /// token another dataset reads its files with. Every path names a commit, so any client that
    /// can read the repository at the endpoint reads the same bytes.
    pub fn register(&self, repo: RepoId, hub: Arc<Hub>) {
        let mut hubs = self.hubs.write();
        if let Some(existing) = hubs.get(&repo)
            && existing.is_authenticated()
            && !hub.is_authenticated()
            && existing.endpoint() == hub.endpoint()
        {
            return;
        }
        hubs.insert(repo, hub);
    }

    fn hub(&self, repo: &RepoId, location: &Path) -> object_store::Result<Arc<Hub>> {
        self.hubs
            .read()
            .get(repo)
            .cloned()
            .ok_or_else(|| object_store::Error::NotFound {
                path: location.to_string(),
                source: format!(
                    "Hugging Face dataset '{repo}' has no dataset registered to read it with"
                )
                .into(),
            })
    }
}

/// The repository, commit and file an object path names.
#[derive(Debug, PartialEq, Eq)]
struct Target<'a> {
    repo: RepoId,
    commit: &'a str,
    /// The path inside the repository, empty for its root.
    path: &'a str,
}

/// Splits `{owner}/{name}@{commit}[/{path}]`.
fn target(location: &Path) -> object_store::Result<Target<'_>> {
    let raw = location.as_ref();
    let invalid = || object_store::Error::InvalidPath {
        source: object_store::path::Error::InvalidPath { path: raw.into() },
    };
    let mut parts = raw.splitn(3, '/');
    let owner = parts.next().filter(|owner| !owner.is_empty());
    let name_and_commit = parts.next();
    let (Some(owner), Some(name_and_commit)) = (owner, name_and_commit) else {
        return Err(invalid());
    };
    let (name, commit) = name_and_commit.split_once('@').ok_or_else(invalid)?;
    if !hub::is_commit_sha(commit) {
        return Err(invalid());
    }
    let repo = RepoId::new(owner, name).map_err(|_| invalid())?;
    Ok(Target {
        repo,
        commit,
        path: parts.next().unwrap_or_default(),
    })
}

/// The object path of `entry` in `repo` at `commit`.
fn entry_path(repo: &RepoId, commit: &str, entry: &TreeEntry) -> object_store::Result<Path> {
    // `parse`, not `from`: the path is the literal one the listing URL decodes to, so a listed
    // object lies under the listing prefix whatever characters its name holds.
    Ok(Path::parse(format!(
        "{}/{}@{commit}/{}",
        repo.owner(),
        repo.name(),
        entry.path
    ))?)
}

fn object_meta(
    repo: &RepoId,
    commit: &str,
    last_modified: chrono::DateTime<chrono::Utc>,
    entry: &TreeEntry,
) -> object_store::Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: entry_path(repo, commit, entry)?,
        last_modified,
        size: entry.size,
        e_tag: Some(entry.content_id().to_string()),
        version: Some(commit.to_string()),
    })
}

/// Maps a Hub error to the `object_store` error a reader acts on: a missing path is
/// [`object_store::Error::NotFound`], so a listing falls back from a file to a folder.
fn store_error(location: &Path, error: hub::Error) -> object_store::Error {
    let path = location.to_string();
    match error {
        hub::Error::EntryNotFound { .. }
        | hub::Error::RepoNotFound { .. }
        | hub::Error::RevisionNotFound { .. } => object_store::Error::NotFound {
            path,
            source: Box::new(error),
        },
        hub::Error::Gated { .. } | hub::Error::TokenRejected { status: 401, .. } => {
            object_store::Error::Unauthenticated {
                path,
                source: Box::new(error),
            }
        }
        hub::Error::TokenRejected { .. } => object_store::Error::PermissionDenied {
            path,
            source: Box::new(error),
        },
        error => object_store::Error::Generic {
            store: STORE_NAME,
            source: Box::new(error),
        },
    }
}

fn read_only(operation: &str) -> object_store::Error {
    object_store::Error::NotSupported {
        source: format!("Hugging Face datasets are read-only: {operation} is not supported").into(),
    }
}

#[async_trait]
impl ObjectStore for HuggingFaceStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(read_only("writing"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(read_only("writing"))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let Target { repo, commit, path } = target(location)?;
        let hub = self.hub(&repo, location)?;
        if let Some(version) = &options.version
            && version != commit
        {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: format!(
                    "'{path}' of Hugging Face dataset '{repo}' was requested at version '{version}', but the path names commit '{commit}'"
                )
                .into(),
            });
        }
        let last_modified = hub
            .commit_date(&repo, commit)
            .await
            .map_err(|e| store_error(location, e))?;

        // A head request, or an empty range, needs only the description of the file.
        let empty_range =
            matches!(&options.range, Some(GetRange::Bounded(range)) if range.is_empty());
        if options.head || empty_range {
            let entry = hub
                .entry(&repo, commit, path)
                .await
                .map_err(|e| store_error(location, e))?
                .filter(|entry| entry.kind == EntryKind::File)
                .ok_or_else(|| {
                    store_error(
                        location,
                        hub::Error::EntryNotFound {
                            repo: repo.clone(),
                            path: path.to_string(),
                        },
                    )
                })?;
            let meta = object_meta(&repo, commit, last_modified, &entry)?;
            options.check_preconditions(&meta)?;
            let start = match &options.range {
                Some(GetRange::Bounded(range)) => range.start.min(meta.size),
                _ => 0,
            };
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                meta,
                range: start..start,
                attributes: Attributes::new(),
            });
        }

        let read = hub
            .read(&repo, commit, path, options.range.clone())
            .await
            .map_err(|e| store_error(location, e))?;
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified,
            size: read.size,
            e_tag: read.e_tag,
            version: Some(commit.to_string()),
        };
        options.check_preconditions(&meta)?;
        let stream_location = location.clone();
        let stream: BoxStream<'static, object_store::Result<Bytes>> = read
            .stream
            .map_err(move |e| store_error(&stream_location, e))
            .boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range: read.range,
            attributes: Attributes::new(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        locations.map(|_| Err(read_only("deleting"))).boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let Some(prefix) = prefix.cloned() else {
            return stream::once(async { Err(read_only("listing every dataset")) }).boxed();
        };
        let hub = target(&prefix).and_then(|target| self.hub(&target.repo, &prefix));
        stream::once(async move {
            let hub = hub?;
            let Target { repo, commit, path } = target(&prefix)?;
            let last_modified = hub
                .commit_date(&repo, commit)
                .await
                .map_err(|e| store_error(&prefix, e))?;
            let entries = hub
                .list(&repo, commit, path, true)
                .await
                .map_err(|e| store_error(&prefix, e))?;
            let objects = entries
                .iter()
                .filter(|entry| entry.kind == EntryKind::File)
                .map(|entry| object_meta(&repo, commit, last_modified, entry))
                .collect::<Vec<_>>();
            Ok::<_, object_store::Error>(stream::iter(objects))
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let Some(prefix) = prefix else {
            return Err(read_only("listing every dataset"));
        };
        let Target { repo, commit, path } = target(prefix)?;
        let hub = self.hub(&repo, prefix)?;
        let last_modified = hub
            .commit_date(&repo, commit)
            .await
            .map_err(|e| store_error(prefix, e))?;
        let entries = hub
            .list(&repo, commit, path, false)
            .await
            .map_err(|e| store_error(prefix, e))?;
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
        };
        for entry in entries.iter() {
            match entry.kind {
                EntryKind::File => {
                    result
                        .objects
                        .push(object_meta(&repo, commit, last_modified, entry)?);
                }
                EntryKind::Directory => result
                    .common_prefixes
                    .push(entry_path(&repo, commit, entry)?),
            }
        }
        Ok(result)
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(read_only("copying"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMMIT: &str = "e6281661ce1c48d982bc483cf8a173c1bbeb5d31";

    #[test]
    fn target_splits_repo_commit_and_path() {
        let location = Path::parse(format!(
            "stanfordnlp/imdb@{COMMIT}/plain_text/train.parquet"
        ))
        .expect("a valid path");
        let named = target(&location).expect("the path names a repository and commit");
        assert_eq!(named.repo.to_string(), "stanfordnlp/imdb");
        assert_eq!(named.commit, COMMIT);
        assert_eq!(named.path, "plain_text/train.parquet");

        let root = Path::parse(format!("stanfordnlp/imdb@{COMMIT}")).expect("a valid path");
        assert_eq!(target(&root).expect("the repository root").path, "");
    }

    #[test]
    fn target_rejects_paths_without_a_commit() {
        for raw in [
            "stanfordnlp/imdb/plain_text/train.parquet",
            "stanfordnlp/imdb@main/plain_text/train.parquet",
            "stanfordnlp",
            &format!("stanfordnlp/im--db@{COMMIT}/x.parquet"),
        ] {
            let location = Path::parse(raw).expect("a valid path");
            assert!(
                matches!(
                    target(&location),
                    Err(object_store::Error::InvalidPath { .. })
                ),
                "{raw} must not name a readable object"
            );
        }
    }

    #[test]
    fn listed_objects_keep_their_literal_name() {
        let repo = RepoId::new("o", "d").expect("a valid repository");
        let entry: TreeEntry = serde_json::from_value(serde_json::json!({
            "type": "file",
            "path": "data~v2/part [1].parquet",
            "size": 7,
            "oid": "abc",
        }))
        .expect("a tree entry");
        let meta = object_meta(&repo, COMMIT, chrono::Utc::now(), &entry).expect("an object");
        assert_eq!(
            meta.location.as_ref(),
            format!("o/d@{COMMIT}/data~v2/part [1].parquet")
        );
        // The listing prefix a URL decodes to must contain the object.
        let prefix = Path::from_url_path(format!("/o/d@{COMMIT}/data~v2/")).expect("a prefix");
        assert!(meta.location.prefix_matches(&prefix));
        assert_eq!(meta.e_tag.as_deref(), Some("abc"));
        assert_eq!(meta.version.as_deref(), Some(COMMIT));
    }
}
