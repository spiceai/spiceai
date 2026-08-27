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

//! Whether a Cayenne data directory and metastore directory can collide on disk.
//!
//! One metastore holds the catalog — manifests, snapshot pointers, partition rows —
//! for *every* Cayenne table sharing a metadata directory, so a metastore that
//! resolves onto or beneath a data directory is a catalog that a recursive delete of
//! that data directory would take with it.
//!
//! Both configuration surfaces that pair these two directories ask the question here,
//! and they act on the answer differently:
//!
//! - The dataset-level accelerator refuses the recursive delete that a schema recreate
//!   performs, so for it the answer gates a destructive call.
//! - The catalog connector has no such call today, so for it the answer refuses the
//!   configuration at startup — where the operator can still edit the spicepod, rather
//!   than on the first teardown a later change introduces.
//!
//! Only the by-name question lives here: given the two configured strings, can the
//! directory they name overlap. Finding a metastore that no parameter names is a
//! property of what is on disk immediately before a delete, so it belongs beside that
//! delete rather than in this shared answer.

use std::path::{Component, Path, PathBuf};

/// Returns true if the path is a local filesystem path (not a remote object store).
///
/// Local paths include:
/// - Absolute paths: `/data/cayenne`
/// - Relative paths: `./data`
/// - file:// URIs: `file:///data/cayenne`
///
/// Remote paths (S3, etc.) return false.
#[must_use]
pub fn is_local_path(path: &str) -> bool {
    !path.contains("://") || path.starts_with("file://")
}

/// Strip a `file:`/`file://` scheme (including an optional authority such as
/// `localhost`) so on-disk storage detection receives a real filesystem path.
///
/// A configured Cayenne directory may be spelled as such a URI, and feeding
/// `file:///x` or `file://localhost/x` into `Path::new` would make `Auto` storage
/// detection misclassify it as `Unknown`. Returns a borrowed slice (no owned path), so
/// callers can pass the result directly as `&str`.
#[must_use]
pub fn fs_probe_path(path: &str) -> &str {
    if let Some(rest) = path.strip_prefix("file://") {
        // `rest` is either `/abs/path` (empty authority, e.g. `file:///x`) or
        // `authority/abs/path` (e.g. `localhost/abs/path`); the filesystem path
        // begins at the first '/'.
        match rest.find('/') {
            Some(slash) => &rest[slash..],
            None => rest,
        }
    } else {
        path.strip_prefix("file:").unwrap_or(path)
    }
}

/// Make a configured Cayenne directory absolute without resolving it, treating it as a
/// filesystem path unconditionally.
///
/// `Err` when the path cannot be placed — a relative path whose `current_dir()` lookup
/// fails. A path this cannot place is a path whose overlap with the metastore is
/// unknown, and every caller must refuse rather than assume.
fn absolute_dir(path: &str) -> std::io::Result<PathBuf> {
    let raw = Path::new(fs_probe_path(path));
    if raw.is_absolute() {
        Ok(raw.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(raw))
    }
}

/// Make a configured Cayenne *data* directory absolute, or `Ok(None)` when it is an
/// object-store location (`s3://…`) — which can never contain the metastore, since
/// `SQLite`/Turso cannot run on object storage.
///
/// The exemption belongs to the data path alone, because it is the data path a
/// recursive delete walks. It must not be applied to a metadata path: [`is_local_path`]
/// is a substring test, so a value merely *containing* `://` would be exempted while the
/// catalog code goes on treating it as the filesystem path it creates `cayenne.db` at —
/// disabling the check on a directory that never reached an object store.
///
/// `Err`, never the exemption, when the path cannot be placed: the exemption waves the
/// configuration through, so "cannot possibly overlap" and "cannot tell" must stay
/// distinguishable.
///
/// # Errors
///
/// Returns an error when a relative path cannot be placed against the working directory.
pub fn absolute_data_dir(path: &str) -> std::io::Result<Option<PathBuf>> {
    if !is_local_path(path) {
        return Ok(None);
    }
    absolute_dir(path).map(Some)
}

/// Resolve `absolute` component by component, in the order the filesystem would.
///
/// The order is the whole point: `..` names the parent of the directory the preceding
/// component *resolves to*, not its lexical parent. Collapsing `..` up front and
/// canonicalizing afterwards gets this backwards — with `link -> /data/subdir`,
/// `link/../catalog` is `/data/catalog`, but a lexical collapse yields `/catalog` and a
/// containment check against `/data` then passes something it must refuse. Resolving in
/// order keeps the accumulated path symlink-free, so `..` may simply pop it.
///
/// A component that does not exist yet resolves to itself — neither directory
/// necessarily exists when this runs at open time. That is the *only* `canonicalize`
/// failure this absorbs. Any other one (`PermissionDenied`, a transient filesystem
/// error) means the component could not be resolved, so a symlink may still be
/// unresolved and the containment check would run against a path the delete never walks;
/// those propagate, so the caller refuses instead of comparing a lexical path.
async fn resolve_in_filesystem_order(absolute: &Path) -> std::io::Result<PathBuf> {
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Prefix(_) | Component::RootDir => resolved.push(component),
            Component::Normal(name) => {
                resolved.push(name);
                match tokio::fs::canonicalize(&resolved).await {
                    Ok(real) => resolved = real,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(resolved)
}

/// Every location a recursive delete of `path` could reach, or `Err` when the path
/// cannot be resolved.
///
/// There is no object-store exemption here: this resolves a *metastore* directory, and
/// the metastore is only ever local — see [`absolute_data_dir`] for why applying the
/// exemption to this side disables the check rather than skipping an impossible case.
///
/// Two forms, because a symlink is both a place and a name:
///
/// 1. **Fully resolved** — where the directory's contents actually live.
/// 2. **The entry**: parent resolved, final component left literal. `remove_dir_all`
///    unlinks the *entry* it walks onto rather than following it, so a metastore
///    directory whose own last component is a symlink pointing out of the tree still
///    loses its link — the catalog file survives with nothing naming it, and the
///    connection pool keeps writing through handles nothing can reopen.
async fn overlap_candidates(path: &str) -> std::io::Result<Vec<PathBuf>> {
    let absolute = absolute_dir(path)?;

    let mut candidates = vec![resolve_in_filesystem_order(&absolute).await?];
    if let (Some(parent), Some(name)) = (absolute.parent(), absolute.file_name()) {
        let entry = resolve_in_filesystem_order(parent).await?.join(name);
        if !candidates.contains(&entry) {
            candidates.push(entry);
        }
    }
    Ok(candidates)
}

/// `true` when `inner` is `outer` itself or lies beneath it — i.e. a recursive delete
/// of `outer` takes `inner` with it. Compares whole components, so `…/meta` does not
/// read as containing `…/metadata`.
fn dir_contains(outer: &Path, inner: &Path) -> bool {
    inner.starts_with(outer)
}

/// Detect the configuration in which deleting a Cayenne data directory destroys the
/// metastore.
///
/// One metastore holds the catalog — manifests, snapshot pointers, partition rows —
/// for *every* Cayenne table sharing a metadata directory. When the metastore directory
/// resolves onto or beneath the data directory, a recursive delete of that data
/// directory unlinks the shared catalog, and because the connection pool already holds
/// handles to the now-unlinked file the run appears healthy while the metastore is
/// simply gone on the next restart.
///
/// The stock dataset-level defaults collide on their own for a dataset named `metadata`:
/// the default data path yields `{spice_data}/metadata/` and the default metadata
/// directory yields `{spice_data}/metadata`. An explicit metadata directory set beneath
/// the data directory collides the same way.
///
/// Returns `Ok(Some((data_dir, metadata_dir)))` — resolved — when they overlap, naming
/// whichever metastore location the delete would reach; `Ok(None)` when they provably
/// cannot overlap — the data path is on object storage; and `Err` when either path
/// cannot be resolved, which the caller must treat as a refusal rather than as `Ok(None)`.
///
/// The data directory is compared in its fully resolved form only, because that is where
/// the recursive walk happens: `remove_dir_all` unlinks a final-component symlink rather
/// than descending it, so nothing beneath the target is deleted. What the unlink does
/// cost is every *name* under the alias, and a metastore configured through one is not
/// compared here — #13465.
///
/// # Errors
///
/// Returns an error when either directory cannot be resolved. A caller must refuse on
/// that error: a path whose location is unknown is a path whose overlap is unknown.
pub async fn overlapping_metastore_dir(
    data_dir: &str,
    metadata_dir: &str,
) -> std::io::Result<Option<(PathBuf, PathBuf)>> {
    let Some(absolute_data) = absolute_data_dir(data_dir)? else {
        return Ok(None);
    };
    let data = resolve_in_filesystem_order(&absolute_data).await?;
    Ok(overlap_candidates(metadata_dir)
        .await?
        .into_iter()
        .find(|candidate| dir_contains(&data, candidate))
        .map(|metadata| (data, metadata)))
}

#[cfg(test)]
mod tests {
    use super::{absolute_data_dir, fs_probe_path, is_local_path, overlapping_metastore_dir};

    /// The shape both configuration surfaces exist to refuse: the metastore sits
    /// beneath the directory a recursive delete would walk.
    #[tokio::test]
    async fn a_metastore_beneath_the_data_dir_overlaps() {
        let base = tempfile::tempdir().expect("temp dir");
        let data = base.path().join("orders");
        let nested = data.join("metadata");

        assert!(
            overlapping_metastore_dir(&data.to_string_lossy(), &nested.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_some(),
            "a metastore beneath the data directory is deleted with it"
        );
    }

    /// The two directories being the same directory is the same loss, and is the shape
    /// the stock defaults reach on their own for a table named `metadata`.
    #[tokio::test]
    async fn a_metastore_at_the_data_dir_itself_overlaps() {
        let base = tempfile::tempdir().expect("temp dir");
        let shared = base.path().join("metadata");

        assert!(
            overlapping_metastore_dir(&shared.to_string_lossy(), &shared.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_some(),
            "one directory serving as both is deleted with itself"
        );
    }

    /// Containment compares whole components, so a shared string prefix is not
    /// containment — refusing these would reject a configuration that is perfectly safe.
    #[tokio::test]
    async fn a_sibling_sharing_a_name_prefix_does_not_overlap() {
        let base = tempfile::tempdir().expect("temp dir");
        let data = base.path().join("meta");
        let metastore = base.path().join("metadata");

        assert!(
            overlapping_metastore_dir(&data.to_string_lossy(), &metastore.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_none(),
            "`meta` and `metadata` are siblings, not nested"
        );
    }

    /// `..` names the parent of what the preceding component *resolves to*. Collapsing
    /// it lexically before resolving would answer `/`-relative nonsense here and let a
    /// genuine overlap through.
    #[tokio::test]
    async fn a_parent_traversal_resolves_in_filesystem_order() {
        let base = tempfile::tempdir().expect("temp dir");
        let data = base.path().join("orders");
        std::fs::create_dir_all(data.join("inner")).expect("create the data tree");

        let traversed = data.join("inner").join("..").join("metadata");
        assert!(
            overlapping_metastore_dir(&data.to_string_lossy(), &traversed.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_some(),
            "`orders/inner/../metadata` is `orders/metadata`, which the delete reaches"
        );
    }

    /// An object-store data directory can never hold a metastore, because the metastore
    /// is a local database file. That exemption is the data path's alone.
    #[tokio::test]
    async fn an_object_store_data_dir_cannot_overlap() {
        assert!(
            overlapping_metastore_dir("s3://bucket/orders/", "/var/spice/metadata")
                .await
                .expect("an object store is not a failure")
                .is_none(),
            "a metastore cannot live inside an object-store prefix"
        );
    }

    /// The exemption must not reach the *metadata* side: [`is_local_path`] is a
    /// substring test, so applying it there would wave through a directory the catalog
    /// still creates `cayenne.db` in.
    #[tokio::test]
    async fn a_metadata_dir_is_never_exempted_as_an_object_store() {
        let base = tempfile::tempdir().expect("temp dir");
        // A perfectly ordinary local directory whose name merely contains `://`.
        let data = base.path().join("orders");
        let nested = data.join("s3://metadata");

        assert!(
            overlapping_metastore_dir(&data.to_string_lossy(), &nested.to_string_lossy())
                .await
                .expect("the test paths resolve")
                .is_some(),
            "a local metadata directory is compared, whatever its name contains"
        );
    }

    #[test]
    fn absolute_data_dir_places_every_local_spelling_and_exempts_object_stores() {
        assert!(
            absolute_data_dir("relative/orders")
                .expect("a relative path resolves")
                .is_some_and(|path| path.is_absolute()),
            "a relative path is placed against the working directory"
        );
        assert_eq!(
            absolute_data_dir("/var/spice/orders").expect("an absolute path resolves"),
            Some(std::path::PathBuf::from("/var/spice/orders")),
        );
        assert_eq!(
            absolute_data_dir("file:///var/spice/orders").expect("a `file://` URI resolves"),
            Some(std::path::PathBuf::from("/var/spice/orders")),
        );
        assert_eq!(
            absolute_data_dir("s3://bucket/orders/").expect("an object store is not a failure"),
            None,
        );
    }

    #[test]
    fn fs_probe_path_strips_every_file_scheme_spelling() {
        assert_eq!(fs_probe_path("file:///data/cayenne"), "/data/cayenne");
        assert_eq!(fs_probe_path("file:/data/cayenne"), "/data/cayenne");
        assert_eq!(fs_probe_path("file://localhost/data"), "/data");
        assert_eq!(fs_probe_path("/data/cayenne"), "/data/cayenne");
        assert_eq!(fs_probe_path("relative/metadata"), "relative/metadata");
    }

    #[test]
    fn is_local_path_admits_local_spellings_and_refuses_object_stores() {
        assert!(is_local_path("/data/cayenne"));
        assert!(is_local_path("./data"));
        assert!(is_local_path("file:///data/cayenne"));
        assert!(!is_local_path("s3://bucket/prefix"));
        assert!(!is_local_path("gs://bucket/prefix"));
    }
}
