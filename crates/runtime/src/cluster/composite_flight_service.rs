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

//! Composite Flight service that routes requests between Ballista and Spice Flight services.
//!
//! On executor nodes in distributed mode, we need to serve both:
//! - Ballista Flight protocol for shuffle data transfer between executors
//! - Spice Flight protocol for SQL queries and `FlightSQL`
//!
//! This composite service inspects incoming requests and routes them to the appropriate
//! underlying service based on the message format.

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService,
};
use ballista_core::execution_plans::range_shuffle::index_path as range_index_path;
use ballista_core::execution_plans::sort_shuffle::get_index_path;
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::protobuf;
use ballista_core::serde::scheduler::Action as BallistaAction;
use ballista_core::serde::scheduler::{ShuffleFileKind, ShuffleLayout};
use ballista_executor::flight_service::BallistaFlightService;
use prost::Message;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use tonic::{Request, Response, Status, Streaming};

use crate::flight::Service as SpiceFlightService;

type BoxedFlightStream<T> =
    Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

/// A composite Flight service that routes between Ballista and Spice Flight services.
///
/// Request routing logic:
/// - `do_get`: If ticket decodes as Ballista protobuf → Ballista, otherwise → Spice
/// - `do_action`: If action type is "`IO_BLOCK_TRANSPORT`" → Ballista, otherwise → Spice
/// - `list_actions`: Combined from both services
/// - All other methods → Spice (Ballista returns unimplemented)
pub struct CompositeFlightService {
    ballista: BallistaFlightService,
    spice: SpiceFlightService,
    /// This executor's shuffle work directory. A peer fetches shuffle output
    /// by the explicit file path on its `FetchPartition`; that path is
    /// confined to this directory so a cluster peer cannot read other files by
    /// path. `None` when the executor has no work directory, or when the
    /// current directory cannot be read to resolve it; every explicit path is
    /// then refused.
    work_dir: Option<Arc<ShuffleWorkDir>>,
}

/// A shuffle work directory and the directory relative paths resolve against,
/// so a request and the work directory are compared as the absolute paths
/// they open, whether either was given relative or absolute.
struct ShuffleWorkDir {
    /// The process's current directory when the service was created. A
    /// relative path opens against it; `spiced` does not change its current
    /// directory, and reading it once keeps a system call off every fetch.
    base: PathBuf,
    /// The work directory, absolute and lexically normalized.
    dir: PathBuf,
    /// The work directory with its symbolic links resolved, read from the
    /// filesystem the first time a fetch finds it there.
    resolved: OnceLock<PathBuf>,
}

/// Why a fetch was refused after its path was resolved.
#[derive(Debug)]
enum Refusal {
    /// `file`, a file the fetch would open, resolves to `resolved`, which is
    /// outside the work directory.
    Outside { file: PathBuf, resolved: PathBuf },
    /// `file`, a file the fetch would open, could not be resolved.
    Unresolvable {
        file: PathBuf,
        source: std::io::Error,
    },
}

impl ShuffleWorkDir {
    fn new(configured: &str) -> std::io::Result<Self> {
        let base = std::env::current_dir()?;
        // An empty work directory is the current directory: Ballista writes
        // shuffle output relative to it.
        let configured = if configured.is_empty() {
            "."
        } else {
            configured
        };
        let dir = absolute_lexically(&base, Path::new(configured));
        Ok(Self {
            base,
            dir,
            resolved: OnceLock::new(),
        })
    }

    /// Whether `path`, made absolute and normalized, sits inside the work
    /// directory.
    fn contains(&self, path: &str) -> bool {
        absolute_lexically(&self.base, Path::new(path)).starts_with(&self.dir)
    }

    /// The work directory with its symbolic links resolved, or `None` while
    /// it does not exist.
    fn resolved(&self) -> Option<&Path> {
        if let Some(dir) = self.resolved.get() {
            return Some(dir);
        }
        let dir = std::fs::canonicalize(&self.dir).ok()?;
        Some(self.resolved.get_or_init(|| dir))
    }

    /// Checks every file Ballista opens to serve a fetch for `path` against
    /// the work directory, with symbolic links resolved on both sides, and
    /// returns the path to forward in place of `path`: the requested file
    /// with its links resolved, or `None` when it does not exist, so the fetch
    /// reaches Ballista unchanged and fails as it always has (a missing file is
    /// an empty partition that was never written).
    ///
    /// Ballista opens the file the fetch names, which is the data file or, for
    /// an index request, the index its `layout` puts beside it; and it reads the
    /// sort shuffle's index beside that file whenever one exists. Each of them
    /// that exists must resolve inside the work directory.
    ///
    /// Ballista opens files by path, so a path component replaced with a
    /// symbolic link after this check and before Ballista opens the file is
    /// still followed. Closing that window takes an open that refuses to
    /// follow links at each component, which belongs to the fetch service that
    /// opens the file.
    ///
    /// This blocks on the filesystem.
    fn resolve_fetch(
        &self,
        path: &str,
        layout: ShuffleLayout,
        file_kind: ShuffleFileKind,
    ) -> Result<Option<PathBuf>, Refusal> {
        let requested = self.base.join(path);
        let forwarded = match resolve_existing(&requested)? {
            Some(resolved) => {
                self.ensure_inside(&requested, &resolved)?;
                Some(resolved)
            }
            None => None,
        };
        let data = forwarded.as_deref().unwrap_or(requested.as_path());
        let named = match file_kind {
            ShuffleFileKind::Data => data.to_path_buf(),
            ShuffleFileKind::Index => match layout {
                ShuffleLayout::Sort => get_index_path(data),
                ShuffleLayout::Passthrough => range_index_path(data),
            },
        };
        let sort_index = get_index_path(&named);
        for file in [named, sort_index] {
            // `data` itself was resolved above.
            if file == data {
                continue;
            }
            // Beside a resolved data file every directory on the way is real,
            // so a file there that is not itself a link is its own resolved
            // path, and one `lstat` stands in for resolving it.
            if forwarded.is_some() {
                match std::fs::symlink_metadata(&file) {
                    Ok(meta) if !meta.file_type().is_symlink() => {
                        self.ensure_inside(&file, &file)?;
                        continue;
                    }
                    Err(e) if is_missing(&e) => continue,
                    _ => {}
                }
            }
            if let Some(resolved) = resolve_existing(&file)? {
                self.ensure_inside(&file, &resolved)?;
            }
        }
        Ok(forwarded)
    }

    /// Refuses `file` unless `resolved`, its path with links resolved, lies
    /// inside the work directory with its links resolved.
    fn ensure_inside(&self, file: &Path, resolved: &Path) -> Result<(), Refusal> {
        match self.resolved() {
            Some(dir) if resolved.starts_with(dir) => Ok(()),
            _ => Err(Refusal::Outside {
                file: file.to_path_buf(),
                resolved: resolved.to_path_buf(),
            }),
        }
    }
}

/// `path` with every symbolic link resolved, or `None` when it does not exist
/// (opening it then fails without reading anything).
fn resolve_existing(path: &Path) -> Result<Option<PathBuf>, Refusal> {
    match std::fs::canonicalize(path) {
        Ok(resolved) => Ok(Some(resolved)),
        Err(e) if is_missing(&e) => Ok(None),
        Err(source) => Err(Refusal::Unresolvable {
            file: path.to_path_buf(),
            source,
        }),
    }
}

/// Whether `e` says the path names nothing: no such file, or a component on the
/// way that is not a directory.
fn is_missing(e: &std::io::Error) -> bool {
    matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory)
}

/// `encoded`, a Ballista fetch action, with the file path it names replaced by
/// `path`.
fn with_fetch_path(encoded: &[u8], path: String) -> Result<bytes::Bytes, Status> {
    let mut action = protobuf::Action::decode(encoded)
        .map_err(|e| Status::invalid_argument(format!("invalid fetch action: {e}")))?;
    let Some(protobuf::action::ActionType::FetchPartition(fetch)) = action.action_type.as_mut()
    else {
        return Err(Status::invalid_argument(
            "invalid fetch action: it names no partition",
        ));
    };
    fetch.path = path;
    Ok(action.encode_to_vec().into())
}

impl CompositeFlightService {
    /// Creates a new composite Flight service that confines shuffle fetches to
    /// `work_dir` (the executor's shuffle directory).
    #[must_use]
    pub fn new(spice: SpiceFlightService, work_dir: Option<String>) -> Self {
        Self {
            ballista: BallistaFlightService::new(),
            spice,
            work_dir: work_dir.and_then(|dir| ShuffleWorkDir::new(&dir).ok().map(Arc::new)),
        }
    }

    /// Refuses a Ballista fetch whose explicit file path escapes this executor's
    /// shuffle work directory, and returns the path to forward to Ballista in
    /// its place. A shuffle fetch names the output file a map task wrote under
    /// the work directory; a path outside it — a traversal out of it, or a
    /// symbolic link under it that leads out — would read an unrelated file the
    /// executor process can open, so it is refused. An empty path (resolved by
    /// the executor from the fetch identifiers) and a `memory://` key address
    /// no file and pass through unchanged, as does a file that does not exist.
    async fn confine_fetch(&self, action: &BallistaAction) -> Result<Option<String>, Status> {
        let BallistaAction::FetchPartition {
            path,
            layout,
            file_kind,
            ..
        } = action;
        if path.is_empty() || path.starts_with("memory://") {
            return Ok(None);
        }
        let Some(work_dir) = &self.work_dir else {
            tracing::warn!(
                "Refused a cluster shuffle fetch for an explicit file path '{path}': this executor has no shuffle work directory to confine it to."
            );
            return Err(Status::permission_denied(
                "shuffle fetch by explicit path is not served by this executor",
            ));
        };
        if !work_dir.contains(path) {
            tracing::warn!(
                "Refused a cluster shuffle fetch for '{path}', which is outside this executor's shuffle work directory '{}': a cluster peer may fetch only shuffle output this executor wrote there.",
                work_dir.dir.display()
            );
            return Err(Status::permission_denied(
                "shuffle fetch path is outside this executor's work directory",
            ));
        }
        // Resolving links reads the filesystem, so it runs off the async
        // threads, in a single blocking task that checks every file the fetch
        // opens.
        let resolution = {
            let work_dir = Arc::clone(work_dir);
            let path = path.clone();
            let (layout, file_kind) = (*layout, *file_kind);
            tokio::task::spawn_blocking(move || work_dir.resolve_fetch(&path, layout, file_kind))
                .await
        };
        match resolution {
            Ok(Ok(None)) => Ok(None),
            Ok(Ok(Some(resolved))) => match resolved.into_os_string().into_string() {
                Ok(resolved) => Ok(Some(resolved)),
                Err(resolved) => {
                    tracing::warn!(
                        "Refused a cluster shuffle fetch for '{path}': it resolves to '{}', a path that is not valid UTF-8, which this executor cannot serve.",
                        resolved.display()
                    );
                    Err(Status::permission_denied(
                        "shuffle fetch path cannot be served by this executor",
                    ))
                }
            },
            Ok(Err(Refusal::Outside { file, resolved })) => {
                tracing::warn!(
                    "Refused a cluster shuffle fetch for '{path}': '{}' resolves to '{}', which is outside this executor's shuffle work directory '{}': a cluster peer may fetch only shuffle output this executor wrote there.",
                    file.display(),
                    resolved.display(),
                    work_dir.dir.display()
                );
                Err(Status::permission_denied(
                    "shuffle fetch path is outside this executor's work directory",
                ))
            }
            Ok(Err(Refusal::Unresolvable { file, source })) => {
                tracing::warn!(
                    "Refused a cluster shuffle fetch for '{path}': '{}' could not be resolved to check that it is inside this executor's shuffle work directory '{}'. Cause: {source}",
                    file.display(),
                    work_dir.dir.display()
                );
                Err(Status::permission_denied(
                    "shuffle fetch path cannot be resolved inside this executor's work directory",
                ))
            }
            Err(e) => Err(Status::internal(format!(
                "shuffle fetch path could not be checked: {e}"
            ))),
        }
    }
}

/// Resolves `.` and `..` components textually, without touching the
/// filesystem, so the containment check does not depend on a file existing (an
/// empty shuffle partition is never written).
///
/// A `..` removes the named directory before it. One with nothing to remove
/// is kept when the path is relative, because the directory it climbs out of
/// is not known here, and is dropped at the root of an absolute path, which
/// the operating system resolves `/..` to.
///
/// This is the first gate on a request, and it does not resolve symbolic
/// links: [`ShuffleWorkDir::resolve_fetch`] resolves them next, for every file
/// the fetch opens, and the fetch is forwarded with the requested file's
/// resolved path.
fn normalize_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match out.components().next_back() {
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                Some(Component::RootDir) => {}
                Some(Component::ParentDir | Component::CurDir | Component::Prefix(_)) | None => {
                    out.push(Component::ParentDir.as_os_str());
                }
            },
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// `path` resolved against `base` when it is relative, as opening it from
/// that current directory resolves it, then normalized with
/// [`normalize_lexically`].
fn absolute_lexically(base: &Path, path: &Path) -> PathBuf {
    normalize_lexically(&base.join(path))
}

#[tonic::async_trait]
impl FlightService for CompositeFlightService {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;

    // These methods are unimplemented by `BallistaFlightService`.
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        self.spice.handshake(request).await
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.spice.list_flights(request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.spice.get_flight_info(request).await
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        self.spice.poll_flight_info(request).await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.spice.get_schema(request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.spice.do_put(request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.spice.do_exchange(request).await
    }

    // These methods are implemented by both `BallistaFlightService` and `SpiceFlightService`.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // A Ballista-format ticket is a shuffle fetch: confine it to the work
        // directory before serving the file it names, by its resolved path.
        if let Ok(action) = decode_protobuf(&request.get_ref().ticket) {
            let request = match self.confine_fetch(&action).await? {
                Some(path) => {
                    let (metadata, extensions, ticket) = request.into_parts();
                    let ticket = Ticket {
                        ticket: with_fetch_path(&ticket.ticket, path)?,
                    };
                    Request::from_parts(metadata, extensions, ticket)
                }
                None => request,
            };
            return self.ballista.do_get(request).await;
        }
        // Otherwise route to Spice for SQL/FlightSQL
        self.spice.do_get(request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        // Route based. BallistaFlightService only handles IO_BLOCK_TRANSPORT actions.
        if request.get_ref().r#type == "IO_BLOCK_TRANSPORT" {
            // The action body names a file to serve; confine it to the work
            // directory first, and serve it by its resolved path.
            let action = decode_protobuf(&request.get_ref().body)
                .map_err(|e| Status::invalid_argument(format!("invalid fetch action: {e}")))?;
            let request = match self.confine_fetch(&action).await? {
                Some(path) => {
                    let (metadata, extensions, action) = request.into_parts();
                    let action = Action {
                        r#type: action.r#type,
                        body: with_fetch_path(&action.body, path)?,
                    };
                    Request::from_parts(metadata, extensions, action)
                }
                None => request,
            };
            return self.ballista.do_action(request).await;
        }
        // All other actions go to Spice
        self.spice.do_action(request).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let spice = self.spice.list_actions(Request::new(Empty {})).await?;
        let ballista = self.ballista.list_actions(request).await?;

        Ok(Response::new(Box::pin(futures::stream::select(
            spice.into_inner(),
            ballista.into_inner(),
        )) as Self::ListActionsStream))
    }
}

#[cfg(test)]
mod tests {
    use super::{Refusal, ShuffleFileKind, ShuffleLayout, ShuffleWorkDir, normalize_lexically};
    use std::path::Path;

    #[test]
    fn a_shuffle_fetch_is_confined_to_the_work_directory() {
        let work_dir = ShuffleWorkDir::new("/var/spice/shuffle").expect("current dir");

        // Legitimate shuffle output under the work directory.
        for inside in [
            "/var/spice/shuffle/job-1/0/data-0.arrow",
            "/var/spice/shuffle/job-1/0/data-0.arrow.index",
            "/var/spice/shuffle/./job-2/data.arrow",
        ] {
            assert!(work_dir.contains(inside), "{inside}");
        }

        // Absolute paths elsewhere, and traversals out of the directory.
        for outside in [
            "/etc/hostname",
            "/var/spice/shuffle/../secrets/key.pem",
            "/var/spice/shuffle/../../etc/passwd",
            "/var/spice/shuffle-sibling/data.arrow",
            "relative/data.arrow",
        ] {
            assert!(!work_dir.contains(outside), "{outside}");
        }
    }

    #[test]
    fn a_relative_work_directory_confines_requests_where_they_open() {
        let cwd = std::env::current_dir().expect("current dir");
        let work_dir = ShuffleWorkDir::new("shuffle").expect("current dir");
        assert_eq!(work_dir.dir, cwd.join("shuffle"));

        // Shuffle output under the work directory, named relative to the
        // current directory as Ballista writes it, or absolute.
        let absolute_inside = cwd.join("shuffle/job-1/0/data-0.arrow");
        for inside in [
            "shuffle/job-1/0/data-0.arrow",
            "./shuffle/job-1/../job-1/0/data-0.arrow",
            absolute_inside.to_str().expect("utf-8 path"),
        ] {
            assert!(work_dir.contains(inside), "{inside}");
        }

        // Parent components that climb out of the current directory, however
        // they are arranged, and paths beside the work directory.
        for outside in [
            "../../shuffle/data.arrow",
            "../shuffle/data.arrow",
            "shuffle/../../shuffle/data.arrow",
            "shuffle/../../../shuffle/data.arrow",
            "shuffle/../shuffle-sibling/data.arrow",
            "data.arrow",
            "/etc/hostname",
        ] {
            assert!(!work_dir.contains(outside), "{outside}");
        }
    }

    #[test]
    fn normalization_resolves_dot_and_parent_without_escaping() {
        assert_eq!(
            normalize_lexically(Path::new("/a/b/./c/../d")),
            Path::new("/a/b/d")
        );
        // At the root of an absolute path `..` stays at the root, as the
        // operating system resolves it.
        assert_eq!(
            normalize_lexically(Path::new("/a/../../b")),
            Path::new("/b")
        );
        assert_eq!(normalize_lexically(Path::new("/../..")), Path::new("/"));
        // In a relative path a `..` with nothing before it to remove is kept,
        // and a second one does not cancel the first.
        assert_eq!(
            normalize_lexically(Path::new("../../shuffle/data.arrow")),
            Path::new("../../shuffle/data.arrow")
        );
        assert_eq!(
            normalize_lexically(Path::new("shuffle/../../x/../../y")),
            Path::new("../../y")
        );
        assert_eq!(normalize_lexically(Path::new("a/./..")), Path::new(""));
    }

    /// The outcome of resolving a fetch for `path`, with paths shown relative
    /// to `root` so the expectations read the same wherever the test runs.
    fn resolution(
        work_dir: &ShuffleWorkDir,
        root: &Path,
        path: &Path,
        layout: ShuffleLayout,
        file_kind: ShuffleFileKind,
    ) -> String {
        let shown = |p: &Path| {
            p.strip_prefix(root)
                .map_or_else(|_| p.display().to_string(), |p| p.display().to_string())
        };
        let path = path.to_str().expect("utf-8 path");
        match work_dir.resolve_fetch(path, layout, file_kind) {
            Ok(Some(resolved)) => format!("forward {}", shown(&resolved)),
            Ok(None) => "forward unchanged".to_string(),
            Err(Refusal::Outside { file, resolved }) => {
                format!("refuse {} -> {}", shown(&file), shown(&resolved))
            }
            Err(Refusal::Unresolvable { file, source }) => {
                format!("unresolvable {}: {:?}", shown(&file), source.kind())
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_fetch_is_judged_by_the_files_its_links_lead_to() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().canonicalize().expect("canonical temp dir");
        // The work directory is configured through a link, as an operator's
        // mount or the platform's temporary directory may be: its files must
        // still count as inside it.
        let real = root.join("disk").join("shuffle");
        let job = real.join("job-1").join("0");
        std::fs::create_dir_all(&job).expect("create work dir");
        symlink(&real, root.join("shuffle")).expect("link work dir");
        let outside = root.join("secrets");
        std::fs::create_dir_all(&outside).expect("create outside dir");
        std::fs::write(outside.join("key.pem"), b"secret").expect("write outside file");

        std::fs::write(job.join("data.arrow"), b"data").expect("write data");
        std::fs::write(job.join("indexed.arrow"), b"data").expect("write data");
        std::fs::write(job.join("indexed.arrow.index"), b"index").expect("write index");
        // An index named after the work directory itself sits beside it.
        std::fs::write(root.join("disk").join("shuffle.arrow.index"), b"index")
            .expect("write index");
        std::fs::write(job.join("sorted.arrow"), b"data").expect("write data");
        symlink(outside.join("key.pem"), job.join("sorted.arrow.index")).expect("link");
        symlink(outside.join("key.pem"), job.join("link.arrow")).expect("link");
        symlink(&outside, real.join("escape")).expect("link");
        symlink(outside.join("key.pem"), job.join("ranged.rangeidx.arrow")).expect("link");
        symlink(job.join("data.arrow"), job.join("alias.arrow")).expect("link");
        symlink(outside.join("missing.pem"), job.join("dangling.arrow")).expect("link");
        symlink(job.join("loop-b.arrow"), job.join("loop-a.arrow")).expect("link");
        symlink(job.join("loop-a.arrow"), job.join("loop-b.arrow")).expect("link");

        let work_dir =
            ShuffleWorkDir::new(root.join("shuffle").to_str().expect("utf-8 path")).expect("cwd");
        let configured = root.join("shuffle").join("job-1").join("0");
        let (data, sort, index) = (
            ShuffleFileKind::Data,
            ShuffleLayout::Sort,
            ShuffleFileKind::Index,
        );
        let passthrough = ShuffleLayout::Passthrough;
        let outcomes: Vec<String> = [
            (configured.join("data.arrow"), passthrough, data),
            (configured.join("alias.arrow"), passthrough, data),
            (configured.join("indexed.arrow"), sort, data),
            (configured.join("indexed.arrow"), sort, index),
            (root.join("shuffle"), sort, index),
            (configured.join("never-written.arrow"), passthrough, data),
            (configured.join("dangling.arrow"), passthrough, data),
            (configured.join("link.arrow"), passthrough, data),
            (root.join("shuffle/escape/key.pem"), passthrough, data),
            (configured.join("sorted.arrow"), sort, data),
            (configured.join("sorted.arrow"), sort, index),
            (configured.join("ranged.arrow"), passthrough, index),
            (configured.join("loop-a.arrow"), passthrough, data),
        ]
        .into_iter()
        .map(|(path, layout, kind)| resolution(&work_dir, &root, &path, layout, kind))
        .collect();
        assert_eq!(
            outcomes,
            [
                "forward disk/shuffle/job-1/0/data.arrow",
                "forward disk/shuffle/job-1/0/data.arrow",
                "forward disk/shuffle/job-1/0/indexed.arrow",
                "forward disk/shuffle/job-1/0/indexed.arrow",
                "refuse disk/shuffle.arrow.index -> disk/shuffle.arrow.index",
                "forward unchanged",
                "forward unchanged",
                "refuse shuffle/job-1/0/link.arrow -> secrets/key.pem",
                "refuse shuffle/escape/key.pem -> secrets/key.pem",
                "refuse disk/shuffle/job-1/0/sorted.arrow.index -> secrets/key.pem",
                "refuse disk/shuffle/job-1/0/sorted.arrow.index -> secrets/key.pem",
                "refuse shuffle/job-1/0/ranged.rangeidx.arrow -> secrets/key.pem",
                "unresolvable shuffle/job-1/0/loop-a.arrow: FilesystemLoop",
            ]
        );
    }
}
