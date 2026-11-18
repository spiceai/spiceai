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
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::scheduler::Action as BallistaAction;
use ballista_executor::flight_service::BallistaFlightService;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
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
    work_dir: Option<ShuffleWorkDir>,
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
        Ok(Self { base, dir })
    }

    /// Whether `path`, made absolute and normalized, sits inside the work
    /// directory.
    fn contains(&self, path: &str) -> bool {
        absolute_lexically(&self.base, Path::new(path)).starts_with(&self.dir)
    }
}

impl CompositeFlightService {
    /// Creates a new composite Flight service that confines shuffle fetches to
    /// `work_dir` (the executor's shuffle directory).
    #[must_use]
    pub fn new(spice: SpiceFlightService, work_dir: Option<String>) -> Self {
        Self {
            ballista: BallistaFlightService::new(),
            spice,
            work_dir: work_dir.and_then(|dir| ShuffleWorkDir::new(&dir).ok()),
        }
    }

    /// Refuses a Ballista fetch whose explicit file path escapes this executor's
    /// shuffle work directory. A shuffle fetch names the output file a map task
    /// wrote under the work directory; a path outside it — or a traversal out of
    /// it — would read an unrelated file the executor process can open, so it is
    /// refused. An empty path (resolved by the executor from the fetch
    /// identifiers) and a `memory://` key address no file and pass through.
    fn ensure_fetch_within_work_dir(&self, action: &BallistaAction) -> Result<(), Status> {
        let BallistaAction::FetchPartition { path, .. } = action;
        if path.is_empty() || path.starts_with("memory://") {
            return Ok(());
        }
        let Some(work_dir) = &self.work_dir else {
            tracing::warn!(
                "Refused a cluster shuffle fetch for an explicit file path '{path}': this executor has no shuffle work directory to confine it to."
            );
            return Err(Status::permission_denied(
                "shuffle fetch by explicit path is not served by this executor",
            ));
        };
        if work_dir.contains(path) {
            return Ok(());
        }
        tracing::warn!(
            "Refused a cluster shuffle fetch for '{path}', which is outside this executor's shuffle work directory '{}': a cluster peer may fetch only shuffle output this executor wrote there.",
            work_dir.dir.display()
        );
        Err(Status::permission_denied(
            "shuffle fetch path is outside this executor's work directory",
        ))
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
/// The check is lexical and does not resolve symbolic links: a link under the
/// work directory that points elsewhere passes it. It is a first gate on the
/// request, not a guarantee about the file that is opened; that takes an open
/// that refuses to follow links, which belongs to the fetch service that
/// opens the file.
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
        // directory before serving the file it names.
        if let Ok(action) = decode_protobuf(&request.get_ref().ticket) {
            self.ensure_fetch_within_work_dir(&action)?;
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
            // directory first.
            let action = decode_protobuf(&request.get_ref().body)
                .map_err(|e| Status::invalid_argument(format!("invalid fetch action: {e}")))?;
            self.ensure_fetch_within_work_dir(&action)?;
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
    use super::{ShuffleWorkDir, normalize_lexically};
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
}
