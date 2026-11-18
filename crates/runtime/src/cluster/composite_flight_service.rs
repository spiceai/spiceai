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
    /// This executor's shuffle work directory. A peer fetches shuffle output by
    /// the explicit file path on its `FetchPartition`; that path is confined to
    /// this directory so a cluster peer cannot read other files by absolute path.
    /// `None` only when the executor has no work directory, where every explicit
    /// path is refused.
    work_dir: Option<PathBuf>,
}

impl CompositeFlightService {
    /// Creates a new composite Flight service that confines shuffle fetches to
    /// `work_dir` (the executor's shuffle directory).
    #[must_use]
    pub fn new(spice: SpiceFlightService, work_dir: Option<String>) -> Self {
        Self {
            ballista: BallistaFlightService::new(),
            spice,
            work_dir: work_dir
                .map(PathBuf::from)
                .map(|dir| normalize_lexically(&dir)),
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
        let Some(work_dir) = self.work_dir.as_deref() else {
            tracing::warn!(
                "Refused a cluster shuffle fetch for an explicit file path '{path}': this executor has no shuffle work directory to confine it to."
            );
            return Err(Status::permission_denied(
                "shuffle fetch by explicit path is not served by this executor",
            ));
        };
        if path_within(path, work_dir) {
            return Ok(());
        }
        tracing::warn!(
            "Refused a cluster shuffle fetch for '{path}', which is outside this executor's shuffle work directory '{}': a cluster peer may fetch only shuffle output this executor wrote there.",
            work_dir.display()
        );
        Err(Status::permission_denied(
            "shuffle fetch path is outside this executor's work directory",
        ))
    }
}

/// Resolves `.` and `..` components textually, without touching the filesystem,
/// so the containment check does not depend on a file existing (an empty shuffle
/// partition is never written) and cannot be raced through a symlink created
/// between the check and the open.
fn normalize_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Whether `path`, once `.`/`..` are resolved textually, sits inside the
/// already-normalized `work_dir`.
fn path_within(path: &str, work_dir: &Path) -> bool {
    normalize_lexically(Path::new(path)).starts_with(work_dir)
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
    use super::{normalize_lexically, path_within};
    use std::path::Path;

    #[test]
    fn a_shuffle_fetch_is_confined_to_the_work_directory() {
        let work_dir = normalize_lexically(Path::new("/var/spice/shuffle"));

        // Legitimate shuffle output under the work directory.
        for inside in [
            "/var/spice/shuffle/job-1/0/data-0.arrow",
            "/var/spice/shuffle/job-1/0/data-0.arrow.index",
            "/var/spice/shuffle/./job-2/data.arrow",
        ] {
            assert!(path_within(inside, &work_dir), "{inside}");
        }

        // Absolute paths elsewhere, and traversals out of the directory.
        for outside in [
            "/etc/hostname",
            "/var/spice/shuffle/../secrets/key.pem",
            "/var/spice/shuffle/../../etc/passwd",
            "/var/spice/shuffle-sibling/data.arrow",
            "relative/data.arrow",
        ] {
            assert!(!path_within(outside, &work_dir), "{outside}");
        }
    }

    #[test]
    fn normalization_resolves_dot_and_parent_without_escaping() {
        assert_eq!(
            normalize_lexically(Path::new("/a/b/./c/../d")),
            Path::new("/a/b/d")
        );
        // A traversal that would escape the root keeps a `..` component rather
        // than silently resolving above it, so it cannot match a work-dir prefix.
        assert_eq!(
            normalize_lexically(Path::new("/a/../../b")),
            Path::new("/../b")
        );
    }
}
