// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic request delivery and response faults for protocol experiments.
//!
//! A rule matches one [`Op`] and a path predicate. The path is the object a
//! request writes or reads: the destination of a copy or rename, the upload's
//! path for every multipart step, each path of a delete stream, and the prefix
//! (empty when none) of a listing. What each [`Fault`] does per operation:
//!
//! | Operation | `Fail` | `DropResponse` | `Hold` |
//! |---|---|---|---|
//! | `Create`/`Cas`/`Put` | error, not applied | applied, error | captured until `deliver`/`respond` |
//! | `MultipartComplete` | error, object absent, upload still open | applied (object present), error | captured until `deliver`/`respond` |
//! | `MultipartBegin`/`Part`/`Abort` | error, not applied | applied, error | as `Fail` |
//! | `Delete` (per path) | error for that path, object kept | deleted, error for that path | as `Fail` |
//! | `Copy`/`Rename` | error, not applied | applied, error | as `Fail` |
//! | `Get`/`GetConditional`/`Head`/`List` | error, not sent | as `Fail` | as `Fail` |
//!
//! A read has no side effect, so a lost response is indistinguishable from a
//! failed request; listings report the error as their first item.
use std::{collections::BTreeMap, fmt, sync::Arc};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, future::BoxFuture, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, UploadPart,
    path::Path,
};
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Notify, oneshot};

use super::Op;

type Predicate = Arc<dyn Fn(&Path) -> bool + Send + Sync>;
/// Async work executed immediately before a held request reaches storage.
pub type BeforeHook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;
type SharedUpload = Arc<AsyncMutex<Box<dyn MultipartUpload>>>;

/// One-shot fault selected by operation and path predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    Hold,
    DropResponse,
    Fail,
}

struct Rule {
    op: Op,
    predicate: Predicate,
    fault: Fault,
    repeat: bool,
}

/// A captured request that `deliver` can apply, possibly more than once.
#[derive(Clone)]
enum Request {
    Put {
        payload: PutPayload,
        options: PutOptions,
    },
    Complete(SharedUpload),
}

struct Held {
    path: Path,
    request: Request,
    response: Option<oneshot::Sender<object_store::Result<PutResult>>>,
    first: Option<Result<PutResult, String>>,
    hook: Option<BeforeHook>,
}

/// A request actually applied to storage. Injected pre-I/O failures are excluded.
#[derive(Clone, Debug)]
pub struct Applied {
    pub path: Path,
    pub op: Op,
    pub mode: String,
    pub payload: Option<PutPayload>,
    pub result: Result<(), String>,
}

#[derive(Default)]
struct State {
    rules: Vec<Rule>,
    held: BTreeMap<u64, Held>,
    next: u64,
    log: Vec<Applied>,
    blocked: Vec<(Path, PutPayload, PutOptions)>,
}

/// State shared by the store and the multipart uploads it hands out.
#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    applied: AsyncMutex<()>,
    captured: Notify,
}

impl Shared {
    fn fault(&self, op: Op, path: &Path) -> Option<Fault> {
        let mut state = self.state.lock();
        let index = state
            .rules
            .iter()
            .position(|r| r.op == op && (r.predicate)(path))?;
        let fault = state.rules[index].fault;
        if !state.rules[index].repeat {
            state.rules.remove(index);
        }
        Some(fault)
    }

    fn record<T>(
        &self,
        path: &Path,
        op: Op,
        mode: &str,
        payload: Option<PutPayload>,
        answer: &object_store::Result<T>,
    ) {
        self.state.lock().log.push(Applied {
            path: path.clone(),
            op,
            mode: mode.into(),
            payload,
            result: answer.as_ref().map(|_| ()).map_err(ToString::to_string),
        });
    }

    /// Capture `request` and wait for `respond` or `drop_response`.
    async fn hold(&self, path: &Path, request: Request) -> object_store::Result<PutResult> {
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.state.lock();
            let id = state.next;
            state.next += 1;
            state.held.insert(
                id,
                Held {
                    path: path.clone(),
                    request,
                    response: Some(sender),
                    first: None,
                    hook: None,
                },
            );
        }
        self.captured.notify_waiters();
        receiver
            .await
            .map_err(|_| injected("response channel closed"))?
    }

    async fn complete(
        &self,
        path: &Path,
        upload: &SharedUpload,
    ) -> object_store::Result<PutResult> {
        let _guard = self.applied.lock().await;
        let answer = upload.lock().await.complete().await;
        self.record(
            path,
            Op::MultipartComplete,
            "multipart complete",
            None,
            &answer,
        );
        answer
    }
}

/// Faults remain independent of caller futures: dropping a caller does not cancel
/// a captured request, which may still land after a restart or takeover.
pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    shared: Arc<Shared>,
}

fn injected(reason: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "FaultStore",
        source: std::io::Error::other(reason.into()).into(),
    }
}

/// The error a `Fail` (or a fault that acts as one) returns for `op`.
fn refused(op: Op) -> object_store::Error {
    injected(format!("{op:?} failed before I/O"))
}

fn lost() -> object_store::Error {
    injected("response lost after I/O")
}

/// Whether `fault` stops a request that cannot be held before it reaches storage.
fn stops(fault: Option<Fault>) -> bool {
    matches!(fault, Some(Fault::Fail | Fault::Hold))
}

impl FaultStore {
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            shared: Arc::new(Shared::default()),
        }
    }

    /// Arm one matching request. Rules are consumed in insertion order.
    pub fn arm(
        &self,
        op: Op,
        predicate: impl Fn(&Path) -> bool + Send + Sync + 'static,
        fault: Fault,
    ) {
        self.shared.state.lock().rules.push(Rule {
            op,
            predicate: Arc::new(predicate),
            fault,
            repeat: false,
        });
    }

    /// Keep failing matching requests until rules are cleared.
    pub fn fail_while(&self, op: Op, predicate: impl Fn(&Path) -> bool + Send + Sync + 'static) {
        self.shared.state.lock().rules.push(Rule {
            op,
            predicate: Arc::new(predicate),
            fault: Fault::Fail,
            repeat: true,
        });
    }

    pub fn clear_rules(&self) {
        self.shared.state.lock().rules.clear();
    }

    /// Exact single-request puts refused before I/O, for explicitly scheduled copy actions.
    #[must_use]
    pub fn blocked(&self) -> Vec<(Path, PutPayload, PutOptions)> {
        self.shared.state.lock().blocked.clone()
    }

    #[must_use]
    pub fn log(&self) -> Vec<Applied> {
        self.shared.state.lock().log.clone()
    }

    #[must_use]
    pub fn held_ids(&self) -> Vec<u64> {
        self.shared.state.lock().held.keys().copied().collect()
    }

    /// Wait for a newly captured request without polling the executor.
    pub async fn wait_held(&self, after: Option<u64>) -> u64 {
        loop {
            let notified = self.shared.captured.notified();
            if let Some(id) = self
                .held_ids()
                .into_iter()
                .find(|id| after.is_none_or(|a| *id > a))
            {
                return id;
            }
            notified.await;
        }
    }

    /// Install a hook; it runs on each delivery, including duplicates.
    /// # Errors
    /// Unknown request id.
    pub fn before(&self, id: u64, hook: BeforeHook) -> object_store::Result<()> {
        self.shared
            .state
            .lock()
            .held
            .get_mut(&id)
            .ok_or_else(|| injected("unknown request"))?
            .hook = Some(hook);
        Ok(())
    }

    async fn put_inner(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let _guard = self.shared.applied.lock().await;
        let op = put_op(&options.mode);
        let mode = format!("{:?}", options.mode);
        let result = self.inner.put_opts(path, payload.clone(), options).await;
        self.shared.record(path, op, &mode, Some(payload), &result);
        result
    }

    /// Apply the exact original request, even if already delivered: the same
    /// bytes and condition for a put, another `complete` for a multipart
    /// completion (the inner store decides what a repeated completion does).
    /// # Errors
    /// Unknown id or underlying storage failure.
    pub async fn deliver(&self, id: u64) -> object_store::Result<PutResult> {
        let (path, request, hook) = {
            let state = self.shared.state.lock();
            let h = state
                .held
                .get(&id)
                .ok_or_else(|| injected("unknown request"))?;
            (h.path.clone(), h.request.clone(), h.hook.clone())
        };
        if let Some(hook) = hook {
            hook().await;
        }
        let result = match request {
            Request::Put { payload, options } => self.put_inner(&path, payload, options).await,
            Request::Complete(upload) => self.shared.complete(&path, &upload).await,
        };
        if let Some(held) = self.shared.state.lock().held.get_mut(&id)
            && held.first.is_none()
        {
            held.first = Some(result.as_ref().cloned().map_err(ToString::to_string));
        }
        result
    }

    /// Release the first delivery's response; duplicates never replace it.
    /// # Errors
    /// Unknown id, undelivered request, or already released response.
    pub fn respond(&self, id: u64) -> object_store::Result<()> {
        let mut state = self.shared.state.lock();
        let held = state
            .held
            .get_mut(&id)
            .ok_or_else(|| injected("unknown request"))?;
        let result = held
            .first
            .clone()
            .ok_or_else(|| injected("not delivered"))?;
        let sender = held
            .response
            .take()
            .ok_or_else(|| injected("response already released"))?;
        let _ = sender.send(result.map_err(injected));
        Ok(())
    }

    /// Return an ambiguous transport error, including when the write landed.
    /// # Errors
    /// Unknown id or already released response.
    pub fn drop_response(&self, id: u64) -> object_store::Result<()> {
        let sender = self
            .shared
            .state
            .lock()
            .held
            .get_mut(&id)
            .and_then(|h| h.response.take())
            .ok_or_else(|| injected("no pending response"))?;
        let _ = sender.send(Err(injected("response lost")));
        Ok(())
    }

    /// A listing that is refused, or forwarded and logged as one request.
    fn listing(
        &self,
        prefix: Option<&Path>,
        mode: String,
        list: impl FnOnce(&dyn ObjectStore) -> BoxStream<'static, object_store::Result<ObjectMeta>>
        + Send
        + 'static,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let shared = Arc::clone(&self.shared);
        let prefix = prefix.cloned().unwrap_or_default();
        futures::stream::once(async move {
            if shared.fault(Op::List, &prefix).is_some() {
                return futures::stream::iter([Err(refused(Op::List))]).boxed();
            }
            let _guard = shared.applied.lock().await;
            let answer = list(inner.as_ref()).try_collect::<Vec<_>>().await;
            shared.record(&prefix, Op::List, &mode, None, &answer);
            match answer {
                Ok(objects) => futures::stream::iter(objects.into_iter().map(Ok)).boxed(),
                Err(error) => futures::stream::iter([Err(error)]).boxed(),
            }
        })
        .flatten()
        .boxed()
    }
}

fn put_op(mode: &PutMode) -> Op {
    match mode {
        PutMode::Create => Op::Create,
        PutMode::Update(_) => Op::Cas,
        PutMode::Overwrite => Op::Put,
    }
}
impl fmt::Debug for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FaultStore")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        match self.shared.fault(put_op(&options.mode), path) {
            Some(Fault::Fail) => {
                self.shared
                    .state
                    .lock()
                    .blocked
                    .push((path.clone(), payload, options));
                Err(injected("put failed before I/O"))
            }
            Some(Fault::Hold) => {
                self.shared
                    .hold(path, Request::Put { payload, options })
                    .await
            }
            fault => {
                let result = self.put_inner(path, payload, options).await;
                if fault == Some(Fault::DropResponse) {
                    Err(lost())
                } else {
                    result
                }
            }
        }
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let op = if options.head {
            Op::Head
        } else if options.if_none_match.is_some() {
            Op::GetConditional
        } else {
            Op::Get
        };
        if self.shared.fault(op, path).is_some() {
            return Err(injected("get failed before I/O"));
        }
        let _guard = self.shared.applied.lock().await;
        let mode = format!("{options:?}");
        let result = self.inner.get_opts(path, options).await;
        self.shared.record(path, op, &mode, None, &result);
        result
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let fault = self.shared.fault(Op::MultipartBegin, path);
        if stops(fault) {
            return Err(refused(Op::MultipartBegin));
        }
        let answer = {
            let _guard = self.shared.applied.lock().await;
            let answer = self.inner.put_multipart_opts(path, options).await;
            self.shared
                .record(path, Op::MultipartBegin, "multipart begin", None, &answer);
            answer
        };
        let upload = answer?;
        if fault == Some(Fault::DropResponse) {
            // The caller never learns of the upload, so it is left open, as an
            // orphaned upload is on a real store.
            return Err(lost());
        }
        Ok(Box::new(FaultUpload {
            inner: Arc::new(AsyncMutex::new(upload)),
            path: path.clone(),
            shared: Arc::clone(&self.shared),
        }))
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let shared = Arc::clone(&self.shared);
        paths
            .then(move |path| {
                let inner = Arc::clone(&inner);
                let shared = Arc::clone(&shared);
                async move {
                    let path = path?;
                    let fault = shared.fault(Op::Delete, &path);
                    if stops(fault) {
                        return Err(refused(Op::Delete));
                    }
                    let _guard = shared.applied.lock().await;
                    let answer = inner
                        .delete_stream(futures::stream::iter([Ok(path.clone())]).boxed())
                        .next()
                        .await
                        .unwrap_or_else(|| Err(injected("delete returned no result")));
                    shared.record(&path, Op::Delete, "delete", None, &answer);
                    if answer.is_ok() && fault == Some(Fault::DropResponse) {
                        return Err(lost());
                    }
                    answer
                }
            })
            .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let owned = prefix.cloned();
        self.listing(prefix, "list".into(), move |inner| {
            inner.list(owned.as_ref())
        })
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let owned = prefix.cloned();
        let after = offset.clone();
        self.listing(prefix, format!("list after {offset}"), move |inner| {
            inner.list_with_offset(owned.as_ref(), &after)
        })
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let at = prefix.cloned().unwrap_or_default();
        if self.shared.fault(Op::List, &at).is_some() {
            return Err(refused(Op::List));
        }
        let _guard = self.shared.applied.lock().await;
        let answer = self.inner.list_with_delimiter(prefix).await;
        self.shared
            .record(&at, Op::List, "delimiter", None, &answer);
        answer
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let fault = self.shared.fault(Op::Copy, to);
        if stops(fault) {
            return Err(refused(Op::Copy));
        }
        let _guard = self.shared.applied.lock().await;
        let mode = format!("copy from {from}: {options:?}");
        let answer = self.inner.copy_opts(from, to, options).await;
        self.shared.record(to, Op::Copy, &mode, None, &answer);
        if answer.is_ok() && fault == Some(Fault::DropResponse) {
            return Err(lost());
        }
        answer
    }
    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let fault = self.shared.fault(Op::Rename, to);
        if stops(fault) {
            return Err(refused(Op::Rename));
        }
        let _guard = self.shared.applied.lock().await;
        let mode = format!("rename from {from}: {options:?}");
        let answer = self.inner.rename_opts(from, to, options).await;
        self.shared.record(to, Op::Rename, &mode, None, &answer);
        if answer.is_ok() && fault == Some(Fault::DropResponse) {
            return Err(lost());
        }
        answer
    }
}

/// A multipart upload whose part, complete and abort requests are faultable.
/// A held completion keeps the upload, so a part or abort issued while it is
/// being delivered fails rather than racing it.
struct FaultUpload {
    inner: SharedUpload,
    path: Path,
    shared: Arc<Shared>,
}
impl fmt::Debug for FaultUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FaultUpload")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
#[async_trait]
impl MultipartUpload for FaultUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let fault = self.shared.fault(Op::MultipartPart, &self.path);
        if stops(fault) {
            return Box::pin(futures::future::ready(Err(refused(Op::MultipartPart))));
        }
        let Ok(mut upload) = self.inner.try_lock() else {
            return Box::pin(futures::future::ready(Err(injected(
                "multipart upload busy with a held completion",
            ))));
        };
        let future = upload.put_part(data.clone());
        drop(upload);
        let shared = Arc::clone(&self.shared);
        let path = self.path.clone();
        Box::pin(async move {
            let _guard = shared.applied.lock().await;
            let answer = future.await;
            shared.record(
                &path,
                Op::MultipartPart,
                "multipart part",
                Some(data),
                &answer,
            );
            if answer.is_ok() && fault == Some(Fault::DropResponse) {
                return Err(lost());
            }
            answer
        })
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        match self.shared.fault(Op::MultipartComplete, &self.path) {
            Some(Fault::Fail) => Err(refused(Op::MultipartComplete)),
            Some(Fault::Hold) => {
                self.shared
                    .hold(&self.path, Request::Complete(Arc::clone(&self.inner)))
                    .await
            }
            fault => {
                let answer = self.shared.complete(&self.path, &self.inner).await;
                if answer.is_ok() && fault == Some(Fault::DropResponse) {
                    return Err(lost());
                }
                answer
            }
        }
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        let fault = self.shared.fault(Op::MultipartAbort, &self.path);
        if stops(fault) {
            return Err(refused(Op::MultipartAbort));
        }
        let _guard = self.shared.applied.lock().await;
        let answer = self.inner.lock().await.abort().await;
        self.shared.record(
            &self.path,
            Op::MultipartAbort,
            "multipart abort",
            None,
            &answer,
        );
        if answer.is_ok() && fault == Some(Fault::DropResponse) {
            return Err(lost());
        }
        answer
    }
}
