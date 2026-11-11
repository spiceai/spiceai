// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic request delivery and response faults for protocol experiments.
use std::{collections::BTreeMap, fmt, sync::Arc};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, future::BoxFuture, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart, path::Path,
};
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Notify, oneshot};

use super::Op;

type Predicate = Arc<dyn Fn(&Path) -> bool + Send + Sync>;
/// Async work executed immediately before a held request reaches storage.
pub type BeforeHook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

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

struct Held {
    path: Path,
    payload: PutPayload,
    options: PutOptions,
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

/// Faults remain independent of caller futures: dropping a caller does not cancel
/// a captured request, which may still land after a restart or takeover.
pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    state: Arc<Mutex<State>>,
    applied: Arc<AsyncMutex<()>>,
    captured: Notify,
}

fn injected(reason: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "FaultStore",
        source: std::io::Error::other(reason.into()).into(),
    }
}

impl FaultStore {
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(State::default())),
            applied: Arc::new(AsyncMutex::new(())),
            captured: Notify::new(),
        }
    }

    /// Arm one matching request. Rules are consumed in insertion order.
    pub fn arm(
        &self,
        op: Op,
        predicate: impl Fn(&Path) -> bool + Send + Sync + 'static,
        fault: Fault,
    ) {
        self.state.lock().rules.push(Rule {
            op,
            predicate: Arc::new(predicate),
            fault,
            repeat: false,
        });
    }

    /// Keep failing matching requests until rules are cleared.
    pub fn fail_while(&self, op: Op, predicate: impl Fn(&Path) -> bool + Send + Sync + 'static) {
        self.state.lock().rules.push(Rule {
            op,
            predicate: Arc::new(predicate),
            fault: Fault::Fail,
            repeat: true,
        });
    }

    pub fn clear_rules(&self) {
        self.state.lock().rules.clear();
    }

    /// Exact requests refused before I/O, for explicitly scheduled copy actions.
    #[must_use]
    pub fn blocked(&self) -> Vec<(Path, PutPayload, PutOptions)> {
        self.state.lock().blocked.clone()
    }

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

    #[must_use]
    pub fn log(&self) -> Vec<Applied> {
        self.state.lock().log.clone()
    }

    #[must_use]
    pub fn held_ids(&self) -> Vec<u64> {
        self.state.lock().held.keys().copied().collect()
    }

    /// Wait for a newly captured request without polling the executor.
    pub async fn wait_held(&self, after: Option<u64>) -> u64 {
        loop {
            let notified = self.captured.notified();
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
        self.state
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
        let _guard = self.applied.lock().await;
        let op = put_op(&options.mode);
        let mode = format!("{:?}", options.mode);
        let result = self.inner.put_opts(path, payload.clone(), options).await;
        self.state.lock().log.push(Applied {
            path: path.clone(),
            op,
            mode,
            payload: Some(payload),
            result: result.as_ref().map(|_| ()).map_err(ToString::to_string),
        });
        result
    }

    /// Apply the exact original bytes and condition, even if already delivered.
    /// # Errors
    /// Unknown id or underlying conditional-put failure.
    pub async fn deliver(&self, id: u64) -> object_store::Result<PutResult> {
        let (path, payload, options, hook) = {
            let state = self.state.lock();
            let h = state
                .held
                .get(&id)
                .ok_or_else(|| injected("unknown request"))?;
            (
                h.path.clone(),
                h.payload.clone(),
                h.options.clone(),
                h.hook.clone(),
            )
        };
        if let Some(hook) = hook {
            hook().await;
        }
        let result = self.put_inner(&path, payload, options).await;
        if let Some(held) = self.state.lock().held.get_mut(&id)
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
        let mut state = self.state.lock();
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
            .state
            .lock()
            .held
            .get_mut(&id)
            .and_then(|h| h.response.take())
            .ok_or_else(|| injected("no pending response"))?;
        let _ = sender.send(Err(injected("response lost")));
        Ok(())
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
        match self.fault(put_op(&options.mode), path) {
            Some(Fault::Fail) => {
                self.state
                    .lock()
                    .blocked
                    .push((path.clone(), payload, options));
                Err(injected("put failed before I/O"))
            }
            Some(Fault::Hold) => {
                let (sender, receiver) = oneshot::channel();
                {
                    let mut state = self.state.lock();
                    let id = state.next;
                    state.next += 1;
                    state.held.insert(
                        id,
                        Held {
                            path: path.clone(),
                            payload,
                            options,
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
            fault => {
                let result = self.put_inner(path, payload, options).await;
                if fault == Some(Fault::DropResponse) {
                    Err(injected("response lost after I/O"))
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
        if self.fault(op, path).is_some() {
            return Err(injected("get failed before I/O"));
        }
        let _guard = self.applied.lock().await;
        let mode = format!("{options:?}");
        let result = self.inner.get_opts(path, options).await;
        self.state.lock().log.push(Applied {
            path: path.clone(),
            op,
            mode,
            payload: None,
            result: result.as_ref().map(|_| ()).map_err(ToString::to_string),
        });
        result
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let _guard = self.applied.lock().await;
        let answer = self.inner.put_multipart_opts(path, options).await;
        record(&self.state, path, Op::Put, "multipart begin", None, &answer);
        Ok(Box::new(LoggedUpload {
            inner: answer?,
            path: path.clone(),
            state: Arc::clone(&self.state),
            applied: Arc::clone(&self.applied),
        }))
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        let applied = Arc::clone(&self.applied);
        paths
            .then(move |path| {
                let inner = Arc::clone(&inner);
                let state = Arc::clone(&state);
                let applied = Arc::clone(&applied);
                async move {
                    let path = path?;
                    let _guard = applied.lock().await;
                    let answer = inner
                        .delete_stream(futures::stream::iter([Ok(path.clone())]).boxed())
                        .next()
                        .await
                        .unwrap_or_else(|| Err(injected("delete returned no result")));
                    record(&state, &path, Op::Delete, "delete", None, &answer);
                    answer
                }
            })
            .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let state = Arc::clone(&self.state);
        let applied = Arc::clone(&self.applied);
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let _guard = applied.lock().await;
            let answer = inner.list(prefix.as_ref()).try_collect::<Vec<_>>().await;
            record(
                &state,
                &prefix.unwrap_or_default(),
                Op::List,
                "list",
                None,
                &answer,
            );
            match answer {
                Ok(objects) => futures::stream::iter(objects.into_iter().map(Ok)).boxed(),
                Err(error) => futures::stream::iter([Err(error)]).boxed(),
            }
        })
        .flatten()
        .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let _guard = self.applied.lock().await;
        let answer = self.inner.list_with_delimiter(prefix).await;
        record(
            &self.state,
            &prefix.cloned().unwrap_or_default(),
            Op::List,
            "delimiter",
            None,
            &answer,
        );
        answer
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let _guard = self.applied.lock().await;
        let mode = format!("copy from {from}: {options:?}");
        let answer = self.inner.copy_opts(from, to, options).await;
        record(&self.state, to, Op::Copy, &mode, None, &answer);
        answer
    }
}

fn record<T>(
    state: &Mutex<State>,
    path: &Path,
    op: Op,
    mode: &str,
    payload: Option<PutPayload>,
    answer: &object_store::Result<T>,
) {
    state.lock().log.push(Applied {
        path: path.clone(),
        op,
        mode: mode.into(),
        payload,
        result: answer.as_ref().map(|_| ()).map_err(ToString::to_string),
    });
}

struct LoggedUpload {
    inner: Box<dyn MultipartUpload>,
    path: Path,
    state: Arc<Mutex<State>>,
    applied: Arc<AsyncMutex<()>>,
}
impl fmt::Debug for LoggedUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoggedUpload")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
#[async_trait]
impl MultipartUpload for LoggedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let future = self.inner.put_part(data.clone());
        let state = Arc::clone(&self.state);
        let applied = Arc::clone(&self.applied);
        let path = self.path.clone();
        Box::pin(async move {
            let _guard = applied.lock().await;
            let answer = future.await;
            record(
                &state,
                &path,
                Op::Put,
                "multipart part",
                Some(data),
                &answer,
            );
            answer
        })
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let _guard = self.applied.lock().await;
        let answer = self.inner.complete().await;
        record(
            &self.state,
            &self.path,
            Op::Put,
            "multipart complete",
            None,
            &answer,
        );
        answer
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        let _guard = self.applied.lock().await;
        let answer = self.inner.abort().await;
        record(
            &self.state,
            &self.path,
            Op::Put,
            "multipart abort",
            None,
            &answer,
        );
        answer
    }
}
