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

//! A process crash at a chosen write, for enumerating crash points.
//!
//! [`CrashStore`] passes requests through until the write it was told to crash
//! at. That write never reaches storage, and from then on every request, read
//! or write, waits forever: the process is gone, so nothing it would have done
//! next happens. The caller drops its work and reopens the inner store, which
//! holds exactly the writes that landed before the crash.
//!
//! A write that landed but whose response the process never saw needs no mode
//! of its own: crashing at write `n + 1` leaves storage as it would be after
//! write `n` landed and the process died before reading its response.

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use parking_lot::Mutex;
use tokio::sync::Notify;

#[derive(Default)]
struct State {
    /// Writes that reached storage.
    writes: usize,
    /// The write (counted from creation) that crashes the process.
    crash_at: Option<usize>,
    crashed: bool,
}

struct Shared {
    state: Mutex<State>,
    crashed: Notify,
}

impl Shared {
    /// Let a request through, or never return once the process has crashed.
    async fn admit(&self, write: bool) {
        let crash = {
            let mut state = self.state.lock();
            if !state.crashed && write {
                if state.crash_at == Some(state.writes) {
                    state.crashed = true;
                    true
                } else {
                    state.writes += 1;
                    false
                }
            } else {
                state.crashed
            }
        };
        if crash {
            self.crashed.notify_one();
            std::future::pending::<()>().await;
        }
        if self.state.lock().crashed {
            std::future::pending::<()>().await;
        }
    }
}

/// Passes requests through until a chosen write, then stops answering.
pub struct CrashStore {
    inner: Arc<dyn ObjectStore>,
    shared: Arc<Shared>,
}

impl CrashStore {
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            shared: Arc::new(Shared {
                state: Mutex::new(State::default()),
                crashed: Notify::new(),
            }),
        }
    }

    /// Writes that reached storage so far.
    #[must_use]
    pub fn writes(&self) -> usize {
        self.shared.state.lock().writes
    }

    /// Crash at the `after`-th write from now: `0` crashes at the next write.
    pub fn crash_after(&self, after: usize) {
        let mut state = self.shared.state.lock();
        state.crash_at = Some(state.writes + after);
    }

    #[must_use]
    pub fn has_crashed(&self) -> bool {
        self.shared.state.lock().crashed
    }

    /// Wait until the process has crashed.
    pub async fn crashed(&self) {
        let notified = self.shared.crashed.notified();
        if self.has_crashed() {
            return;
        }
        notified.await;
    }
}

impl fmt::Debug for CrashStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CrashStore")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Display for CrashStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CrashStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CrashStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.shared.admit(true).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.shared.admit(true).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.shared.admit(false).await;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.shared.admit(false).await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        // Each delete is its own write.
        let shared = Arc::clone(&self.shared);
        let admitted = locations
            .then(move |location| {
                let shared = Arc::clone(&shared);
                async move {
                    shared.admit(true).await;
                    location
                }
            })
            .boxed();
        self.inner.delete_stream(admitted)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let shared = Arc::clone(&self.shared);
        let inner = self.inner.list(prefix);
        futures::stream::once(async move {
            shared.admit(false).await;
            inner
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let shared = Arc::clone(&self.shared);
        let inner = self.inner.list_with_offset(prefix, offset);
        futures::stream::once(async move {
            shared.admit(false).await;
            inner
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.shared.admit(false).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.shared.admit(true).await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.shared.admit(true).await;
        self.inner.rename_opts(from, to, options).await
    }
}
