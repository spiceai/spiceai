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

//! The [`ObjectStore`] [`NodeCache::wrap`] returns.
//!
//! Reads of paths the `cacheable` predicate accepts go through the node
//! cache. Everything else is passed to the wrapped store unchanged: lists;
//! HEAD requests; reads with any precondition (`If-Match`, `If-None-Match`,
//! modified-since, version); reads of rejected paths; empty or invalid
//! ranges and `get_ranges` ranges that end past the object, so the wrapped
//! store produces its own result or error; and every read of a namespace the
//! cache is off for.
//!
//! Writes, deletes, copies and renames go to the wrapped store after the
//! wrapper evicts each cacheable path they change (a delete's and a rename's
//! source, a rename's, copy's or upload's target) durably, and no fill of
//! the path is written until the store's call returns (an upload: until it
//! completes or is aborted or dropped). A deleted, renamed or overwritten
//! object is never served from blocks of what it was, before or after a
//! restart. An eviction that cannot be made durable turns the cache off for
//! the namespace and the store's call still goes ahead, so the cache never
//! blocks a delete. A cacheable path uploaded through the wrapper (a
//! successful put, a completed multipart upload) is remembered as
//! [written here](NodeCache::written_here) until it is deleted, renamed or
//! overwritten through it.
//!
//! Blocks are keyed by object size, so the wrapper needs each object's size
//! before it can look a block up. Sizes come from [`NodeCache::note_size`]
//! and [`NodeCache::prefetch`], from puts, and from listings; for an object
//! with resident blocks, from their keys (so a restarted cache serves it
//! without asking the store); otherwise from a HEAD on the first read of the
//! object in this process. They are kept per namespace, shared by every
//! wrapper of it, and dropped when the wrapper writes, deletes, copies onto
//! or renames the path. At most [`MAX_SIZES`] objects' sizes are kept per
//! namespace, the least recently used dropped first. A read whose HEAD fails
//! goes to the wrapped store. Every fill checks the size the store reports,
//! so a size that is no longer right only sends the read to the store.
//!
//! Reads served from the cache return the object's metadata as of the HEAD
//! or listing that sized it, or as the GET that fetched its blocks reported
//! it, and no attributes. An object sized from its resident blocks reports
//! the e-tag and modification time stored with them (no version). Stored
//! metadata without a modification time is not complete (an entry whose
//! metadata did not fit beside its key has none): a read that returns
//! metadata (`get_opts`) whose blocks were all resident then asks the store
//! with a HEAD, and still serves the bytes from the cache; `get_ranges`,
//! which returns bytes only, needs only the size. A modification time is
//! never made up.
//!
//! A read of more than an eighth of the cache's budget, or of more than
//! 64 MiB whatever the budget, is a scan: it is passed to the wrapped store
//! as is, so its bytes stream from the store without being held in memory,
//! and it admits nothing. Every read through the wrapper, whichever way it
//! is served, holds prefetch back until it returns (a streamed response:
//! until its stream ends, fails or is dropped).

use std::fmt;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result, UploadPart,
};

use crate::block_store::StoredMeta;
use crate::lru::Lru;
use crate::node_cache::{ForegroundGuard, Hold, Namespace, NodeCache, ReadCtx, ReadError, Served};

/// Decides which paths of a wrapped store hold immutable objects the cache
/// may keep.
///
/// The contract: a path accepted here is written once and never overwritten
/// in place, so its bytes never change while it exists. The cache relies on
/// it and does not check cached blocks against the store again. Writes,
/// deletes, copies and renames through the wrapper evict what they change,
/// but a change made around the wrapper (another process, another client of
/// the store) is not seen: blocks are keyed by object size, so a rewrite with
/// another size is not served from old blocks, but a rewrite with the same
/// size would be. Reject every path that can be rewritten in place (table
/// heads, manifests, logs).
pub type Cacheable = Arc<dyn Fn(&Path) -> bool + Send + Sync>;

/// Objects whose sizes one namespace keeps; a forgotten size costs one HEAD
/// on the object's next read.
pub(crate) const MAX_SIZES: usize = 65_536;

/// Paths one delete evicts at once: one durable removal per batch.
const DELETE_BATCH: usize = 1024;

/// What is known of an object: its size, and its full metadata when a HEAD,
/// a listing, a GET or its resident blocks reported it.
#[derive(Debug, Clone)]
pub(crate) struct Known {
    size: u64,
    meta: Option<ObjectMeta>,
}

/// Known objects by path, at most [`MAX_SIZES`] entries, least recently used
/// dropped first.
#[derive(Debug)]
pub(crate) struct Sizes(Lru<Path, Known>);

impl Default for Sizes {
    fn default() -> Self {
        Self(Lru::new(MAX_SIZES))
    }
}

impl Sizes {
    fn get(&mut self, location: &Path) -> Option<Known> {
        self.0.get(location).cloned()
    }

    /// The object's size, without its metadata.
    fn size_of(&mut self, location: &Path) -> Option<u64> {
        self.0.get(location).map(|known| known.size)
    }

    fn insert_meta(&mut self, meta: ObjectMeta) {
        let location = meta.location.clone();
        self.0.insert(
            location,
            Known {
                size: meta.size,
                meta: Some(meta),
            },
        );
    }

    /// Records the size, keeping the metadata already known for that size.
    pub(crate) fn insert_size(&mut self, location: &Path, size: u64) {
        let meta = self
            .0
            .peek(location)
            .and_then(|k| k.meta.clone().filter(|m| m.size == size));
        self.0.insert(location.clone(), Known { size, meta });
    }

    fn remove(&mut self, location: &Path) {
        self.0.remove(location);
    }

    pub(crate) fn remove_str(&mut self, location: &str) {
        if let Ok(p) = Path::parse(location) {
            self.remove(&p);
        }
    }
}

pub(crate) struct CachedObjectStore {
    cache: Arc<NodeCache>,
    /// The caller's namespace for `inner`, which every block key carries.
    ns: Arc<Namespace>,
    inner: Arc<dyn ObjectStore>,
    cacheable: Cacheable,
}

/// Why a read is passed to the store untouched.
#[derive(Debug, Clone, Copy)]
enum Pass {
    /// Not served through the cache ([`crate::NodeCacheStats::bypass_reads`]).
    Bypass,
    /// A scan ([`crate::NodeCacheStats::admission_bypass_reads`]).
    Scan,
}

/// An object's metadata for a read, as far as it is known without asking
/// the store.
enum MetaFor {
    Full(ObjectMeta),
    /// The size alone: the metadata comes from the read's GET, or a HEAD.
    Size(u64),
}

fn object_meta(location: &Path, size: u64, stored: &StoredMeta) -> Option<ObjectMeta> {
    stored.last_modified.map(|last_modified| ObjectMeta {
        location: location.clone(),
        last_modified,
        size,
        e_tag: stored.e_tag.clone(),
        version: None,
    })
}

impl CachedObjectStore {
    pub(crate) fn new(
        cache: Arc<NodeCache>,
        ns: Arc<Namespace>,
        inner: Arc<dyn ObjectStore>,
        cacheable: Cacheable,
    ) -> Self {
        Self {
            cache,
            ns,
            inner,
            cacheable,
        }
    }

    /// True for a read the cache may serve: a cacheable path of a namespace
    /// the cache is on for.
    fn serves(&self, location: &Path) -> bool {
        !self.ns.is_disabled() && (self.cacheable)(location)
    }

    /// True for a read of `requested` bytes that is passed to the store as
    /// a scan.
    fn is_scan(&self, requested: u64) -> bool {
        requested > self.cache.scan_threshold()
    }

    /// Holds prefetch back while the read runs.
    fn foreground(&self) -> ForegroundGuard {
        self.cache.foreground()
    }

    fn forget(&self, location: &Path) {
        self.ns.sizes.lock().remove(location);
    }

    /// The object's size: noted, listed or from an earlier HEAD; else, for an
    /// object with resident blocks (after a restart), the size their keys
    /// carry; else a HEAD.
    async fn size(&self, location: &Path) -> Result<u64> {
        if let Some(size) = self.ns.sizes.lock().size_of(location) {
            return Ok(size);
        }
        if let Some(info) = self.cache.resident_info(&self.ns.name, location) {
            return Ok(info.size);
        }
        Ok(self.head(location).await?.size)
    }

    /// The object's metadata: listed or from an earlier HEAD; else its noted
    /// size with the metadata its resident blocks carry when it is complete
    /// (has a modification time), or the size alone; else, for an object
    /// with resident blocks, the size their keys carry and the metadata
    /// stored with them when it is complete; else a HEAD.
    async fn meta(&self, location: &Path) -> Result<MetaFor> {
        let known = self.ns.sizes.lock().get(location);
        if let Some(Known {
            meta: Some(meta), ..
        }) = known
        {
            return Ok(MetaFor::Full(meta));
        }
        let resident = self.cache.resident_info(&self.ns.name, location);
        if let Some(Known { size, .. }) = known {
            if let Some(meta) = resident
                .filter(|info| info.size == size)
                .and_then(|info| object_meta(location, size, &info.meta))
            {
                self.ns.sizes.lock().insert_meta(meta.clone());
                return Ok(MetaFor::Full(meta));
            }
            return Ok(MetaFor::Size(size));
        }
        if let Some(meta) = resident.and_then(|info| object_meta(location, info.size, &info.meta)) {
            self.ns.sizes.lock().insert_meta(meta.clone());
            return Ok(MetaFor::Full(meta));
        }
        self.head(location).await.map(MetaFor::Full)
    }

    /// A HEAD of the object, remembered.
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let meta = self.inner.head(location).await?;
        self.ns.sizes.lock().insert_meta(meta.clone());
        Ok(meta)
    }

    fn count(&self, why: Pass) {
        match why {
            Pass::Bypass => self.cache.count_bypass(),
            Pass::Scan => self.cache.count_scan(),
        }
    }

    /// Passes a read to the store, its bytes streamed from the store and
    /// none admitted, counted as `why`.
    async fn pass(
        &self,
        why: Pass,
        location: &Path,
        options: GetOptions,
        foreground: ForegroundGuard,
    ) -> Result<GetResult> {
        self.count(why);
        let result = self.inner.get_opts(location, options).await?;
        Ok(held_until_read(result, foreground))
    }

    /// [`CachedObjectStore::pass`] for `get_ranges`.
    async fn pass_ranges(
        &self,
        why: Pass,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>> {
        self.count(why);
        self.inner.get_ranges(location, ranges).await
    }

    fn ctx<'a>(&'a self, location: &'a Path, size: u64) -> ReadCtx<'a> {
        ReadCtx {
            ns: &self.ns,
            path: location,
            size,
            inner: &self.inner,
        }
    }

    /// `listed`, with the size and metadata of each cacheable object it lists
    /// remembered.
    fn remembering(
        &self,
        listed: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let ns = Arc::clone(&self.ns);
        let cacheable = Arc::clone(&self.cacheable);
        listed
            .inspect(move |m| {
                if let Ok(m) = m {
                    remember(&ns, &cacheable, m);
                }
            })
            .boxed()
    }

    /// Holds the cacheable paths of `paths` and evicts them durably. An
    /// eviction that fails has turned the cache off for the namespace; the
    /// caller's change goes ahead either way.
    async fn hold(&self, paths: &[&Path]) -> Hold {
        let hold = self.cache.hold(&self.ns);
        let cacheable: Vec<Arc<str>> = paths
            .iter()
            .filter(|p| (self.cacheable)(p))
            .map(|p| Arc::from(p.as_ref()))
            .collect();
        // A failure is counted, recorded and has turned the namespace off.
        let _ = hold.evict(cacheable).await;
        hold
    }
}

/// A response stream that holds a read's foreground guard until it ends,
/// fails or is dropped.
struct HeldStream {
    inner: BoxStream<'static, Result<Bytes>>,
    foreground: Option<ForegroundGuard>,
}

impl Stream for HeldStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = self.inner.poll_next_unpin(cx);
        if matches!(item, Poll::Ready(None | Some(Err(_)))) {
            self.foreground = None;
        }
        item
    }
}

/// `result`, with `foreground` held until its stream is read (a file
/// payload, already open, drops it at once).
fn held_until_read(result: GetResult, foreground: ForegroundGuard) -> GetResult {
    let GetResult {
        payload,
        meta,
        range,
        attributes,
    } = result;
    let payload = match payload {
        GetResultPayload::Stream(inner) => GetResultPayload::Stream(
            HeldStream {
                inner,
                foreground: Some(foreground),
            }
            .boxed(),
        ),
        payload @ GetResultPayload::File(..) => payload,
    };
    GetResult {
        payload,
        meta,
        range,
        attributes,
    }
}

fn remember(ns: &Namespace, cacheable: &Cacheable, meta: &ObjectMeta) {
    if cacheable(&meta.location) {
        ns.sizes.lock().insert_meta(meta.clone());
    }
}

fn is_conditional(options: &GetOptions) -> bool {
    options.if_match.is_some()
        || options.if_none_match.is_some()
        || options.if_modified_since.is_some()
        || options.if_unmodified_since.is_some()
        || options.version.is_some()
}

fn concat(parts: Vec<Bytes>) -> Bytes {
    if parts.len() == 1 {
        return parts.into_iter().next().unwrap_or_default();
    }
    let mut out = BytesMut::with_capacity(parts.iter().map(Bytes::len).sum());
    for p in parts {
        out.extend_from_slice(&p);
    }
    out.freeze()
}

impl fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeCache({})", self.inner)
    }
}

impl fmt::Debug for CachedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedObjectStore")
            .field("store", &self.ns.name)
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

/// A multipart upload through the wrapper: its path stays held until the
/// upload completes, is aborted or is dropped, and a completed upload of a
/// cacheable path is remembered as written here.
#[derive(Debug)]
struct HeldUpload {
    inner: Box<dyn MultipartUpload>,
    ns: Arc<Namespace>,
    location: Path,
    hold: Option<Hold>,
}

#[async_trait]
impl MultipartUpload for HeldUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let done = self.inner.complete().await;
        if done.is_ok() {
            self.ns.note_written(&self.location);
        }
        self.hold = None;
        done
    }

    async fn abort(&mut self) -> Result<()> {
        let aborted = self.inner.abort().await;
        self.hold = None;
        aborted
    }
}

#[async_trait]
impl ObjectStore for CachedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if !(self.cacheable)(location) {
            return self.inner.put_opts(location, payload, opts).await;
        }
        let _hold = self.hold(&[location]).await;
        let size = payload.content_length() as u64;
        let put = self.inner.put_opts(location, payload, opts).await;
        if put.is_ok() {
            self.ns.note_written(location);
            self.ns.sizes.lock().insert_size(location, size);
        }
        put
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        if !(self.cacheable)(location) {
            return self.inner.put_multipart_opts(location, opts).await;
        }
        let hold = self.hold(&[location]).await;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(HeldUpload {
            inner,
            ns: Arc::clone(&self.ns),
            location: location.clone(),
            hold: Some(hold),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let foreground = self.foreground();
        if options.head || is_conditional(&options) || !self.serves(location) {
            return self.pass(Pass::Bypass, location, options, foreground).await;
        }
        // Without the object's size no block can be found: the store serves
        // the read, or returns its own error.
        let Ok(known) = self.meta(location).await else {
            return self.pass(Pass::Bypass, location, options, foreground).await;
        };
        let size = match &known {
            MetaFor::Full(meta) => meta.size,
            MetaFor::Size(size) => *size,
        };
        let range = match &options.range {
            None => 0..size,
            Some(r) => match r.as_range(size) {
                Ok(r) => r,
                Err(_) => return self.pass(Pass::Bypass, location, options, foreground).await,
            },
        };
        if range.is_empty() {
            return self.pass(Pass::Bypass, location, options, foreground).await;
        }
        if self.is_scan(range.end - range.start) {
            return self.pass(Pass::Scan, location, options, foreground).await;
        }
        let Served {
            parts,
            fetched_meta,
        } = match self
            .cache
            .read(&self.ctx(location, size), range.clone())
            .await
        {
            Ok(served) => served,
            Err(ReadError::Store(e)) => return Err(e),
            Err(ReadError::SizeChanged) => {
                self.forget(location);
                return self.pass(Pass::Bypass, location, options, foreground).await;
            }
        };
        let meta = match known {
            MetaFor::Full(meta) => meta,
            MetaFor::Size(_) => {
                let reported = fetched_meta
                    .and_then(|m| object_meta(location, size, &m))
                    .or_else(|| {
                        self.cache
                            .resident_info(&self.ns.name, location)
                            .filter(|info| info.size == size)
                            .and_then(|info| object_meta(location, size, &info.meta))
                    });
                match reported {
                    Some(meta) => {
                        self.ns.sizes.lock().insert_meta(meta.clone());
                        meta
                    }
                    None => self.head(location).await?,
                }
            }
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::iter(parts.into_iter().map(Ok)).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let _foreground = self.foreground();
        // An empty list goes to the store too: whether it is an error (a
        // missing object, an unreachable store) is the store's answer.
        if ranges.is_empty() || !self.serves(location) {
            return self.pass_ranges(Pass::Bypass, location, ranges).await;
        }
        let Ok(size) = self.size(location).await else {
            return self.pass_ranges(Pass::Bypass, location, ranges).await;
        };
        // A range that ends past the object goes to the store unchanged: some
        // stores return what there is, others an error.
        if ranges.iter().any(|r| r.start >= r.end || r.end > size) {
            return self.pass_ranges(Pass::Bypass, location, ranges).await;
        }
        let requested = ranges
            .iter()
            .map(|r| r.end - r.start)
            .fold(0u64, u64::saturating_add);
        if self.is_scan(requested) {
            return self.pass_ranges(Pass::Scan, location, ranges).await;
        }
        let ctx = self.ctx(location, size);
        let reads = ranges.iter().map(|r| {
            let ctx = &ctx;
            async move {
                self.cache
                    .read(ctx, r.clone())
                    .await
                    .map(|served| concat(served.parts))
            }
        });
        match futures::future::try_join_all(reads).await {
            Ok(out) => Ok(out),
            Err(ReadError::Store(e)) => Err(e),
            Err(ReadError::SizeChanged) => {
                self.forget(location);
                self.pass_ranges(Pass::Bypass, location, ranges).await
            }
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // Each batch of paths is evicted durably before the store sees it, and
        // stays held (nothing of it filled) until the store reports it
        // deleted or the response stream is dropped.
        let hold = Arc::new(self.cache.hold(&self.ns));
        let cacheable = Arc::clone(&self.cacheable);
        let evicting = {
            let hold = Arc::clone(&hold);
            locations
                .ready_chunks(DELETE_BATCH)
                .then(move |batch| {
                    let hold = Arc::clone(&hold);
                    let cacheable = Arc::clone(&cacheable);
                    async move {
                        let paths: Vec<Arc<str>> = batch
                            .iter()
                            .filter_map(|r| r.as_ref().ok())
                            .filter(|p| cacheable(p))
                            .map(|p| Arc::from(p.as_ref()))
                            .collect();
                        // A failure has turned the namespace off; the delete
                        // goes ahead.
                        let _ = hold.evict(paths).await;
                        stream::iter(batch)
                    }
                })
                .flatten()
                .boxed()
        };
        self.inner
            .delete_stream(evicting)
            .inspect(move |deleted| {
                if let Ok(p) = deleted {
                    hold.release(p.as_ref());
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.remembering(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.remembering(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let listed = self.inner.list_with_delimiter(prefix).await?;
        for m in &listed.objects {
            remember(&self.ns, &self.cacheable, m);
        }
        Ok(listed)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let _hold = self.hold(&[to]).await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let _hold = self.hold(&[from, to]).await;
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use futures::TryStreamExt;
    use object_store::memory::InMemory;

    use super::*;
    use crate::NodeCacheConfig;

    #[tokio::test]
    async fn remembered_sizes_are_bounded() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = NodeCache::open(NodeCacheConfig::new(tmp.path(), 1 << 30))
            .await
            .expect("open");
        let inner = Arc::new(InMemory::new());
        let bound = 65_536;
        let n = bound + 4_464;
        for i in 0..n {
            inner
                .put(
                    &Path::from(format!("t/obj-{i:06}")),
                    PutPayload::from_static(b"x"),
                )
                .await
                .expect("put");
        }
        let store = cache.wrap("s3://bucket/", inner, Arc::new(|_| true));
        let listed = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list");
        assert_eq!(listed.len(), n);
        let ns = cache.namespace_for_test("s3://bucket/");
        let mut metas = ns.sizes.lock();
        assert_eq!(bound, MAX_SIZES);
        // Exactly the bound is kept: the `bound` most recently listed objects
        // (the in-memory store lists in path order), none listed before them.
        assert_eq!(metas.0.len(), bound, "sizes remembered");
        let name = |i: usize| Path::from(format!("t/obj-{i:06}"));
        for i in [n - bound - 1, n - bound - 2, 0] {
            assert!(metas.get(&name(i)).is_none(), "obj-{i:06} kept");
        }
        for i in [n - bound, n - bound + 1, n - bound / 2, n - 1] {
            assert_eq!(
                metas.get(&name(i)).map(|m| m.size),
                Some(1),
                "obj-{i:06} dropped"
            );
        }
    }
}
