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

//! An object store with modeled remote latency, after the benchmark
//! harness's `ModeledStore`: every request waits a fixed first-byte latency,
//! and every GET's payload then crosses one shared link of fixed bandwidth.
//! The link is reserved in the order first bytes arrive, so concurrent
//! transfers queue behind each other: a GET that arrives while others are
//! transferring finishes later. HEADs transfer nothing.

use std::fmt;
use std::ops::Range;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use parking_lot::Mutex;
use tokio::time::Instant;

pub(crate) struct ModeledStore {
    pub(crate) inner: InMemory,
    first_byte: Duration,
    link_bytes_per_sec: f64,
    /// When the link is next free.
    link_free: Mutex<Instant>,
}

impl ModeledStore {
    pub(crate) fn new(first_byte: Duration, link_bytes_per_sec: f64) -> Self {
        Self {
            inner: InMemory::new(),
            first_byte,
            link_bytes_per_sec,
            link_free: Mutex::new(Instant::now()),
        }
    }

    /// Reserves the link for `bytes` after everything already on it; returns
    /// when the transfer finishes.
    fn reserve(&self, bytes: u64) -> Instant {
        #[expect(
            clippy::cast_precision_loss,
            reason = "test payloads are far below 2^53 bytes"
        )]
        let took = Duration::from_secs_f64(bytes as f64 / self.link_bytes_per_sec);
        let mut free = self.link_free.lock();
        let start = (*free).max(Instant::now());
        *free = start + took;
        *free
    }
}

impl fmt::Display for ModeledStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ModeledStore")
    }
}

impl fmt::Debug for ModeledStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ModeledStore")
    }
}

#[async_trait]
impl ObjectStore for ModeledStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        tokio::time::sleep(self.first_byte).await;
        let head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            let finish = self.reserve(result.range.end - result.range.start);
            tokio::time::sleep_until(finish).await;
        }
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            let got = self
                .get_opts(location, GetOptions::new().with_range(Some(r.clone())))
                .await?;
            out.push(got.bytes().await?);
        }
        Ok(out)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
