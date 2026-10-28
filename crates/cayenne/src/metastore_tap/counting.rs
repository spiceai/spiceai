// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, path::Path,
};
use parking_lot::Mutex;
use std::{collections::BTreeMap, ops::Range, sync::Arc, time::Instant};

#[derive(Debug)]
pub(super) struct CountingStore {
    pub inner: Arc<dyn ObjectStore>,
    counts: Arc<Mutex<BTreeMap<String, u64>>>,
    pub accepted: Mutex<Option<Instant>>,
}
impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            counts: Arc::new(Mutex::new(
                ["creates", "cas", "gets", "list", "deletes", "puts", "copy"]
                    .into_iter()
                    .map(|k| (k.to_string(), 0))
                    .collect(),
            )),
            accepted: Mutex::new(None),
        }
    }
    fn bump(&self, kind: &str) {
        *self.counts.lock().entry(kind.to_string()).or_default() += 1;
    }
    pub fn take_counts(&self) -> BTreeMap<String, u64> {
        let mut counts = self.counts.lock();
        let result = counts.clone();
        for count in counts.values_mut() {
            *count = 0;
        }
        result
    }
}
impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shadow({})", self.inner)
    }
}
#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.bump(match opts.mode {
            PutMode::Create => "creates",
            PutMode::Update(_) => "cas",
            PutMode::Overwrite => "puts",
        });
        let is_cas = matches!(opts.mode, PutMode::Update(_));
        let result = self.inner.put_opts(location, payload, opts).await;
        if is_cas && result.is_ok() {
            *self.accepted.lock() = Some(std::time::Instant::now());
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.bump("puts");
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.bump("gets");
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.bump("gets");
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let counts = Arc::clone(&self.counts);
        self.inner.delete_stream(
            locations
                .map(move |item| {
                    *counts.lock().entry("deletes".to_string()).or_default() += 1;
                    item
                })
                .boxed(),
        )
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.bump("list");
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.bump("list");
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.bump("list");
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.bump("copy");
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.bump("copy");
        self.inner.rename_opts(from, to, options).await
    }
}
