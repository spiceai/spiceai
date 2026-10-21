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

//! Test helpers for writing ORC fixtures and placing them in an object store.

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use orc_rust::arrow_writer::ArrowWriterBuilder;

/// Two columns so stripe reads are plural rather than a single footer fetch.
pub(crate) fn write_two_column_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("builds a two-column batch")
}

/// Encode `batch` as a single in-memory ORC file.
pub(crate) fn write_orc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut out = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut out, batch.schema())
        .try_build()
        .expect("construct ORC writer");
    writer.write(batch).expect("write ORC batch");
    writer.close().expect("close ORC writer");
    out
}

/// Store `bytes` at `path` in `store` and return the resulting object metadata.
pub(crate) async fn put_orc(store: &InMemory, path: &str, bytes: Vec<u8>) -> ObjectMeta {
    let location = Path::from(path);
    store
        .put(&location, PutPayload::from_bytes(bytes.into()))
        .await
        .expect("put ORC object");
    store.head(&location).await.expect("head ORC object")
}

/// An `InMemory` store that records the [`object_store::GetOptions`] of every
/// read, and reports a version for the object it holds so a reader has something
/// to pin to.
///
/// `InMemory` itself reports no version, and a reader cannot pin what the store
/// does not give it — which would make a pin assertion pass whether or not the
/// pinning worked.
#[derive(Debug)]
pub(crate) struct VersionRecordingStore {
    inner: InMemory,
    version: String,
    reads: std::sync::Mutex<Vec<object_store::GetOptions>>,
}

impl VersionRecordingStore {
    pub(crate) fn new(version: &str) -> Self {
        Self {
            inner: InMemory::new(),
            version: version.to_string(),
            reads: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn reads(&self) -> Vec<object_store::GetOptions> {
        self.reads.lock().expect("reads lock").clone()
    }

    /// Forget what has been recorded, so a test can set itself up through the
    /// same store it is about to assert on.
    pub(crate) fn forget_reads(&self) {
        self.reads.lock().expect("reads lock").clear();
    }
}

impl std::fmt::Display for VersionRecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VersionRecordingStore")
    }
}

#[async_trait]
impl ObjectStore for VersionRecordingStore {
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.reads.lock().expect("reads lock").push(options.clone());
        let mut result = self.inner.get_opts(location, options).await?;
        result.meta.version = Some(self.version.clone());
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Like [`VersionRecordingStore`], but the listing has an `ETag` and no version
/// id — the unversioned-bucket shape.
#[derive(Debug)]
pub(crate) struct EtagRecordingStore {
    inner: InMemory,
    reads: std::sync::Mutex<Vec<object_store::GetOptions>>,
}

impl EtagRecordingStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: InMemory::new(),
            reads: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn reads(&self) -> Vec<object_store::GetOptions> {
        self.reads.lock().expect("reads lock").clone()
    }

    pub(crate) fn forget_reads(&self) {
        self.reads.lock().expect("reads lock").clear();
    }
}

impl std::fmt::Display for EtagRecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EtagRecordingStore")
    }
}

#[async_trait]
impl ObjectStore for EtagRecordingStore {
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.reads.lock().expect("reads lock").push(options.clone());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
