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

use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use object_store::{ObjectMeta, ObjectStoreExt, PutPayload, path::Path};
use orc_rust::arrow_writer::ArrowWriterBuilder;

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
