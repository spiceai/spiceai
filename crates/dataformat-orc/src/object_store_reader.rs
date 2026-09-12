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

//! [`orc_rust::reader::AsyncChunkReader`] over [`object_store::ObjectStore`].

use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use orc_rust::reader::AsyncChunkReader;

/// Range-reads an ORC object so `orc-rust` can fetch the file tail and stripes
/// without pulling the whole file into memory.
pub(crate) struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    file: ObjectMeta,
}

impl ObjectStoreReader {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, file: ObjectMeta) -> Self {
        Self { store, file }
    }
}

impl AsyncChunkReader for ObjectStoreReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        futures::future::ok(self.file.size).boxed()
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        let end = offset_from_start.saturating_add(length);
        self.store
            .get_range(&self.file.location, offset_from_start..end)
            .map_err(std::io::Error::other)
            .boxed()
    }
}
