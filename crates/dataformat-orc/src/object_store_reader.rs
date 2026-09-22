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
use datafusion::parquet::arrow::async_reader::ObjectVersionType;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use object_store::{GetOptions, GetRange, ObjectMeta, ObjectStore, ObjectStoreExt};
use orc_rust::reader::AsyncChunkReader;

/// `(if_match, version)` for a `GetOptions` pin.
///
/// Mirrors `ParquetObjectReader`: a `Version` pin with no version id uses the
/// listed `ETag`. Unversioned buckets never carry a version id; without that
/// fallback those reads are unpinned and a replacement mid-scan is decoded as
/// a mixture of footer and stripes.
fn pin_for_object(
    object_versioning_type: Option<&ObjectVersionType>,
    meta: &ObjectMeta,
) -> (Option<String>, Option<String>) {
    match object_versioning_type {
        Some(ObjectVersionType::ETag) => (meta.e_tag.clone(), None),
        Some(ObjectVersionType::Version) => match meta.version.clone() {
            Some(version) => (None, Some(version)),
            None => (meta.e_tag.clone(), None),
        },
        None => (None, None),
    }
}

/// Range-reads an ORC object so `orc-rust` can fetch the file tail and stripes
/// without pulling the whole file into memory.
///
/// When [`ObjectVersionType`] is set, every fetch is a bounded `get_opts` that
/// names the listed version, or sends `If-Match` when only an `ETag` is available.
/// Bounded ranges are required: Azure Blob Storage does not serve suffix ranges.
pub(crate) struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    file: ObjectMeta,
    object_versioning_type: Option<ObjectVersionType>,
}

impl ObjectStoreReader {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        file: ObjectMeta,
        object_versioning_type: Option<ObjectVersionType>,
    ) -> Self {
        Self {
            store,
            file,
            object_versioning_type,
        }
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
        let range = offset_from_start..end;
        let (if_match, version) = pin_for_object(self.object_versioning_type.as_ref(), &self.file);
        if if_match.is_some() || version.is_some() {
            let opts = GetOptions {
                range: Some(GetRange::Bounded(range)),
                if_match,
                version,
                ..Default::default()
            };
            self.store
                .get_opts(&self.file.location, opts)
                .and_then(object_store::GetResult::bytes)
                .map_err(std::io::Error::other)
                .boxed()
        } else {
            self.store
                .get_range(&self.file.location, range)
                .map_err(std::io::Error::other)
                .boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        EtagRecordingStore, VersionRecordingStore, write_orc_bytes, write_two_column_batch,
    };
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;
    use object_store::path::Path;
    use orc_rust::arrow_reader::ArrowReaderBuilder;

    const VERSION: &str = "the-version-the-scan-started-from";

    async fn collect_rows(
        store: Arc<dyn ObjectStore>,
        meta: ObjectMeta,
        object_versioning_type: Option<ObjectVersionType>,
    ) -> usize {
        let reader = ObjectStoreReader::new(store, meta, object_versioning_type);
        let stream = ArrowReaderBuilder::try_new_async(reader)
            .await
            .expect("reads the ORC footer")
            .build_async();
        stream
            .try_collect::<Vec<_>>()
            .await
            .expect("reads the stripes")
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum()
    }

    #[tokio::test]
    async fn a_versioned_orc_read_pins_every_request_to_one_object_version() {
        let batch = write_two_column_batch();
        let store = Arc::new(VersionRecordingStore::new(VERSION));
        let location = Path::from("versioned.orc");
        store
            .put(&location, write_orc_bytes(&batch).into())
            .await
            .expect("stores the file");
        let meta = store.head(&location).await.expect("heads the file");
        store.forget_reads();
        let store_handle = Arc::clone(&store);

        let rows = collect_rows(
            store as Arc<dyn ObjectStore>,
            meta,
            Some(ObjectVersionType::Version),
        )
        .await;
        assert_eq!(
            rows, 3,
            "the read has to reach the stripes, not just the footer"
        );

        let reads = store_handle.reads();
        assert!(
            !reads.is_empty(),
            "the read issued no request at all, so this asserts nothing"
        );
        for options in &reads {
            assert_eq!(
                options.version.as_deref(),
                Some(VERSION),
                "a read did not pin the object version, so a replacement mid-scan is read as a \
                 mixture of both versions: {options:?}"
            );
            assert!(
                !matches!(options.range, Some(GetRange::Suffix(_))),
                "a read fell back to a suffix range, which Azure Blob Storage does not serve: \
                 {options:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_versioned_orc_read_pins_by_etag_when_the_listing_has_no_version_id() {
        let batch = write_two_column_batch();
        let store = Arc::new(EtagRecordingStore::new());
        let location = Path::from("unversioned.orc");
        store
            .put(&location, write_orc_bytes(&batch).into())
            .await
            .expect("stores the file");
        let meta = store.head(&location).await.expect("heads the file");
        let etag = meta
            .e_tag
            .clone()
            .expect("an unversioned listing still carries an ETag");
        assert!(
            meta.version.is_none(),
            "this test needs a listing with no version id"
        );
        store.forget_reads();
        let store_handle = Arc::clone(&store);

        let rows = collect_rows(
            store as Arc<dyn ObjectStore>,
            meta,
            Some(ObjectVersionType::Version),
        )
        .await;
        assert_eq!(
            rows, 3,
            "the read has to reach the stripes, not just the footer"
        );

        let reads = store_handle.reads();
        assert!(
            !reads.is_empty(),
            "the read issued no request at all, so this asserts nothing"
        );
        for options in &reads {
            assert_eq!(
                options.if_match.as_deref(),
                Some(etag.as_str()),
                "a Version pin with no version id must send If-Match, or a replacement mid-scan \
                 is read as a mixture of both generations: {options:?}"
            );
            assert!(
                options.version.is_none(),
                "must not invent a version id the listing did not have: {options:?}"
            );
            assert!(
                !matches!(options.range, Some(GetRange::Suffix(_))),
                "a read fell back to a suffix range, which Azure Blob Storage does not serve: \
                 {options:?}"
            );
        }
    }
}
