/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! A scan that opens a Vortex file must keep the footer it read, so the next
//! open of that file (another split, or a later query) does not read the
//! footer from the object store again. Files on a remote store pay a
//! round trip per footer read, so a footer that is re-read on every open makes
//! every query pay that latency again.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::provider::DefaultTableFactory;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_common::GetExt;
use futures::stream::BoxStream;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parking_lot::Mutex;
use url::Url;
use vortex::VortexSessionDefault;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::arrow::ArrowSessionExt;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::VortexWrite;
use vortex::io::object_store::ObjectStoreWrite;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormatFactory;

const ROWS: i64 = 4_096;

/// Records the byte range of every `GET`, as `(start, end)` with `None` for an
/// open end.
#[derive(Debug)]
struct RecordingStore {
    inner: InMemory,
    gets: Mutex<Vec<(String, GetRange)>>,
}

impl fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecordingStore")
    }
}

#[async_trait]
impl ObjectStore for RecordingStore {
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
        if let Some(range) = options.range.clone() {
            self.gets.lock().push((location.to_string(), range));
        }
        self.inner.get_opts(location, options).await
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

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
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

async fn write_file(store: &Arc<dyn ObjectStore>, name: &str) -> u64 {
    let session = VortexSession::default();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS)),
            Arc::new(Int64Array::from_iter_values((0..ROWS).map(|r| r * 3))),
        ],
    )
    .expect("batch matches schema");
    let array = session
        .arrow()
        .from_arrow_record_batch(batch, &schema)
        .expect("arrow batch converts to vortex");
    let dtype = array.dtype().clone();
    let stream = ArrayStreamAdapter::new(dtype, futures::stream::iter([Ok(array)]));
    let path = Path::from(name);
    let mut writer = ObjectStoreWrite::new(Arc::clone(store), &path)
        .await
        .expect("open vortex file for writing");
    session
        .write_options()
        .write(&mut writer, stream)
        .await
        .expect("write vortex file");
    writer.shutdown().await.expect("finish vortex file");
    store.head(&path).await.expect("head written file").size
}

async fn count(ctx: &SessionContext) -> i64 {
    let batches = ctx
        .sql("SELECT sum(v) AS s FROM t WHERE id >= 0")
        .await
        .expect("plan query")
        .collect()
        .await
        .expect("run query");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum is i64")
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_scan_does_not_re_read_the_footer() {
    let recording = Arc::new(RecordingStore {
        inner: InMemory::new(),
        gets: Mutex::new(Vec::new()),
    });
    let store: Arc<dyn ObjectStore> = recording.clone();
    let size = write_file(&store, "data/part-000.vortex").await;

    let factory = Arc::new(VortexFormatFactory::new());
    let mut builder = SessionStateBuilder::new()
        .with_default_features()
        .with_table_factory(
            factory.get_ext().to_uppercase(),
            Arc::new(DefaultTableFactory::new()),
        )
        .with_object_store(
            &Url::try_from("file://").expect("file:// should parse as a URL"),
            Arc::clone(&store),
        );
    if let Some(file_formats) = builder.file_formats() {
        file_formats.push(factory as _);
    }
    let ctx = SessionContext::new_with_state(builder.build());
    ctx.sql("CREATE EXTERNAL TABLE t STORED AS VORTEX LOCATION '/data/'")
        .await
        .expect("create table");

    // Schema inference caches the footer; drop it so the scans below start from
    // the state of a file whose footer nothing has cached.
    ctx.runtime_env()
        .cache_manager
        .get_file_metadata_cache()
        .clear();

    // A footer read reaches the end of the file; segment reads stop short of
    // the footer.
    let footer_reads = |gets: &[(String, GetRange)]| {
        gets.iter()
            .filter(|(_, range)| match range {
                GetRange::Bounded(r) => r.end >= size,
                GetRange::Offset(_) | GetRange::Suffix(_) => true,
            })
            .count()
    };

    let expected: i64 = (0..ROWS).map(|r| r * 3).sum();

    recording.gets.lock().clear();
    assert_eq!(count(&ctx).await, expected);
    let first = recording.gets.lock().clone();
    eprintln!("first scan GETs: {first:?}");

    recording.gets.lock().clear();
    assert_eq!(count(&ctx).await, expected);
    let second = recording.gets.lock().clone();
    eprintln!("second scan GETs: {second:?}");

    assert_eq!(
        footer_reads(&first),
        1,
        "the first scan reads the footer once: {first:?}"
    );
    assert_eq!(
        footer_reads(&second),
        0,
        "the second scan must reuse the footer the first one read: {second:?}"
    );
}
