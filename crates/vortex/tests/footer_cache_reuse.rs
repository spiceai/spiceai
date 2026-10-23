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

//! Large-file scans retain their footers to avoid repeated footer requests.
//! Files that fit in the initial footer read retain a single full-file request
//! per scan, including when the segment cache is disabled.

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
use vortex::file::{EOF_SIZE, MAX_POSTSCRIPT_SIZE, WriteOptionsSessionExt};
use vortex::io::VortexWrite;
use vortex::io::object_store::ObjectStoreWrite;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormatFactory;

const INITIAL_READ_SIZE: u64 = MAX_POSTSCRIPT_SIZE as u64 + EOF_SIZE as u64;

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

async fn write_file(store: &Arc<dyn ObjectStore>, name: &str, ids: &[i64], values: &[i64]) -> u64 {
    let session = VortexSession::default();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(ids.iter().copied())),
            Arc::new(Int64Array::from_iter_values(values.iter().copied())),
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

async fn aggregate(ctx: &SessionContext) -> (i64, i64) {
    let batches = ctx
        .sql("SELECT count(*) AS n, sum(v) AS s FROM t WHERE id >= 0")
        .await
        .expect("plan aggregate")
        .collect()
        .await
        .expect("run aggregate");
    assert_eq!(batches.len(), 1, "aggregate returns one batch");
    assert_eq!(batches[0].num_rows(), 1, "aggregate returns one row");
    let value = |column| {
        batches[0]
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("aggregate is i64")
            .value(0)
    };
    (value(0), value(1))
}

async fn assert_exact_rows(ctx: &SessionContext, ids: &[i64], values: &[i64]) {
    let batches = ctx
        .sql("SELECT id, v FROM t WHERE id >= 0 ORDER BY id, v")
        .await
        .expect("plan rows")
        .collect()
        .await
        .expect("read rows");
    let actual = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is i64");
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("v is i64");
            ids.iter().zip(values.iter())
        })
        .collect::<Vec<_>>();
    let mut expected = ids
        .iter()
        .zip(values)
        .map(|(id, value)| (Some(*id), Some(*value)))
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(actual, expected, "every row and value matches the fixture");
}

async fn setup(ids: &[i64], values: &[i64]) -> (Arc<RecordingStore>, SessionContext, u64) {
    let recording = Arc::new(RecordingStore {
        inner: InMemory::new(),
        gets: Mutex::new(Vec::new()),
    });
    let store = Arc::clone(&recording) as Arc<dyn ObjectStore>;
    let size = write_file(&store, "data/part-000.vortex", ids, values).await;

    // The ordinary factory has no segment cache unless one is requested.
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

    (recording, ctx, size)
}

#[tokio::test(flavor = "multi_thread")]
async fn small_file_scans_keep_one_full_file_read() {
    let ids = (0..4_096).collect::<Vec<i64>>();
    let values = ids.iter().map(|row| row * 3).collect::<Vec<_>>();
    let (recording, ctx, size) = setup(&ids, &values).await;
    assert!(size <= INITIAL_READ_SIZE, "fixture fits the initial read");
    let expected = (
        i64::try_from(values.len()).expect("row count fits i64"),
        values.iter().sum::<i64>(),
    );

    for scan in 1..=2 {
        recording.gets.lock().clear();
        assert_eq!(aggregate(&ctx).await, expected);
        let gets = recording.gets.lock().clone();
        eprintln!("small-file scan {scan} GETs: {gets:?}");
        assert_eq!(
            gets.len(),
            1,
            "one request supplies the whole file: {gets:?}"
        );
        let full_file = match &gets[0].1 {
            GetRange::Bounded(range) => range.start == 0 && range.end == size,
            GetRange::Offset(offset) => *offset == 0,
            GetRange::Suffix(length) => *length >= size,
        };
        assert!(full_file, "the request includes every file byte: {gets:?}");
    }
    assert_exact_rows(&ctx, &ids, &values).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_file_second_scan_reuses_the_footer() {
    let ids = (0..32_768).collect::<Vec<i64>>();
    let values = random_values(32_768, 0);
    let (recording, ctx, size) = setup(&ids, &values).await;
    assert!(size > INITIAL_READ_SIZE, "fixture exceeds the initial read");
    let expected = (
        i64::try_from(values.len()).expect("row count fits i64"),
        values.iter().sum::<i64>(),
    );

    // A footer request reaches EOF; data segment requests stop before it.
    let footer_reads = |gets: &[(String, GetRange)]| {
        gets.iter()
            .filter(|(_, range)| match range {
                GetRange::Bounded(range) => range.end >= size,
                GetRange::Offset(_) | GetRange::Suffix(_) => true,
            })
            .count()
    };

    recording.gets.lock().clear();
    assert_eq!(aggregate(&ctx).await, expected);
    let first = recording.gets.lock().clone();
    eprintln!("large-file first scan GETs: {first:?}");
    assert_eq!(
        footer_reads(&first),
        1,
        "first scan reads the footer: {first:?}"
    );

    recording.gets.lock().clear();
    assert_eq!(aggregate(&ctx).await, expected);
    let second = recording.gets.lock().clone();
    eprintln!("large-file second scan GETs: {second:?}");
    assert_eq!(
        footer_reads(&second),
        0,
        "second scan reuses the footer: {second:?}"
    );
    assert_exact_rows(&ctx, &ids, &values).await;
}

fn random_values(rows: u64, seed: u64) -> Vec<i64> {
    // Mix each row into 40 bits so the column does not compress to a constant
    // or sequence. Bounded positive values keep the aggregate in i64.
    (0..rows)
        .map(|row| {
            let mut value = (row ^ seed).wrapping_add(0x9e37_79b9_7f4a_7c15);
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            i64::try_from((value ^ (value >> 31)) & ((1_u64 << 40) - 1))
                .expect("40-bit value fits i64")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn initial_tail_read_keeps_prefetched_column_data() {
    let ids = random_values(12_000, 0);
    let values = random_values(12_000, 1 << 32);
    let (recording, ctx, size) = setup(&ids, &values).await;
    assert!(size > INITIAL_READ_SIZE, "fixture exceeds the initial read");
    assert!(size < 2 * INITIAL_READ_SIZE, "one column fits in the tail");
    let expected = (
        i64::try_from(values.len()).expect("row count fits i64"),
        values.iter().sum::<i64>(),
    );

    for scan in 1..=2 {
        recording.gets.lock().clear();
        assert_eq!(aggregate(&ctx).await, expected);
        let gets = recording.gets.lock().clone();
        eprintln!("partial-file scan {scan} GETs: {gets:?}");
        assert_eq!(gets.len(), 2, "tail read supplies one column: {gets:?}");
        assert!(
            gets.iter().any(|(_, range)| matches!(
                range,
                GetRange::Bounded(range)
                    if range.start == size - INITIAL_READ_SIZE && range.end == size
            )),
            "scan retains the initial tail read: {gets:?}"
        );
    }
    assert_exact_rows(&ctx, &ids, &values).await;
}
