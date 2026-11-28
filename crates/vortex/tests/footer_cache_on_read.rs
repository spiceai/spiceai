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

//! A reader that did not write a small Vortex file retains its footer after the
//! first scan, so later scans read only the segments the segment cache does not
//! hold.

use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::{GetRange, ObjectStore};
use url::Url;
use vortex::VortexSessionDefault;
use vortex::file::{EOF_SIZE, MAX_POSTSCRIPT_SIZE};
use vortex::session::VortexSession;
use vortex_datafusion::{VortexFormat, VortexTableOptions};

use super::footer_cache_reuse::{RecordingStore, write_file};

const INITIAL_READ_SIZE: u64 = MAX_POSTSCRIPT_SIZE as u64 + EOF_SIZE as u64;
const FILE: &str = "data/part-000.vortex";
const ROWS: i64 = 4_096;

struct Reader {
    recording: Arc<RecordingStore>,
    format: Arc<VortexFormat>,
    ctx: SessionContext,
    size: u64,
}

/// A reader over one file that fits the initial tail read. The table has an
/// explicit schema and collects no statistics, so the first scan's open is the
/// first read of the file, as on a node that did not write it.
async fn reader(segment_cache_size_bytes: usize) -> Reader {
    let ids = (0..ROWS).collect::<Vec<i64>>();
    let values = ids.iter().map(|row| row * 3).collect::<Vec<_>>();
    let recording = Arc::new(RecordingStore::new());
    let store = Arc::clone(&recording) as Arc<dyn ObjectStore>;
    let size = write_file(&store, FILE, &ids, &values).await;
    assert!(
        size <= INITIAL_READ_SIZE,
        "fixture fits the initial tail read"
    );

    let format = Arc::new(VortexFormat::new_with_options(
        VortexSession::default(),
        VortexTableOptions {
            segment_cache_size_bytes: Some(segment_cache_size_bytes),
            ..Default::default()
        },
    ));
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_collect_statistics(false));
    ctx.register_object_store(
        &Url::try_from("file://").expect("file:// should parse as a URL"),
        store,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let config = ListingTableConfig::new(
        ListingTableUrl::parse("file:///data/").expect("table URL should parse"),
    )
    .with_listing_options(
        ListingOptions::new(Arc::clone(&format) as _).with_file_extension(".vortex"),
    )
    .with_schema(schema);
    ctx.register_table(
        "t",
        Arc::new(ListingTable::try_new(config).expect("build listing table")),
    )
    .expect("register table");

    Reader {
        recording,
        format,
        ctx,
        size,
    }
}

impl Reader {
    /// Scans every row and returns them with the `GET`s the scan issued.
    async fn scan(&self) -> (Vec<(i64, i64)>, Vec<(String, GetRange)>) {
        self.recording.clear_gets();
        let batches = self
            .ctx
            .sql("SELECT id, v FROM t ORDER BY id, v")
            .await
            .expect("plan ordered row scan")
            .collect()
            .await
            .expect("scan every row");
        let rows = batches
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
                ids.values()
                    .iter()
                    .copied()
                    .zip(values.values().iter().copied())
            })
            .collect();
        (rows, self.recording.gets())
    }

    fn full_file_read(&self) -> Vec<(String, GetRange)> {
        vec![(FILE.to_string(), GetRange::Bounded(0..self.size))]
    }

    /// Runs the segment cache's pending evictions and returns its entry count.
    async fn resident_segments(&self) -> u64 {
        self.format
            .segment_cache_entry_count()
            .await
            .expect("the reader has a segment cache")
    }
}

fn expected_rows() -> Vec<(i64, i64)> {
    (0..ROWS).map(|id| (id, id * 3)).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn reader_reuses_the_footer_and_tail_segments_after_first_scan() {
    let reader = reader(1024 * 1024).await;

    let (rows, gets) = reader.scan().await;
    eprintln!("reader first scan GETs: {gets:?}");
    assert_eq!(
        rows,
        expected_rows(),
        "the first scan returns every exact row"
    );
    assert_eq!(
        gets,
        reader.full_file_read(),
        "the first scan keeps its single tail request"
    );
    assert!(
        reader.resident_segments().await > 0,
        "the tail read's segments are cached"
    );

    for scan in 2..=3 {
        let (rows, gets) = reader.scan().await;
        eprintln!("reader scan {scan} GETs: {gets:?}");
        assert_eq!(rows, expected_rows(), "scan {scan} returns every exact row");
        assert_eq!(
            gets,
            Vec::new(),
            "scan {scan} serves the footer and every segment from cache"
        );
    }
}

/// A segment cache that admits nothing must not make a cached footer cost more
/// requests than the tail read it replaces.
#[tokio::test(flavor = "multi_thread")]
async fn reader_without_resident_segments_reads_only_segment_bytes() {
    // Every segment outweighs a one-byte budget, so none stays resident.
    let reader = reader(1).await;

    let (rows, gets) = reader.scan().await;
    eprintln!("evicting reader first scan GETs: {gets:?}");
    assert_eq!(
        rows,
        expected_rows(),
        "the first scan returns every exact row"
    );
    assert_eq!(
        gets,
        reader.full_file_read(),
        "the first scan keeps its single tail request"
    );
    for scan in 2..=3 {
        assert_eq!(
            reader.resident_segments().await,
            0,
            "no segment is resident before scan {scan}"
        );
        let (rows, gets) = reader.scan().await;
        eprintln!("evicting reader scan {scan} GETs: {gets:?}");
        assert_eq!(rows, expected_rows(), "scan {scan} returns every exact row");
        assert_eq!(gets.len(), 1, "scan {scan} issues one request: {gets:?}");
        let (path, range) = &gets[0];
        assert_eq!(path, FILE);
        // A footer request reaches EOF; a data segment request stops before it.
        assert!(
            matches!(range, GetRange::Bounded(range) if range.end < reader.size),
            "scan {scan} reads only data segments, not the footer: {gets:?}"
        );
    }
}
