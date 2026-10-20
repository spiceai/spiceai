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

//! Real-file ordering checks for the experiment's pre-sink sorter.
#![expect(clippy::expect_used, reason = "test setup and exact row assertions")]

use crate::common;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog, experiment_set_sort_on_write};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use vortex::VortexSessionDefault;
use vortex_datafusion::VortexFormat;
use vortex_session::VortexSession;

fn files(root: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(root).expect("read data directory") {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            result.extend(files(&path));
        } else if path.extension().is_some_and(|ext| ext == "vortex") {
            result.push(path);
        }
    }
    result
}
async fn arm(enabled: bool, bulk: bool) {
    experiment_set_sort_on_write(enabled);
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let keys = vec!["a".to_string(), "b".to_string()];
    let config = VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        write_concurrency: Some(if bulk { 4 } else { 1 }),
        compaction_background_interval_ms: 0,
        compaction_trigger_files: usize::MAX,
        compaction_trigger_protected_snapshots: usize::MAX,
        compaction_trigger_snapshot_age_ms: 0,
        ..VortexConfig::default()
    };
    let options = CreateTableOptions {
        table_name: "write_order".into(),
        schema: Arc::clone(&schema),
        primary_key: keys.clone(),
        on_conflict: (!bulk).then(|| OnConflict::Upsert(ColumnReference::new(keys))),
        base_path: fixture.data_path.to_string_lossy().into_owned(),
        partition_column: None,
        vortex_config: config,
    };
    let context = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
        options,
        context.runtime_env(),
    )
    .await
    .expect("create table");
    let mut expected = Vec::new();
    let mut batches = Vec::new();
    for burst in 0..3 {
        let values: Vec<i64> = (0..64).map(|i| burst * 64 + (i * 31) % 64).collect();
        let key_offset = if bulk { 0 } else { burst * 32 };
        expected.extend(
            values
                .iter()
                .map(|&i| ((i - key_offset) / 8 - 12, (i - key_offset) % 8, i)),
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    values.iter().map(|&i| (i - key_offset) / 8 - 12),
                )),
                Arc::new(Int64Array::from_iter_values(
                    values.iter().map(|&i| (i - key_offset) % 8),
                )),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .expect("unsorted burst");
        if bulk {
            batches.push(batch);
        } else {
            assert_eq!(
                common::insert_batch(&table, batch)
                    .await
                    .expect("write small upsert"),
                64
            );
        }
    }
    if bulk {
        assert_eq!(
            common::insert_batches(&table, batches)
                .await
                .expect("round-robin bulk append"),
            192
        );
    }
    check_files(&fixture.data_path, &schema, expected, enabled, bulk).await;
}

async fn check_files(
    root: &Path,
    schema: &Arc<Schema>,
    mut expected: Vec<(i64, i64, i64)>,
    enabled: bool,
    bulk: bool,
) {
    let paths = files(root);
    assert!(
        paths.len() >= if bulk { 1 } else { 3 },
        "real files must exercise the sink: {paths:?}"
    );
    let reader = SessionContext::new();
    let mut actual = Vec::new();
    let mut unsorted = 0;
    for path in paths {
        let format = Arc::new(VortexFormat::new(VortexSession::default()));
        let listing = ListingTable::try_new(
            ListingTableConfig::new(
                ListingTableUrl::parse(path.to_string_lossy().as_ref()).expect("file URL"),
            )
            .with_listing_options(ListingOptions::new(format))
            .with_schema(Arc::clone(schema)),
        )
        .expect("file listing");
        let output = reader
            .read_table(Arc::new(listing))
            .expect("file reader")
            .collect()
            .await
            .expect("read file directly");
        let mut rows = Vec::new();
        for batch in output {
            let columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|c| {
                    c.as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("Int64 output")
                })
                .collect();
            rows.extend((0..batch.num_rows()).map(|i| {
                (
                    columns[0].value(i),
                    columns[1].value(i),
                    columns[2].value(i),
                )
            }));
        }
        if rows
            .windows(2)
            .any(|pair| (pair[0].0, pair[0].1) > (pair[1].0, pair[1].1))
        {
            unsorted += 1;
        }
        actual.extend(rows);
    }
    if enabled {
        assert_eq!(
            unsorted, 0,
            "toggle ON must sort every small file: bulk={bulk}"
        );
    } else {
        assert!(
            unsorted > 0,
            "toggle OFF must preserve scrambled arrival order: bulk={bulk}"
        );
    }
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        actual, expected,
        "exact physical row multiset: enabled={enabled}, bulk={bulk}"
    );
}

struct Restore;
impl Drop for Restore {
    fn drop(&mut self) {
        experiment_set_sort_on_write(true);
    }
}

#[test]
fn primary_key_write_ordering() {
    if !std::env::args().any(|arg| {
        arg == "primary_key_write_ordering"
            || arg == "write_sort_ordering_test::primary_key_write_ordering"
    }) {
        common::require_process_per_test("ordering test changes the writer experiment toggle");
    }
    let _restore = Restore;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("ordering test runtime")
        .block_on(async {
            for enabled in [false, true] {
                for bulk in [false, true] {
                    arm(enabled, bulk).await;
                }
            }
        });
}
