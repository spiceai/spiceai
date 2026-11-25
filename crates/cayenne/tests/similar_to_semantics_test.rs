/*
Copyright 2025 The Spice.ai OSS Authors

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

//! `SIMILAR TO` is a SQL pattern, not an unanchored regex.
//!
//! `DataFusion` 55 handed the pattern to the regex kernel unchanged, so `%` and `_`
//! never matched as wildcards and a pattern matched anywhere in the string. A
//! Cayenne scan accepts a `SIMILAR TO` predicate only as a filter above the scan,
//! so the answer from a Cayenne table must be the one SQL defines. The fork patch
//! is spiceai/datafusion#306 (apache/datafusion#23188).

use crate::common;

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::SessionContext;

test_with_backends!(similar_to_matches_the_whole_string_with_sql_wildcards_impl);

async fn similar_to_matches_the_whole_string_with_sql_wildcards_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let options = CreateTableOptions {
        table_name: "segments".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig::default(),
    };
    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(catalog, options, ctx.runtime_env()).await?;
    common::insert_batch(
        &table,
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                Some("AUTOMOBILE"),
                Some("BUILDING"),
                Some("FURNITURE"),
                Some("MACHINERY"),
                Some("HOUSEHOLD"),
                None,
            ]))],
        )?,
    )
    .await?;
    ctx.register_table("segments", Arc::new(table))?;

    let cases: [(&str, &[&str]); 5] = [
        ("s SIMILAR TO '(AUTO|BUILD)%'", &["AUTOMOBILE", "BUILDING"]),
        (
            "s NOT SIMILAR TO '(AUTO|BUILD)%'",
            &["FURNITURE", "HOUSEHOLD", "MACHINERY"],
        ),
        ("s SIMILAR TO 'MACHIN'", &[]),
        ("s SIMILAR TO '%URE'", &["FURNITURE"]),
        ("s SIMILAR TO 'HOUSEHOL_'", &["HOUSEHOLD"]),
    ];
    for (predicate, expected) in cases {
        let batches = ctx
            .sql(&format!("SELECT s FROM segments WHERE {predicate}"))
            .await?
            .collect()
            .await?;
        let mut rows = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                rows.push(array_value_to_string(batch.column(0), row)?);
            }
        }
        rows.sort_unstable();
        assert_eq!(rows, expected, "segments WHERE {predicate}");
    }
    Ok(())
}
