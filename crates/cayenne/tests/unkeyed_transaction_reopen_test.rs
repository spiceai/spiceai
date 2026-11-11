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

//! A transactional write to a table without a primary key, with no
//! `retention_sql`, survives a reopen.
//!
//! Fails at the upstream merge-base 9aa41988c as well (run 2026-11-11: live
//! `[1, 10]`, reopen `[1]`): `load_protected_snapshots` loads no protected
//! snapshot for a position-based table, while a transaction commits such a
//! table's rows as one. Kept enabled as the regression for that repair.

use crate::common;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{
    CayenneTableProvider, CayenneTableProviderBuilder, CayenneTransaction, MetadataCatalog,
};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;

test_with_backends!(unkeyed_transaction_rows_survive_reopen_impl);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]))
}

fn rows(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec![Some("a"); ids.len()])),
        ],
    )
    .expect("batch")
}

async fn ids(provider: &Arc<CayenneTableProvider>) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(provider) as Arc<dyn TableProvider>)?;
    let batches = ctx
        .sql("SELECT id FROM t ORDER BY id")
        .await?
        .collect()
        .await?;
    let mut out = Vec::new();
    for b in &batches {
        let c = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        out.extend((0..c.len()).map(|i| c.value(i)));
    }
    Ok(out)
}

async fn unkeyed_transaction_rows_survive_reopen_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = "unkeyed_txn_reopen";
    let dir = fixture.data_path.join(name);
    std::fs::create_dir_all(&dir)?;
    let catalog = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let t = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&catalog), SessionContext::new().runtime_env())
            .create(CreateTableOptions {
                table_name: name.to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig {
                    inline_max_rows: 0,
                    inline_max_bytes: 0,
                    inline_max_buffer_bytes: 0,
                    ..VortexConfig::default()
                },
            })
            .await?,
    );
    common::insert_batch(t.as_ref(), rows(&[1])).await?;
    let txn = CayenneTransaction::new();
    let token = t.transaction_write_token().await;
    txn.register(
        t.table_id().to_string(),
        token,
        t.clone_for_write_operations(),
    );
    let staged = t
        .begin_staged_upsert_occ(token, common::single_batch_stream(rows(&[10])), 1)
        .await?;
    txn.set_staged(t.table_id(), staged);
    txn.commit().await?;
    assert_eq!(
        ids(&t).await?,
        vec![1, 10],
        "live after the transaction commits"
    );
    drop(t);
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(catalog, SessionContext::new().runtime_env())
            .open(name)
            .await?,
    );
    assert_eq!(
        ids(&reopened).await?,
        vec![1, 10],
        "a committed transaction's rows survive a reopen"
    );
    Ok(())
}
