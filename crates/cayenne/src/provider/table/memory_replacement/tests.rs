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

use super::*;
use arrow::array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use data_components::cdc::mutation::SetKey;
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;

use crate::metadata::{CreateTableOptions, VortexConfig};
use crate::provider::CayenneTableProviderBuilder;
use crate::{CayenneCatalog, MetadataCatalog};

#[tokio::test]
async fn scoped_memory_replacement_retains_segments_and_refuses_before_publication() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let catalog = Arc::new(
        CayenneCatalog::new(format!(
            "sqlite://{}",
            directory.path().join("catalog.db").display()
        ))
        .expect("catalog"),
    );
    catalog.init().await.expect("initialize catalog");
    let context = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("scope", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let table = CayenneTableProviderBuilder::new(
        catalog as Arc<dyn MetadataCatalog>,
        context.runtime_env(),
    )
    .create(CreateTableOptions {
        table_name: "scoped_memory".into(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: directory.path().join("data").to_string_lossy().into_owned(),
        partition_column: None,
        vortex_config: VortexConfig {
            memory_mode: true,
            cdc_mem_tier_max_bytes: 4096,
            ..VortexConfig::default()
        },
    })
    .await
    .expect("memory table");
    let replacement = |scope: &str, ids: Vec<i64>| {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![scope; ids.len()])),
                Arc::new(Int64Array::from(ids)),
            ],
        )
        .expect("member batch");
        ReplaceSet::try_new(
            SetKey::try_new(
                Arc::clone(&schema),
                vec![("scope".into(), ScalarValue::Utf8(Some(scope.into())))],
            )
            .expect("scope key"),
            vec![batch],
        )
        .expect("complete members")
    };
    for scope in ["cold", "hot"] {
        table
            .write_keyless_memory_replacements(vec![replacement(scope, vec![1, 1])])
            .await
            .expect("seed")
            .finish()
            .await
            .expect("seed completion");
    }
    let before = table.mem_tier.shard(0).load_full();
    let cold = Arc::clone(&before.segments[0].batches);
    let mut epoch = before.epoch;
    for id in 2..22 {
        let write = table
            .write_keyless_memory_replacements(vec![replacement("hot", vec![id, id])])
            .await
            .expect("replace hot group");
        assert_eq!(write.in_memory_epoch(), Some(epoch + 1));
        write.finish().await.expect("publication");
        epoch += 1;
        let current = table.mem_tier.shard(0).load_full();
        assert_eq!(current.segments.len(), 2, "discard empty segment metadata");
        assert_eq!(current.rows, 4);
        assert!(Arc::ptr_eq(&cold, &current.segments[0].batches));
    }
    assert_eq!(before.rows, 4);
    assert_eq!(before.epoch + 20, epoch);
    let before_refusal = table.mem_tier.shard(0).load_full();
    let error = table
        .write_keyless_memory_replacements(vec![replacement("hot", vec![99; 4096])])
        .await
        .err()
        .expect("refuse oversized replacement");
    assert!(error.to_string().contains("memory"), "{error}");
    assert!(Arc::ptr_eq(
        &before_refusal,
        &table.mem_tier.shard(0).load_full()
    ));
    table
        .drain_in_flight_maintenance()
        .await
        .expect("drain fixture");
}
