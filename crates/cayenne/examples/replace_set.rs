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

//! Exercise complete-set replacement against real memory, inline and file storage.
//! Supply a nonexistent directory to retain the catalog and Vortex files.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::provider::{CayenneTableProvider, CayenneTableProviderBuilder};
use cayenne::{CayenneCatalog, MetadataCatalog};
use data_components::cdc::mutation::{Recovery, ReplaceSet, SetKey};
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::ScalarValue;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use serde::Serialize;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct Row {
    tenant: String,
    group: Option<String>,
    id: i64,
}

fn members(tenant: &str, group: Option<&str>, ids: &[i64]) -> Vec<Row> {
    ids.iter()
        .map(|&id| Row {
            tenant: tenant.into(),
            group: group.map(str::to_owned),
            id,
        })
        .collect()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("grp", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
    ]))
}

fn batch(rows: &[Row]) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.tenant.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.group.as_deref()),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.id))),
        ],
    )?)
}

fn key(tenant: &str, group: Option<&str>) -> Result<SetKey> {
    Ok(SetKey::try_new(
        schema(),
        vec![
            ("tenant".into(), ScalarValue::Utf8(Some(tenant.into()))),
            ("grp".into(), ScalarValue::Utf8(group.map(str::to_owned))),
        ],
    )?)
}

async fn replace(
    table: &CayenneTableProvider,
    ctx: &SessionContext,
    tenant: &str,
    group: Option<&str>,
    ids: &[i64],
) -> Result<()> {
    let incoming = members(tenant, group, ids);
    let batches = if incoming.is_empty() {
        Vec::new()
    } else {
        vec![batch(&incoming)?]
    };
    let write = table
        .write_replace_set(
            ReplaceSet::try_new(key(tenant, group)?, batches)?,
            Recovery::Rebuildable,
            ctx,
        )
        .await?;
    if table.is_memory_resident_mode() && write.in_memory_epoch().is_none() {
        return Err("resident replacement did not publish through the shared RAM tier".into());
    }
    if write.rows() != ids.len() as u64 || write.finish().await? != ids.len() as u64 {
        return Err("replacement receipt row count differs from input".into());
    }
    Ok(())
}

fn decode(batches: Vec<RecordBatch>) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for batch in batches {
        for i in 0..batch.num_rows() {
            let ScalarValue::Utf8(Some(tenant)) =
                ScalarValue::try_from_array(batch.column(0), i)?.cast_to(&DataType::Utf8)?
            else {
                return Err("invalid tenant value".into());
            };
            let ScalarValue::Utf8(group) =
                ScalarValue::try_from_array(batch.column(1), i)?.cast_to(&DataType::Utf8)?
            else {
                return Err("invalid group value".into());
            };
            let ScalarValue::Int64(Some(id)) = ScalarValue::try_from_array(batch.column(2), i)?
            else {
                return Err("invalid id value".into());
            };
            rows.push(Row { tenant, group, id });
        }
    }
    rows.sort();
    Ok(rows)
}

async fn read(ctx: &SessionContext, table: &Arc<CayenneTableProvider>) -> Result<Vec<Row>> {
    decode(
        ctx.read_table(Arc::clone(table) as Arc<dyn TableProvider>)?
            .collect()
            .await?,
    )
}

fn check(case: &str, actual: Vec<Row>, mut expected: Vec<Row>) -> Result<()> {
    expected.sort();
    println!(
        "{}",
        serde_json::json!({"case": case, "actual": actual, "expected": expected})
    );
    if actual != expected {
        return Err(format!("complete-set mismatch: {case}").into());
    }
    Ok(())
}

async fn exercise_coalescing(
    name: &str,
    ctx: &SessionContext,
    table: &Arc<CayenneTableProvider>,
    keyed: bool,
    outside: &[Row],
) -> Result<()> {
    let make = |second: bool| -> Result<(Vec<ReplaceSet>, Vec<Row>)> {
        let mut replacements = Vec::new();
        let mut expected = outside.to_vec();
        for (index, group) in ["left", "right"].into_iter().enumerate() {
            let (scope, rows) = if keyed {
                let id = 1000 + index as i64;
                (
                    SetKey::try_new(schema(), vec![("id".into(), ScalarValue::Int64(Some(id)))])?,
                    members(
                        if second { "batch-new" } else { "batch" },
                        Some(group),
                        &[id],
                    ),
                )
            } else {
                let ids = match (second, index) {
                    (false, 0) => vec![1000, 1000],
                    (false, _) => vec![1001, 1002],
                    (true, 0) => vec![1003],
                    (true, _) => vec![],
                };
                (
                    key("batch", Some(group))?,
                    members("batch", Some(group), &ids),
                )
            };
            replacements.push(ReplaceSet::try_new(scope, vec![batch(&rows)?])?);
            expected.extend(rows);
        }
        expected.sort();
        Ok((replacements, expected))
    };
    let (initial, before) = make(false)?;
    if !table.can_coalesce_replace_sets(&initial[0], &initial[1]) {
        return Err("disjoint fixture scopes cannot coalesce".into());
    }
    table
        .write_replace_sets(initial, Recovery::Rebuildable, ctx)
        .await?
        .finish()
        .await?;
    check(
        &format!("{name}:coalesced-initial"),
        read(ctx, table).await?,
        before.clone(),
    )?;
    let held = ctx
        .read_table(Arc::clone(table) as Arc<dyn TableProvider>)?
        .create_physical_plan()
        .await?;
    let (next, after) = make(true)?;
    let write = table
        .write_replace_sets(next, Recovery::Rebuildable, ctx)
        .await?;
    if (keyed || table.is_memory_resident_mode()) && write.in_memory_epoch().is_none() {
        return Err("replacement batch did not use native memory publication".into());
    }
    write.finish().await?;
    check(
        &format!("{name}:coalesced-held"),
        decode(datafusion_physical_plan::collect(held, ctx.task_ctx()).await?)?,
        before.clone(),
    )?;
    check(
        &format!("{name}:coalesced-next"),
        read(ctx, table).await?,
        after.clone(),
    )?;

    let (mut first, _) = make(false)?;
    let (mut second, _) = make(true)?;
    let overlapping = vec![first.remove(0), second.remove(0)];
    if table.can_coalesce_replace_sets(&overlapping[0], &overlapping[1])
        || table
            .write_replace_sets(overlapping, Recovery::Rebuildable, ctx)
            .await
            .is_ok()
    {
        return Err("overlapping replacement scopes were combined".into());
    }
    check(
        &format!("{name}:coalesced-refusal"),
        read(ctx, table).await?,
        after.clone(),
    )?;

    let stop = Arc::new(AtomicBool::new(false));
    let reader_stop = Arc::clone(&stop);
    let reader_table = Arc::clone(table);
    let reader_ctx = ctx.clone();
    let read_before = before.clone();
    let read_after = after.clone();
    let label = format!("{name}:coalesced-read");
    let (ready, started) = tokio::sync::oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut ready = Some(ready);
        let mut count = 0;
        while !reader_stop.load(Ordering::Acquire) {
            let rows = read(&reader_ctx, &reader_table).await?;
            println!("{}", serde_json::json!({"case": label, "actual": rows}));
            if rows != read_before && rows != read_after {
                return Err::<_, Box<dyn Error + Send + Sync>>(
                    format!("partial coalesced replacement: {rows:?}").into(),
                );
            }
            count += 1;
            if let Some(ready) = ready.take() {
                let _ = ready.send(());
            }
            tokio::task::yield_now().await;
        }
        Ok(count)
    });
    started.await?;
    let mut write_result = Ok(());
    for i in 0..8 {
        let (replacements, _) = make(i % 2 != 0)?;
        let result = async {
            table
                .write_replace_sets(replacements, Recovery::Rebuildable, ctx)
                .await?
                .finish()
                .await?;
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        }
        .await;
        if let Err(error) = result {
            write_result = Err(error);
            break;
        }
    }
    stop.store(true, Ordering::Release);
    let observations = reader.await??;
    write_result?;
    println!(
        "{}",
        serde_json::json!({"case": format!("{name}:coalesced-concurrent"), "complete_snapshots": observations})
    );
    check(
        &format!("{name}:coalesced-final"),
        read(ctx, table).await?,
        after,
    )?;

    if keyed {
        for id in [1000, 1001] {
            table
                .write_replace_set(
                    ReplaceSet::try_new(
                        SetKey::try_new(
                            schema(),
                            vec![("id".into(), ScalarValue::Int64(Some(id)))],
                        )?,
                        vec![],
                    )?,
                    Recovery::Rebuildable,
                    ctx,
                )
                .await?
                .finish()
                .await?;
        }
    } else {
        table
            .write_replace_sets(
                vec![
                    ReplaceSet::try_new(key("batch", Some("left"))?, vec![])?,
                    ReplaceSet::try_new(key("batch", Some("right"))?, vec![])?,
                ],
                Recovery::Rebuildable,
                ctx,
            )
            .await?
            .finish()
            .await?;
    }
    check(
        &format!("{name}:coalesced-clear"),
        read(ctx, table).await?,
        outside.to_vec(),
    )?;
    Ok(())
}

async fn exercise(root: &Path, name: &str, memory: bool, inline: bool, keyed: bool) -> Result<()> {
    let directory = root.join(name);
    tokio::fs::create_dir(&directory).await?;
    tokio::fs::create_dir(directory.join("data")).await?;
    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        directory.join("catalog.db").display()
    ))?);
    catalog.init().await?;
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::new().with_target_partitions(1),
        RuntimeEnvBuilder::new()
            .with_memory_limit(256 * 1024 * 1024, 1.0)
            .build_arc()?,
    );
    let table = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            ctx.runtime_env(),
        )
        .create(CreateTableOptions {
            table_name: name.into(),
            schema: schema(),
            primary_key: if keyed { vec!["id".into()] } else { vec![] },
            on_conflict: keyed.then(|| OnConflict::Upsert(ColumnReference::new(vec!["id".into()]))),
            base_path: directory.join("data").to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: VortexConfig {
                memory_mode: memory,
                inline_max_rows: if inline { 1000 } else { 0 },
                cdc_mem_tier_shards: 1,
                cdc_mem_tier_max_age_ms: 60_000,
                cdc_mem_tier_checkpoint_interval_ms: 60_000,
                ..VortexConfig::default()
            },
        })
        .await?,
    );
    replace(&table, &ctx, "b", Some("g"), &[90]).await?;
    replace(&table, &ctx, "a", None, &[50]).await?;
    replace(&table, &ctx, "a", Some(""), &[60]).await?;
    let mut outside = members("b", Some("g"), &[90]);
    outside.extend(members("a", None, &[50]));
    outside.extend(members("a", Some(""), &[60]));
    if keyed {
        let replacement = ReplaceSet::try_new(
            SetKey::try_new(schema(), vec![("id".into(), ScalarValue::Int64(Some(1)))])?,
            vec![batch(&members("a", Some("g"), &[1]))?],
        )?;
        let write = table
            .write_replace_set(replacement, Recovery::Rebuildable, &ctx)
            .await?;
        if write.in_memory_epoch().is_none() {
            return Err("cross-tier fixture did not seed the native memory tier".into());
        }
        write.finish().await?;
        println!(
            "{}",
            serde_json::json!({"case": format!("{name}:ram-seed"), "native_memory": true})
        );
    }
    let initial = if keyed { vec![1, 2] } else { vec![1, 2, 2] };
    replace(&table, &ctx, "a", Some("g"), &initial).await?;
    let mut old = outside.clone();
    old.extend(members("a", Some("g"), &initial));
    check(
        &format!("{name}:initial"),
        read(&ctx, &table).await?,
        old.clone(),
    )?;
    let held = ctx
        .read_table(Arc::clone(&table) as Arc<dyn TableProvider>)?
        .create_physical_plan()
        .await?;
    let short = if keyed { vec![3] } else { vec![3, 3] };
    replace(&table, &ctx, "a", Some("g"), &short).await?;
    check(
        &format!("{name}:held-snapshot"),
        decode(datafusion_physical_plan::collect(held, ctx.task_ctx()).await?)?,
        old,
    )?;
    let mut expected = outside.clone();
    expected.extend(members("a", Some("g"), &short));
    check(
        &format!("{name}:shrink"),
        read(&ctx, &table).await?,
        expected.clone(),
    )?;

    if keyed {
        let result = replace(&table, &ctx, "a", Some("g"), &[90]).await;
        if result.is_ok() {
            return Err("cross-group primary key conflict was accepted".into());
        }
        println!(
            "{}",
            serde_json::json!({"case": format!("{name}:cross-group-refusal"), "error": result.err().map(|error| error.to_string())})
        );
        check(
            &format!("{name}:refusal-preserves-old"),
            read(&ctx, &table).await?,
            expected.clone(),
        )?;
        if replace(&table, &ctx, "a", Some("g"), &[4, 4]).await.is_ok() {
            return Err("duplicate primary keys were accepted".into());
        }
        check(
            &format!("{name}:duplicate-refusal-preserves-old"),
            read(&ctx, &table).await?,
            expected.clone(),
        )?;
    }

    let large = if keyed {
        vec![4, 5, 6]
    } else {
        vec![4, 5, 5, 6]
    };
    let mut grown = outside.clone();
    grown.extend(members("a", Some("g"), &large));
    expected.sort();
    grown.sort();
    let stop = Arc::new(AtomicBool::new(false));
    let observations = Arc::new(AtomicUsize::new(0));
    let reader_table = Arc::clone(&table);
    let reader_ctx = ctx.clone();
    let reader_stop = Arc::clone(&stop);
    let reader_observations = Arc::clone(&observations);
    let before = expected.clone();
    let after = grown.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let reader = tokio::spawn(async move {
        let mut ready = Some(ready);
        while !reader_stop.load(Ordering::Acquire) {
            let rows = read(&reader_ctx, &reader_table).await?;
            if rows != before && rows != after {
                return Err::<_, Box<dyn Error + Send + Sync>>(
                    format!("partial replacement: {rows:?}").into(),
                );
            }
            reader_observations.fetch_add(1, Ordering::Relaxed);
            if let Some(ready) = ready.take() {
                let _ = ready.send(());
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    });
    started.await?;
    let mut write_result = Ok(());
    for i in 0..8 {
        let ids = if i % 2 == 0 { &large } else { &short };
        if let Err(error) = replace(&table, &ctx, "a", Some("g"), ids).await {
            write_result = Err(error);
            break;
        }
    }
    stop.store(true, Ordering::Release);
    reader.await??;
    write_result?;
    println!(
        "{}",
        serde_json::json!({"case": format!("{name}:concurrent"), "complete_snapshots": observations.load(Ordering::Relaxed)})
    );
    check(
        &format!("{name}:ordered-final"),
        read(&ctx, &table).await?,
        expected,
    )?;
    if keyed || memory {
        let input = Box::pin(RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::iter(vec![Ok(batch(&members("a", Some("g"), &[8]))?)]),
        ));
        table
            .write_cdc_append_stream(input, &ctx.task_ctx())
            .await?
            .finish()
            .await?;
        let mut after_rows = read(&ctx, &table).await?;
        if !after_rows.iter().any(|row| row.id == 8) {
            return Err("ordinary CDC row was not visible".into());
        }
        after_rows.retain(|row| row.id != 8);
        let mut before_rows = outside.clone();
        before_rows.extend(members("a", Some("g"), &short));
        check(
            &format!("{name}:ordinary-cdc-preserves-groups"),
            after_rows,
            before_rows,
        )?;
    }
    replace(&table, &ctx, "a", Some("g"), &[]).await?;
    check(
        &format!("{name}:empty-group"),
        read(&ctx, &table).await?,
        outside,
    )?;
    replace(&table, &ctx, "a", None, &[70]).await?;
    let mut final_rows = members("a", None, &[70]);
    final_rows.extend(members("a", Some(""), &[60]));
    final_rows.extend(members("b", Some("g"), &[90]));
    check(
        &format!("{name}:null-group"),
        read(&ctx, &table).await?,
        final_rows.clone(),
    )?;
    exercise_coalescing(name, &ctx, &table, keyed, &final_rows).await?;
    table.drain_in_flight_maintenance().await?;
    drop(table);
    if !memory {
        let reopened = Arc::new(
            CayenneTableProviderBuilder::new(
                catalog as Arc<dyn MetadataCatalog>,
                ctx.runtime_env(),
            )
            .open(name)
            .await?,
        );
        check(
            &format!("{name}:reopen"),
            read(&ctx, &reopened).await?,
            final_rows,
        )?;
        reopened.drain_in_flight_maintenance().await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or("supply a nonexistent artifact directory")?;
    let root = Path::new(&path);
    tokio::fs::create_dir(root).await?;
    for (name, memory, inline, keyed) in [
        ("keyless_inline", false, true, false),
        ("keyless_files", false, false, false),
        ("keyless_memory", true, true, false),
        ("keyed_files", false, false, true),
    ] {
        exercise(root, name, memory, inline, keyed).await?;
    }
    println!("REPLACE_SET_PROBE_COMPLETE");
    Ok(())
}
