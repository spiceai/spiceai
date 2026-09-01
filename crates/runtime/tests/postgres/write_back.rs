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

#![expect(
    clippy::expect_used,
    clippy::clone_on_ref_ptr,
    reason = "test code clones Arcs and asserts with expect rather than propagating"
)]

//! End-to-end durable write-back against a Postgres source.
//!
//! The write is driven through a `BEGIN … COMMIT` transaction deliberately. An
//! ordinary write on a `write_mode: write_back` dataset also forwards to the
//! source fire-and-forget, so it reaches Postgres whether or not the delivery
//! worker ever runs. A transactional write skips that forward and records
//! dirty-key markers in its commit transaction instead, leaving the delivery
//! worker as its only path to the source — so this is the shape that observes
//! whether delivery actually happens (#13396).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::cayenne::transaction::{describe, run_txn};
use crate::postgres::common;
use anyhow::anyhow;
use app::AppBuilder;
use cayenne::CayenneTableProvider;
use runtime::Runtime;
use runtime_table::accelerated::AcceleratedTable;
use runtime_table::accelerated::write::dual_write::{
    CayenneWriteTarget, extract_cayenne_write_target,
};
use secrecy::ExposeSecret;
use spice_table::LayerWalk;
use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode, WriteMode};
use spicepod::component::access::AccessMode;
use spicepod::component::dataset::{Dataset, replication::Replication};
use spicepod::param::Params;

use crate::utils::{
    register_test_connectors, runtime_ready_check_with_timeout, test_request_context,
    wait_until_true,
};
use crate::{configure_test_datafusion, init_tracing};

/// Startup budget: a logical-replication slot plus the accelerator bootstrap.
const RUNTIME_READY_TIMEOUT: Duration = Duration::from_mins(2);
/// How long the source is given to converge on the written value. Delivery
/// retries on Fibonacci backoff, so this covers several failed passes.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(90);
/// How long the commit's marker must become visible in. Generous next to the
/// worker's one-second idle poll, but bounded: never observing a marker is the
/// signal that the write bypassed the staged path, not something to wait out.
const MARKER_VISIBLE_TIMEOUT: Duration = Duration::from_secs(5);

/// The value the accelerator write sets; the source must end up holding it.
const UPDATED_VALUE: i64 = 4_242;
/// The `BIGINT` primary key of the row under test.
const COUNTER_ID: i64 = 1;

/// A `BIGINT`-keyed source table holding one row — the shape that selects
/// Cayenne's converter-free `Int64Pk` deletion strategy.
async fn seed_counter(client: &tokio_postgres::Client) -> Result<(), anyhow::Error> {
    client
        .simple_query(
            "CREATE TABLE public.wb_counter (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)",
        )
        .await?;
    client
        .execute(
            "INSERT INTO public.wb_counter VALUES ($1, $2)",
            &[&COUNTER_ID, &1_i64],
        )
        .await?;
    Ok(())
}

/// Read `value` for the row under test straight from Postgres.
async fn source_value(client: &tokio_postgres::Client) -> Result<Option<i64>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT value FROM public.wb_counter WHERE id = $1",
            &[&COUNTER_ID],
        )
        .await?;
    Ok(rows.first().map(|row| row.get::<_, i64>(0)))
}

async fn write_back_provider(rt: &Runtime) -> Result<Box<CayenneTableProvider>, anyhow::Error> {
    let provider = rt
        .datafusion()
        .get_accelerated_table_provider("wb_counter")
        .await?;
    let accelerated =
        spice_table::find_layer::<AcceleratedTable>(provider.as_ref(), LayerWalk::Read)
            .ok_or_else(|| anyhow!("wb_counter has no accelerated-table layer"))?;
    match extract_cayenne_write_target(accelerated.get_accelerator_ref()) {
        Some(CayenneWriteTarget::Staged(provider)) => Ok(provider),
        Some(CayenneWriteTarget::Partitioned(_)) => {
            Err(anyhow!("wb_counter unexpectedly uses partitioned Cayenne"))
        }
        None => Err(anyhow!("wb_counter has no Cayenne write target")),
    }
}

async fn backlog_display(provider: &CayenneTableProvider) -> String {
    provider
        .dirty_key_count()
        .await
        .map_or_else(|e| format!("unreadable ({e})"), |n| n.to_string())
}

/// A durable-write-back BIGINT-keyed dataset must actually reach its source.
///
/// Regression for #13396: a single-`Int64` primary key selects the
/// converter-free `Int64Pk` deletion strategy, while durable marker decoding
/// still requires the `OwnedRow` converter used by marker encoding.
#[tokio::test(flavor = "multi_thread")]
async fn bigint_primary_key_write_back_reaches_the_source() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,runtime_table::accelerated::write_back_worker=debug,info",
    ));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port)
                .await
                .map_err(|e| anyhow!("start container: {e}"))?;

            let mut source = common::connect(u16::try_from(port)?).await?;
            seed_counter(&source).await?;

            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne");
            let metadata_dir = temp_dir.path().join("metadata");

            let pg_params: HashMap<String, String> = common::get_pg_params(port)
                .into_iter()
                .map(|(k, v)| (k, v.expose_secret().to_string()))
                .collect();
            let accel_params = HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    cayenne_dir.display().to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    metadata_dir.display().to_string(),
                ),
            ]);

            let mut dataset = Dataset::new("postgres:public.wb_counter", "wb_counter");
            dataset.access = AccessMode::ReadWrite;
            dataset.params = Some(Params::from_string_map(pg_params));
            dataset.replication = Some(Replication { enabled: true });
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Changes),
                write_mode: WriteMode::WriteBack,
                primary_key: Some("id".to_string()),
                on_conflict: [("id".to_string(), OnConflictBehavior::Upsert)]
                    .into_iter()
                    .collect(),
                params: Some(Params::from_string_map(accel_params)),
                ..Acceleration::default()
            });

            configure_test_datafusion();
            let app = AppBuilder::new("postgres_write_back_test")
                .with_dataset(dataset)
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(RUNTIME_READY_TIMEOUT) => {
                    return Err(anyhow!("Timed out waiting for wb_counter to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check_with_timeout(&rt, RUNTIME_READY_TIMEOUT).await;
            let provider = write_back_provider(&rt).await?;
            assert_eq!(
                provider.dirty_key_count().await?,
                0,
                "a fresh write-back dataset must start with no pending markers"
            );

            // Hold the source row until the marker is observed. This makes the
            // marker-visible assertion deterministic: the delivery worker may
            // claim the key, but its upsert cannot finish and clear the marker.
            let source_lock = source.transaction().await?;
            source_lock
                .query_one(
                    "SELECT id FROM public.wb_counter WHERE id = $1 FOR UPDATE",
                    &[&COUNTER_ID],
                )
                .await?;

            // The transaction commits to the accelerator and marks its primary
            // keys; the delivery worker is the only thing that can carry the
            // value to Postgres from here.
            run_txn(
                &rt,
                &format!(
                    "BEGIN; UPDATE wb_counter SET value = {UPDATED_VALUE} \
                     WHERE id = {COUNTER_ID}; COMMIT;"
                ),
            )
            .await
            .map_err(|e| anyhow!("transactional write-back UPDATE failed: {}", describe(&e)))?;

            // Only a commit-publish transaction marks primary keys. Observing
            // the marker proves the write took the staged path rather than the
            // fire-and-forget source forward.
            let marked = wait_until_true(MARKER_VISIBLE_TIMEOUT, || async {
                provider.dirty_key_count().await.is_ok_and(|n| n == 1)
            })
            .await;
            assert!(
                marked,
                "the commit left no write-back marker within {MARKER_VISIBLE_TIMEOUT:?} \
                 (last read: {}), so the write never took the staged path this test \
                 exists to exercise",
                backlog_display(&provider).await,
            );

            source_lock.commit().await?;

            let converged = wait_until_true(DELIVERY_TIMEOUT, || async {
                matches!(
                    source_value(&source).await,
                    Ok(Some(value)) if value == UPDATED_VALUE
                )
            })
            .await;
            let observed = source_value(&source).await;

            assert!(
                converged,
                "durable write-back never reached Postgres within {DELIVERY_TIMEOUT:?}: \
                 wb_counter.value is {:?} in the source, want {UPDATED_VALUE}; \
                 undelivered write-back markers: {}",
                observed,
                backlog_display(&provider).await,
            );

            // Marker seen, then the source value, then no marker: that ordering
            // is a complete claim -> deliver -> clear cycle, which no other
            // route to the source can produce.
            let drained = wait_until_true(DELIVERY_TIMEOUT, || async {
                provider.dirty_key_count().await.is_ok_and(|n| n == 0)
            })
            .await;
            assert!(
                drained,
                "the source converged but {} write-back marker(s) were never cleared",
                backlog_display(&provider).await,
            );

            Ok(())
        })
        .await
}
