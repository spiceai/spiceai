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

//! Integration tests for leftover SQLite-rollback generated slots.
//!
//! These talk to a real Postgres with `wal_level=logical`: inspect an inactive
//! leftover, an active live slot, a missing name, and the path where
//! `pg_current_wal_lsn` is not executable. They also start a replication
//! stream so the warning's metric snapshot is recorded through the wired path.

use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use data_components::cdc::AccelerationContents;
use data_components::postgres_replication::{
    NoopAppliedLsnStore, PgOutputFormat, ReplicationMetrics, ReplicationMetricsCollector,
    ReplicationParams, ReplicationStreamInput, RollbackSlotLookup, SchemaEvolutionPolicy, config,
    inspect_inactive_rollback_slot, start_replication_stream,
};
use secrecy::SecretString;
use tokio_postgres::NoTls;

use super::replication::{next_envelope, wait_for_ready};
use crate::init_tracing;
use crate::postgres::common;

const LIMITED_PASSWORD: &str = "runtime-integration-limited-pw";

fn dataset_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn params_for(port: u16, slot_name: &str, publication_name: &str) -> ReplicationParams {
    ReplicationParams {
        host: "localhost".into(),
        port,
        user: "postgres".into(),
        password: SecretString::from(common::PG_PASSWORD.to_string()),
        database: "postgres".into(),
        sslmode: config::SslMode::Disable,
        sslrootcert: None,
        slot_name: slot_name.into(),
        publication_name: publication_name.into(),
        initial_snapshot: true,
        snapshot_on_resume: false,
        ephemeral_accelerator: false,
        acceleration: AccelerationContents::Unknown,
        status_interval: Duration::from_secs(1),
        bootstrap_batch_size: 8192,
        shared: false,
        member_channel_capacity:
            data_components::postgres_replication::shared::DEFAULT_MEMBER_CHANNEL_CAPACITY,
        pg_output_format: PgOutputFormat::Binary,
        unclaimed_reservation_grace:
            data_components::postgres_replication::shared::DEFAULT_UNCLAIMED_RESERVATION_GRACE,
        watermark_flush_interval:
            data_components::postgres_replication::shared::DEFAULT_WATERMARK_FLUSH_INTERVAL,
        ready_lag: Duration::from_secs(2),
        rollback_slot_name: None,
    }
}

async fn connect_as(
    port: u16,
    user: &str,
    password: &str,
) -> Result<tokio_postgres::Client, anyhow::Error> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("localhost")
        .port(port)
        .user(user)
        .password(password)
        .dbname("postgres");
    let (client, connection) = cfg.connect(NoTls).await?;
    tokio::spawn(async move {
        let _: Result<(), tokio_postgres::Error> = connection.await;
    });
    Ok(client)
}

async fn drop_slot_if_exists(client: &tokio_postgres::Client, slot: &str) {
    let _ = client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await;
}

async fn create_logical_slot(client: &tokio_postgres::Client, slot: &str) {
    drop_slot_if_exists(client, slot).await;
    client
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .expect("create logical slot");
}

#[tokio::test(flavor = "multi_thread")]
async fn inspect_finds_an_inactive_leftover_and_its_retained_wal() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));
    let container = common::start_postgres_docker_container_with_logical_wal().await?;
    let port = u16::try_from(container.host_port(5432)?).expect("port fits in u16");
    let client = connect_as(port, "postgres", common::PG_PASSWORD).await?;

    const LEFTOVER: &str = "spice_itest_rollback_inactive";
    create_logical_slot(&client, LEFTOVER).await;
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS public.rollback_wal (id int PRIMARY KEY);
             INSERT INTO public.rollback_wal VALUES (1) ON CONFLICT DO NOTHING;
             INSERT INTO public.rollback_wal VALUES (2) ON CONFLICT DO NOTHING;",
        )
        .await?;

    let lookup = inspect_inactive_rollback_slot(&client, LEFTOVER).await;
    match lookup {
        RollbackSlotLookup::Inactive {
            slot_name,
            retained_wal_bytes,
            ..
        } => {
            assert_eq!(slot_name, LEFTOVER);
            let bytes = retained_wal_bytes.expect("superuser can compute retained WAL");
            assert!(
                bytes > 0,
                "WAL written after slot creation must retain at least one byte, got {bytes}"
            );
        }
        other => panic!("expected Inactive leftover, got {other:?}"),
    }

    drop_slot_if_exists(&client, LEFTOVER).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn inspect_is_absent_when_the_slot_does_not_exist() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));
    let container = common::start_postgres_docker_container_with_logical_wal().await?;
    let port = u16::try_from(container.host_port(5432)?).expect("port fits in u16");
    let client = connect_as(port, "postgres", common::PG_PASSWORD).await?;
    let lookup = inspect_inactive_rollback_slot(&client, "spice_itest_rollback_missing").await;
    assert_eq!(lookup, RollbackSlotLookup::Absent);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn inspect_does_not_treat_the_live_slot_as_a_leftover() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));
    let container = common::start_postgres_docker_container_with_logical_wal().await?;
    let port = u16::try_from(container.host_port(5432)?).expect("port fits in u16");
    let source = connect_as(port, "postgres", common::PG_PASSWORD).await?;
    source
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS public.repl_users (id int PRIMARY KEY, name text);
             TRUNCATE public.repl_users;
             INSERT INTO public.repl_users VALUES (1, 'Alice');",
        )
        .await?;

    const LIVE: &str = "spice_itest_rollback_live_active";
    const PUBLICATION: &str = "spice_itest_rollback_live_pub";
    let collector = ReplicationMetricsCollector::new();
    let mut stream = start_replication_stream(ReplicationStreamInput {
        dataset_name: "repl_users".into(),
        params: params_for(port, LIVE, PUBLICATION),
        schema: dataset_schema(),
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: "repl_users".into(),
        metrics: Arc::clone(&collector),
        policy: SchemaEvolutionPolicy::Block,
        applied_lsn_store: Arc::new(NoopAppliedLsnStore),
        write_back_registry: None,
    });
    next_envelope(&mut stream, "bootstrap")
        .await?
        .commit()
        .await?;
    wait_for_ready(&mut stream, "live slot ready")
        .await?
        .commit()
        .await?;

    let lookup = inspect_inactive_rollback_slot(&source, LIVE).await;
    assert_eq!(
        lookup,
        RollbackSlotLookup::Active,
        "a consumed slot is not a leftover"
    );
    let view = ReplicationMetrics::new(collector);
    assert_eq!(
        view.inactive_rollback_slots(),
        None,
        "the live slot must not publish a leftover series"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_records_inactive_leftover_metrics() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));
    let container = common::start_postgres_docker_container_with_logical_wal().await?;
    let port = u16::try_from(container.host_port(5432)?).expect("port fits in u16");
    let source = connect_as(port, "postgres", common::PG_PASSWORD).await?;
    source
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS public.repl_users (id int PRIMARY KEY, name text);
             TRUNCATE public.repl_users;
             INSERT INTO public.repl_users VALUES (1, 'Alice');",
        )
        .await?;

    const LEFTOVER: &str = "spice_itest_rollback_wired_leftover";
    const LIVE: &str = "spice_itest_rollback_wired_live";
    const PUBLICATION: &str = "spice_itest_rollback_wired_pub";
    create_logical_slot(&source, LEFTOVER).await;
    source
        .batch_execute("INSERT INTO public.repl_users VALUES (2, 'Bob') ON CONFLICT DO NOTHING;")
        .await?;

    let mut params = params_for(port, LIVE, PUBLICATION);
    params.rollback_slot_name = Some(LEFTOVER.to_string());
    let collector = ReplicationMetricsCollector::new();
    let mut stream = start_replication_stream(ReplicationStreamInput {
        dataset_name: "repl_users".into(),
        params,
        schema: dataset_schema(),
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: "repl_users".into(),
        metrics: Arc::clone(&collector),
        policy: SchemaEvolutionPolicy::Block,
        applied_lsn_store: Arc::new(NoopAppliedLsnStore),
        write_back_registry: None,
    });
    next_envelope(&mut stream, "bootstrap")
        .await?
        .commit()
        .await?;
    wait_for_ready(&mut stream, "wired leftover ready")
        .await?
        .commit()
        .await?;

    let view = ReplicationMetrics::new(collector);
    assert_eq!(view.inactive_rollback_slots(), Some(1));
    assert_eq!(view.rollback_slot_name().as_deref(), Some(LEFTOVER));
    let bytes = view
        .rollback_slot_retained_wal_bytes()
        .expect("superuser can compute retained WAL");
    assert!(
        bytes > 0,
        "wired path must publish retained WAL bytes, got {bytes}"
    );
    drop_slot_if_exists(&source, LEFTOVER).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn inspect_degrades_when_pg_current_wal_lsn_is_not_executable() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));
    let container = common::start_postgres_docker_container_with_logical_wal().await?;
    let port = u16::try_from(container.host_port(5432)?).expect("port fits in u16");
    let admin = connect_as(port, "postgres", common::PG_PASSWORD).await?;

    admin
        .batch_execute(&format!(
            "DO $$ BEGIN
               IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'spice_limited') THEN
                 DROP OWNED BY spice_limited;
                 DROP ROLE spice_limited;
               END IF;
             END $$;
             CREATE ROLE spice_limited LOGIN PASSWORD '{LIMITED_PASSWORD}' REPLICATION;
             GRANT CONNECT ON DATABASE postgres TO spice_limited;
             REVOKE EXECUTE ON FUNCTION pg_catalog.pg_current_wal_lsn() FROM PUBLIC;
             GRANT EXECUTE ON FUNCTION pg_catalog.pg_current_wal_lsn() TO postgres;"
        ))
        .await?;

    let limited = connect_as(port, "spice_limited", LIMITED_PASSWORD).await?;
    const LEFTOVER: &str = "spice_itest_rollback_noperm";
    create_logical_slot(&limited, LEFTOVER).await;

    let lookup = inspect_inactive_rollback_slot(&limited, LEFTOVER).await;
    match lookup {
        RollbackSlotLookup::Inactive {
            slot_name,
            retained_wal_bytes,
            ..
        } => {
            assert_eq!(slot_name, LEFTOVER);
            assert_eq!(
                retained_wal_bytes, None,
                "EXECUTE revoked on pg_current_wal_lsn must not invent WAL bytes"
            );
        }
        other => panic!("expected Inactive without WAL bytes, got {other:?}"),
    }

    // A role that cannot see pg_replication_slots still must not fail dataset load.
    admin
        .batch_execute(
            "REVOKE SELECT ON pg_catalog.pg_replication_slots FROM PUBLIC;
             REVOKE SELECT ON pg_catalog.pg_replication_slots FROM spice_limited;",
        )
        .await?;
    let denied = inspect_inactive_rollback_slot(&limited, LEFTOVER).await;
    match denied {
        RollbackSlotLookup::PermissionDenied { slot_name } => {
            assert_eq!(slot_name, LEFTOVER);
        }
        RollbackSlotLookup::Absent => {
            // Some catalogs hide the view as empty rows rather than SQLSTATE 42501.
        }
        RollbackSlotLookup::Unreadable { slot_name, error } => {
            assert_eq!(slot_name, LEFTOVER);
            assert!(
                error.to_lowercase().contains("permission")
                    || error.contains("42501")
                    || error.to_lowercase().contains("denied"),
                "unreadable without SELECT must name the permission problem, got {error}"
            );
        }
        other => panic!("expected a degraded lookup after REVOKE SELECT, got {other:?}"),
    }

    drop_slot_if_exists(&admin, LEFTOVER).await;
    Ok(())
}
