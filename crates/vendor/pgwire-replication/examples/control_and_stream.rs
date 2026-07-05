// examples/control_and_stream.rs
//
// cargo run --example control_and_stream --features examples

use anyhow::Context;
use pgwire_replication::{
    client::ReplicationEvent, Lsn, ReplicationClient, ReplicationConfig, TlsConfig,
};

use tokio_postgres::NoTls;

/// Control-plane helper:
/// - ensures publication / slot exist
/// - determines a safe start LSN
async fn control_plane_get_start_lsn(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
    slot: &str,
    publication: &str,
) -> anyhow::Result<Lsn> {
    let dsn = format!("host={host} port={port} user={user} password={password} dbname={database}");
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect (control plane)")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    client
        .batch_execute(&format!("CREATE PUBLICATION {publication} FOR ALL TABLES;"))
        .await
        .ok();

    let _ = client
        .batch_execute(&format!(
            "SELECT * FROM pg_create_logical_replication_slot('{slot}','pgoutput');"
        ))
        .await;

    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text, restart_lsn::text
                FROM pg_replication_slots
                WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .context("query pg_replication_slots")?
        .context("replication slot not found (expected it to exist)")?;

    let confirmed: Option<String> = row.get(0);
    let restart: Option<String> = row.get(1);

    if let Some(s) = confirmed {
        return Lsn::parse(&s).context("parse confirmed_flush_lsn");
    }
    if let Some(s) = restart {
        return Lsn::parse(&s).context("parse restart_lsn");
    }

    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .context("query pg_current_wal_lsn")?;
    let now: String = row.get(0);
    Lsn::parse(&now).context("parse pg_current_wal_lsn")
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let host = "127.0.0.1";
    let port = 5432;
    let user = "postgres";
    let password = "postgres";
    let database = "postgres";
    let slot = "my_slot";
    let publication = "my_pub";

    let start_lsn =
        control_plane_get_start_lsn(host, port, user, password, database, slot, publication)
            .await?;

    println!("got start lsn = {start_lsn}");

    let cfg = ReplicationConfig {
        host: host.into(),
        port,
        user: user.into(),
        password: password.into(),
        database: database.into(),
        tls: TlsConfig::disabled(),
        slot: slot.into(),
        publication: publication.into(),
        start_lsn,
        stop_at_lsn: None,

        status_interval: std::time::Duration::from_secs(1),
        idle_wakeup_interval: std::time::Duration::from_secs(30),
        buffer_events: 8192,
    };

    let mut repl = ReplicationClient::connect(cfg).await?;

    loop {
        match repl.recv().await {
            Ok(Some(ev)) => match ev {
                ReplicationEvent::XLogData { wal_end, data, .. } => {
                    println!("XLogData wal_end={wal_end} bytes={}", data.len());
                    repl.update_applied_lsn(wal_end);
                }
                ReplicationEvent::KeepAlive {
                    wal_end,
                    reply_requested,
                    ..
                } => {
                    println!("KeepAlive wal_end={wal_end} reply_requested={reply_requested}");
                }
                ReplicationEvent::StoppedAt { reached } => {
                    println!("StoppedAt reached={reached}");
                    // break is optional; the stream should end shortly anyway
                    break;
                }
                ReplicationEvent::Begin { .. } => println!(
                    "Transaction start, probably want to flush in-flight events to the sinks."
                ),
                ReplicationEvent::Commit { .. } => println!(
                    "Transanction finished, good time to store a checkpoint at the higher level."
                ),
                ReplicationEvent::Message {
                    prefix, content, ..
                } => {
                    println!("Message prefix={prefix:?} bytes={}", content.len());
                }
            },
            Ok(None) => {
                println!("Replication ended cleanly");
                break;
            }
            Err(e) => {
                eprintln!("Replication failed: {e}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}
