// examples/with_tls.rs
//
// cargo run --example with_tls --features "examples,tls-rustls"

#[path = "support/common.rs"]
mod common;

use pgwire_replication::{
    client::ReplicationEvent, ReplicationClient, ReplicationConfig, SslMode, TlsConfig,
};
use std::path::PathBuf;

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // Connection parameters
    let host = env("PGHOST", "localhost");
    let port: u16 = env("PGPORT", "5432").parse()?;
    let user = env("PGUSER", "postgres");
    let password = env("PGPASSWORD", "postgres");
    let database = env("PGDATABASE", "postgres");
    let slot = env("PGSLOT", "example_slot_tls");
    let publication = env("PGPUBLICATION", "example_pub_tls");

    // TLS parameters
    let ca_pem_path = PathBuf::from(env("PGTLS_CA", "/etc/ssl/certs/ca-certificates.crt"));
    let sni_hostname = env("PGTLS_SNI", &host);

    // Control plane (SQL)
    let sql = common::connect_control_plane(&host, port, &user, &password, &database).await?;
    let start_lsn = common::ensure_slot_and_get_start_lsn(&sql, &slot, &publication).await?;

    println!("starting TLS replication from start_lsn={start_lsn}");

    let cfg = ReplicationConfig {
        host,
        port,
        user,
        password,
        database,

        tls: TlsConfig {
            mode: SslMode::VerifyFull,
            ca_pem_path: Some(ca_pem_path),
            sni_hostname: Some(sni_hostname),
            client_cert_pem_path: None,
            client_key_pem_path: None,
        },

        slot,
        publication,
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
