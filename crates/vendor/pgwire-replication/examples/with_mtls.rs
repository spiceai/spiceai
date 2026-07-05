// examples/with_mtls.rs
//
// cargo run --example with_mtls --features "examples,tls-rustls"

#[path = "support/common.rs"]
mod common;
use pgwire_replication::{
    client::ReplicationEvent, ReplicationClient, ReplicationConfig, SslMode, TlsConfig,
};
use std::path::PathBuf;

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_opt(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // ---- Connection parameters ----
    let host = env("PGHOST", "db.example.com");
    let port: u16 = env("PGPORT", "5432").parse()?;

    // Control-plane identity (SQL)
    let pg_user = env("PGUSER", "postgres");
    let pg_password = env("PGPASSWORD", "postgres");
    let pg_database = env("PGDATABASE", "postgres");

    // Replication-plane identity (logical replication)
    let repl_user = env_opt("REPL_USER").unwrap_or_else(|| pg_user.clone());
    let repl_password = env_opt("REPL_PASSWORD").unwrap_or_else(|| pg_password.clone());

    let slot = env("PGSLOT", "example_slot_mtls");
    let publication = env("PGPUBLICATION", "example_pub_mtls");

    // ---- TLS / mTLS parameters ----
    let ca_pem = PathBuf::from(env("PGTLS_CA", "/etc/ssl/ca.pem"));
    let client_cert = PathBuf::from(env("PGTLS_CLIENT_CERT", "/etc/ssl/client.crt.pem"));
    let client_key = PathBuf::from(env("PGTLS_CLIENT_KEY", "/etc/ssl/client.key.pem"));
    let sni = env("PGTLS_SNI", &host);

    // ---- Control plane (SQL) ----
    let sql =
        common::connect_control_plane(&host, port, &pg_user, &pg_password, &pg_database).await?;
    let start_lsn = common::ensure_slot_and_get_start_lsn(&sql, &slot, &publication).await?;

    println!(
        "starting mTLS replication: start_lsn={start_lsn} control_plane_user={pg_user} repl_user={repl_user}"
    );

    // ---- Replication config ----
    let cfg = ReplicationConfig {
        host,
        port,
        user: repl_user,
        password: repl_password,
        database: pg_database,

        tls: TlsConfig {
            mode: SslMode::VerifyFull,
            ca_pem_path: Some(ca_pem),
            sni_hostname: Some(sni),
            client_cert_pem_path: Some(client_cert),
            client_key_pem_path: Some(client_key),
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
