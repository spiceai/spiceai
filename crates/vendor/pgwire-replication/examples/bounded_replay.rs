// examples/bounded_replay.rs
//
// cargo run --example bounded_replay --features examples

#[path = "support/common.rs"]
mod common;
use pgwire_replication::{
    client::ReplicationEvent, Lsn, ReplicationClient, ReplicationConfig, TlsConfig,
};
fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

async fn current_wal_lsn(sql: &tokio_postgres::Client) -> anyhow::Result<Lsn> {
    let row = sql
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?;
    let s: String = row.get(0);
    Ok(Lsn::parse(&s).unwrap())
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let host = env("PGHOST", "127.0.0.1");
    let port: u16 = env("PGPORT", "5432").parse()?;
    let user = env("PGUSER", "postgres");
    let password = env("PGPASSWORD", "postgres");
    let database = env("PGDATABASE", "postgres");
    let slot = env("PGSLOT", "example_slot");
    let publication = env("PGPUBLICATION", "example_pub");

    let sql = common::connect_control_plane(&host, port, &user, &password, &database).await?;
    let start_lsn = common::ensure_slot_and_get_start_lsn(&sql, &slot, &publication).await?;

    // Choose a stop LSN. For demonstration we use “now + some activity”.
    // In a real system, stop_at_lsn would come from a stored checkpoint boundary.
    let stop_at_lsn = current_wal_lsn(&sql).await?;
    println!("bounded replay start={start_lsn} stop_at={stop_at_lsn}");

    let cfg = ReplicationConfig {
        host,
        port,
        user,
        password,
        database,
        tls: TlsConfig::disabled(),
        slot,
        publication,
        start_lsn,
        stop_at_lsn: Some(stop_at_lsn),

        status_interval: std::time::Duration::from_secs(1),
        idle_wakeup_interval: std::time::Duration::from_secs(30),
        buffer_events: 8192,
    };

    let mut repl = ReplicationClient::connect(cfg).await?;

    loop {
        match repl.recv().await {
            Ok(Some(ev)) => match ev {
                ReplicationEvent::KeepAlive { .. } => {}
                ReplicationEvent::XLogData {
                    wal_start,
                    wal_end,
                    data,
                    ..
                } => {
                    println!(
                        "XLogData wal_start={wal_start} wal_end={wal_end} bytes={}",
                        data.len()
                    );
                    repl.update_applied_lsn(wal_end);
                }
                ReplicationEvent::StoppedAt { reached } => {
                    println!("StoppedAt reached={reached}");
                    break;
                }
                ReplicationEvent::Begin {
                    final_lsn,
                    xid,
                    commit_time_micros,
                } => println!("Transaction started, final_lsn={final_lsn}, xid={xid}, commit_t={commit_time_micros}"),
                ReplicationEvent::Commit {
                    ..
                } => print!("transaction commit."),
                ReplicationEvent::Message { prefix, content, .. } => println!(
                    "Logical message: prefix={}, {} bytes", prefix, content.len()
                ),
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
