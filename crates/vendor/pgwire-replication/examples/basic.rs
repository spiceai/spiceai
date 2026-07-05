// examples/basic.rs
//
// START_LSN="0/16B6C50" cargo run --example basic --features examples

use pgwire_replication::{
    client::ReplicationEvent, Lsn, ReplicationClient, ReplicationConfig, TlsConfig,
};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // If PGHOST starts with '/', connects via Unix socket.
    // e.g. PGHOST=/var/run/postgresql
    let host = env("PGHOST", "127.0.0.1");
    let port: u16 = env("PGPORT", "5432").parse()?;
    let user = env("PGUSER", "postgres");
    let password = env("PGPASSWORD", "postgres");
    let database = env("PGDATABASE", "postgres");
    let slot = env("PGSLOT", "my_slot");
    let publication = env("PGPUBLICATION", "my_pub");

    // Provide explicitly, e.g. START_LSN=0/16B6C50
    let start_lsn = Lsn::parse(&env("START_LSN", "0/0")).unwrap();

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
                    break;
                }
                ReplicationEvent::Begin { xid, .. } => println!("Transaction started, xid={xid}"),
                ReplicationEvent::Commit { end_lsn, .. } => {
                    print!("Transaction finished, end_lsn={end_lsn}")
                }
                ReplicationEvent::Message {
                    transactional,
                    prefix,
                    content,
                    lsn,
                } => {
                    println!(
                        "Message lsn={lsn} transactional={transactional} \
                         prefix={prefix:?} bytes={}",
                        content.len()
                    );
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
