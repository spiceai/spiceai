use anyhow::Context;
use tokio_postgres::NoTls;

use pgwire_replication::Lsn;

/// Connect a control-plane Postgres client (SQL).
pub async fn connect_control_plane(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
) -> anyhow::Result<tokio_postgres::Client> {
    let dsn = format!("host={host} port={port} user={user} password={password} dbname={database}");
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect (control plane)")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

/// Ensure publication/slot exist and return a safe start LSN.
/// Preference: confirmed_flush_lsn -> restart_lsn -> pg_current_wal_lsn.
pub async fn ensure_slot_and_get_start_lsn(
    client: &tokio_postgres::Client,
    slot: &str,
    publication: &str,
) -> anyhow::Result<Lsn> {
    // Publication: best-effort. In real systems you may manage publications via migrations.
    let _ = client
        .batch_execute(&format!("CREATE PUBLICATION {publication} FOR ALL TABLES;"))
        .await;

    // Slot: create if absent (ignore errors for already-existing).
    let _ = client
        .batch_execute(&format!(
            "SELECT * FROM pg_create_logical_replication_slot('{slot}','pgoutput');"
        ))
        .await;

    let row = client
        .query_one(
            "SELECT confirmed_flush_lsn::text, restart_lsn::text
             FROM pg_replication_slots
             WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .context("query pg_replication_slots")?;

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
