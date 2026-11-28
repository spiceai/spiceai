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

//! Detect an inactive generated `PostgreSQL` slot left behind by the `SQLite`
//! rollback copy after an upgrade to object-native Cayenne.
//!
//! Default slots are named for the acceleration's state location. The `SQLite`
//! copy kept for rollback used an instance-scoped name, so acknowledging the
//! object-native copy does not advance that slot. An inactive leftover retains
//! WAL on the source until it is dropped. This module inspects that slot at
//! startup, logs one warning, and records the quantity a metric can carry.
//!
//! Inspection never fails dataset load: permission denied and other catalog
//! errors degrade to a warning without WAL bytes (or without confirming the
//! slot exists).

use std::fmt::Write;

use super::config;
use super::metrics::MetricsCollector;
use super::{CONNECTOR_DOCS, pg_error_detail};

/// SQLSTATE `42501` (`insufficient_privilege`).
const SQLSTATE_INSUFFICIENT_PRIVILEGE: &str = "42501";
/// SQLSTATE `42703` (`undefined_column`) — `pg_replication_slots.inactive_since`
/// is `PostgreSQL` 14+.
const SQLSTATE_UNDEFINED_COLUMN: &str = "42703";

/// What inspecting the `SQLite` rollback copy's generated slot found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RollbackSlotLookup {
    /// No slot by that name.
    Absent,
    /// The slot exists and a consumer holds it — not a leftover.
    Active,
    /// The slot exists, nobody consumes it, so it retains WAL on the source.
    Inactive {
        slot_name: String,
        /// Seconds since `inactive_since`, when the server reports it (`PostgreSQL` 14+).
        inactive_age_seconds: Option<u64>,
        /// `pg_current_wal_lsn() - restart_lsn`, when readable.
        retained_wal_bytes: Option<u64>,
    },
    /// The catalog query was refused (`SQLSTATE 42501`).
    PermissionDenied { slot_name: String },
    /// Some other error reading the catalog; startup continues.
    Unreadable { slot_name: String, error: String },
}

/// The instance-scoped generated slot the `SQLite` rollback copy still holds
/// after an upgrade onto a location-scoped (object-native) generated slot.
///
/// `None` when there is no distinct rollback slot: the dataset names its slot
/// explicitly (both copies share that name), the acceleration is not shared
/// through a location, or the two generated names collide.
#[must_use]
pub fn generated_rollback_slot_name(
    dataset_name: &str,
    shared_location_id: Option<&str>,
    explicit_slot: bool,
) -> Option<String> {
    if explicit_slot {
        return None;
    }
    let location_id = shared_location_id?;
    let location_slot = config::default_slot_name_on(dataset_name, Some(location_id));
    let instance_slot = config::default_slot_name(dataset_name);
    (instance_slot != location_slot).then_some(instance_slot)
}

/// Look up `slot_name` on the source. Never fails the caller: permission
/// denied and other catalog errors become [`RollbackSlotLookup`] variants so
/// dataset load continues.
pub async fn inspect_inactive_rollback_slot(
    client: &tokio_postgres::Client,
    slot_name: &str,
) -> RollbackSlotLookup {
    let row = match read_slot_row(client, slot_name, true).await {
        SlotRowQuery::Row(row) => row,
        SlotRowQuery::Missing => return RollbackSlotLookup::Absent,
        SlotRowQuery::PermissionDenied => {
            return RollbackSlotLookup::PermissionDenied {
                slot_name: slot_name.to_string(),
            };
        }
        SlotRowQuery::Unreadable { error } => {
            return RollbackSlotLookup::Unreadable {
                slot_name: slot_name.to_string(),
                error,
            };
        }
        SlotRowQuery::NoInactiveSince => match read_slot_row(client, slot_name, false).await {
            SlotRowQuery::Row(row) => row,
            SlotRowQuery::Missing => return RollbackSlotLookup::Absent,
            SlotRowQuery::PermissionDenied => {
                return RollbackSlotLookup::PermissionDenied {
                    slot_name: slot_name.to_string(),
                };
            }
            SlotRowQuery::Unreadable { error } => {
                return RollbackSlotLookup::Unreadable {
                    slot_name: slot_name.to_string(),
                    error,
                };
            }
            SlotRowQuery::NoInactiveSince => {
                return RollbackSlotLookup::Unreadable {
                    slot_name: slot_name.to_string(),
                    error: "inactive_since is not available on this PostgreSQL version".to_string(),
                };
            }
        },
    };

    let active: bool = row.get(0);
    if active {
        return RollbackSlotLookup::Active;
    }
    let inactive_age_seconds = nonneg_i64(row.get::<_, Option<i64>>(1));
    let restart_lsn: Option<String> = row.get(2);
    let retained_wal_bytes = match restart_lsn.as_deref() {
        Some(lsn) => read_retained_wal_bytes(client, lsn).await,
        None => None,
    };
    RollbackSlotLookup::Inactive {
        slot_name: slot_name.to_string(),
        inactive_age_seconds,
        retained_wal_bytes,
    }
}

enum SlotRowQuery {
    Row(tokio_postgres::Row),
    Missing,
    PermissionDenied,
    NoInactiveSince,
    Unreadable { error: String },
}

async fn read_slot_row(
    client: &tokio_postgres::Client,
    slot_name: &str,
    with_inactive_since: bool,
) -> SlotRowQuery {
    let sql = if with_inactive_since {
        "SELECT active, \
                EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - inactive_since))::bigint, \
                restart_lsn::text \
           FROM pg_catalog.pg_replication_slots \
          WHERE slot_name = $1"
    } else {
        "SELECT active, \
                NULL::bigint, \
                restart_lsn::text \
           FROM pg_catalog.pg_replication_slots \
          WHERE slot_name = $1"
    };
    match client.query_opt(sql, &[&slot_name]).await {
        Ok(Some(row)) => SlotRowQuery::Row(row),
        Ok(None) => SlotRowQuery::Missing,
        Err(error) if sqlstate_is(&error, SQLSTATE_INSUFFICIENT_PRIVILEGE) => {
            SlotRowQuery::PermissionDenied
        }
        Err(error) if with_inactive_since && sqlstate_is(&error, SQLSTATE_UNDEFINED_COLUMN) => {
            SlotRowQuery::NoInactiveSince
        }
        Err(error) => SlotRowQuery::Unreadable {
            error: pg_error_detail(&error),
        },
    }
}

async fn read_retained_wal_bytes(
    client: &tokio_postgres::Client,
    restart_lsn: &str,
) -> Option<u64> {
    // Read the current insert LSN as text (the same form `current_wal_lsn`
    // uses) and subtract the parsed 64-bit values. A combined
    // `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` on the slot row
    // would turn EXECUTE-revoked on `pg_current_wal_lsn` into a failure to
    // see the slot at all.
    let restart = super::slot::parse_lsn(restart_lsn).ok()?;
    let current = match client
        .query_one("SELECT pg_catalog.pg_current_wal_lsn()::text", &[])
        .await
    {
        Ok(row) => row.get::<_, String>(0),
        Err(error) if sqlstate_is(&error, SQLSTATE_INSUFFICIENT_PRIVILEGE) => return None,
        Err(_) => return None,
    };
    let current = super::slot::parse_lsn(&current).ok()?;
    Some(current.saturating_sub(restart))
}

fn sqlstate_is(error: &tokio_postgres::Error, code: &str) -> bool {
    error
        .as_db_error()
        .is_some_and(|db| db.code().code() == code)
}

fn nonneg_i64(value: Option<i64>) -> Option<u64> {
    value.and_then(|n| u64::try_from(n.max(0)).ok())
}

/// The operator-facing warning for a leftover rollback slot, or `None` when
/// nothing should be logged (the slot is absent or still consumed).
///
/// A pure function so a reword cannot quietly drop the dataset, the slot, the
/// impact, the drop command, or the docs link.
#[must_use]
pub fn rollback_slot_warning(dataset: &str, lookup: &RollbackSlotLookup) -> Option<String> {
    match lookup {
        RollbackSlotLookup::Absent | RollbackSlotLookup::Active => None,
        RollbackSlotLookup::Inactive {
            slot_name,
            inactive_age_seconds,
            retained_wal_bytes,
        } => {
            let mut detail = String::new();
            if let Some(age) = inactive_age_seconds.filter(|age| *age > 0) {
                detail.push_str(", inactive for ");
                detail.push_str(&format_age(age));
            }
            match retained_wal_bytes {
                Some(bytes) => {
                    let _ = write!(detail, ", retaining {bytes} bytes of WAL");
                }
                None => detail.push_str(", retained WAL bytes could not be read"),
            }
            Some(format!(
                "Dataset '{dataset}' left an inactive PostgreSQL replication slot '{slot_name}' from the SQLite rollback copy after upgrading to object-native Cayenne, so the source keeps WAL for that slot until it is dropped{detail}. Drop it only after the upgrade is committed and you will not roll back to the SQLite copy: `SELECT pg_drop_replication_slot('{slot_name}')`. See: {CONNECTOR_DOCS}"
            ))
        }
        RollbackSlotLookup::PermissionDenied { slot_name } => Some(format!(
            "Dataset '{dataset}' could not inspect PostgreSQL replication slot '{slot_name}' (the SQLite rollback copy's generated slot) because the role lacks permission, so any WAL that slot retains on the source is not reported. Grant SELECT on `pg_replication_slots` and EXECUTE on `pg_current_wal_lsn`, or inspect and drop the slot as a superuser after the upgrade is committed: `SELECT pg_drop_replication_slot('{slot_name}')`. See: {CONNECTOR_DOCS}"
        )),
        RollbackSlotLookup::Unreadable { slot_name, error } => Some(format!(
            "Dataset '{dataset}' could not inspect PostgreSQL replication slot '{slot_name}' (the SQLite rollback copy's generated slot), so any WAL that slot retains on the source is not reported. Cause: {error}. After the upgrade is committed and you will not roll back to the SQLite copy, drop it with `SELECT pg_drop_replication_slot('{slot_name}')`. See: {CONNECTOR_DOCS}"
        )),
    }
}

/// Log the leftover-slot warning, if any, and record the quantity on `metrics`.
pub fn report_rollback_slot_lookup(
    dataset: &str,
    lookup: &RollbackSlotLookup,
    metrics: &MetricsCollector,
) {
    if let Some(message) = rollback_slot_warning(dataset, lookup) {
        let slot = match lookup {
            RollbackSlotLookup::Inactive { slot_name, .. }
            | RollbackSlotLookup::PermissionDenied { slot_name }
            | RollbackSlotLookup::Unreadable { slot_name, .. } => slot_name.as_str(),
            RollbackSlotLookup::Absent | RollbackSlotLookup::Active => "",
        };
        tracing::warn!(dataset = %dataset, slot = %slot, "{message}");
    }
    if let RollbackSlotLookup::Inactive {
        slot_name,
        retained_wal_bytes,
        ..
    } = lookup
    {
        metrics.set_inactive_rollback_slot(slot_name.clone(), *retained_wal_bytes);
    }
}

fn format_age(seconds: u64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else {
        format!("{}h", seconds / 3600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn warning(lookup: &RollbackSlotLookup) -> String {
        rollback_slot_warning("users", lookup).expect("warning")
    }

    #[test]
    fn generated_rollback_slot_is_the_instance_scoped_name() {
        let location = "s3://bucket/spice";
        let rollback = generated_rollback_slot_name("orders", Some(location), false)
            .expect("location-scoped generated slots leave an instance-scoped leftover");
        assert_eq!(rollback, config::default_slot_name("orders"));
        assert_ne!(
            rollback,
            config::default_slot_name_on("orders", Some(location))
        );
    }

    #[test]
    fn explicit_or_unshared_slots_have_no_distinct_rollback_name() {
        assert_eq!(
            generated_rollback_slot_name("orders", Some("s3://bucket/spice"), true),
            None,
            "an explicit slot is shared by both copies"
        );
        assert_eq!(
            generated_rollback_slot_name("orders", None, false),
            None,
            "without a shared location the generated slot is already instance-scoped"
        );
    }

    #[test]
    fn absent_or_active_slots_are_not_a_leftover() {
        assert_eq!(
            rollback_slot_warning("users", &RollbackSlotLookup::Absent),
            None
        );
        assert_eq!(
            rollback_slot_warning("users", &RollbackSlotLookup::Active),
            None
        );
    }

    #[test]
    fn inactive_warning_names_the_dataset_slot_impact_drop_and_docs() {
        let message = warning(&RollbackSlotLookup::Inactive {
            slot_name: "spice_users_abc_def".into(),
            inactive_age_seconds: Some(7200),
            retained_wal_bytes: Some(1_048_576),
        });
        assert!(message.contains("Dataset 'users'"), "{message}");
        assert!(message.contains("'spice_users_abc_def'"), "{message}");
        assert!(message.contains("SQLite rollback copy"), "{message}");
        assert!(message.contains("source keeps WAL"), "{message}");
        assert!(message.contains("inactive for 2h"), "{message}");
        assert!(
            message.contains("retaining 1048576 bytes of WAL"),
            "{message}"
        );
        assert!(
            message.contains("SELECT pg_drop_replication_slot('spice_users_abc_def')"),
            "{message}"
        );
        assert!(
            message.contains("after the upgrade is committed"),
            "{message}"
        );
        assert!(message.contains(CONNECTOR_DOCS), "{message}");
        assert!(
            !message.contains('\n'),
            "user-facing warnings stay on one line: {message}"
        );
    }

    #[test]
    fn inactive_warning_degrades_when_age_or_bytes_are_unread() {
        let message = warning(&RollbackSlotLookup::Inactive {
            slot_name: "spice_users_abc_def".into(),
            inactive_age_seconds: None,
            retained_wal_bytes: None,
        });
        assert!(
            message.contains("retained WAL bytes could not be read"),
            "{message}"
        );
        assert!(
            !message.contains("retaining "),
            "unread bytes must not invent a quantity: {message}"
        );
        assert!(!message.contains("inactive for"), "{message}");
        let zero_age = warning(&RollbackSlotLookup::Inactive {
            slot_name: "spice_users_abc_def".into(),
            inactive_age_seconds: Some(0),
            retained_wal_bytes: Some(1),
        });
        assert!(
            !zero_age.contains("inactive for"),
            "a 0s age is not useful: {zero_age}"
        );
        assert!(
            message.contains("SELECT pg_drop_replication_slot('spice_users_abc_def')"),
            "{message}"
        );
        assert!(!message.contains('\n'), "{message}");
    }

    #[test]
    fn permission_denied_warning_names_the_grant_and_the_drop() {
        let message = warning(&RollbackSlotLookup::PermissionDenied {
            slot_name: "spice_users_abc_def".into(),
        });
        assert!(message.contains("Dataset 'users'"), "{message}");
        assert!(message.contains("'spice_users_abc_def'"), "{message}");
        assert!(message.contains("lacks permission"), "{message}");
        assert!(message.contains("`pg_replication_slots`"), "{message}");
        assert!(message.contains("`pg_current_wal_lsn`"), "{message}");
        assert!(
            message.contains("SELECT pg_drop_replication_slot('spice_users_abc_def')"),
            "{message}"
        );
        assert!(message.contains(CONNECTOR_DOCS), "{message}");
        assert!(!message.contains('\n'), "{message}");
    }

    #[test]
    fn unreadable_warning_carries_the_cause_and_the_drop() {
        let message = warning(&RollbackSlotLookup::Unreadable {
            slot_name: "spice_users_abc_def".into(),
            error: "connection reset".into(),
        });
        assert!(message.contains("connection reset"), "{message}");
        assert!(
            message.contains("SELECT pg_drop_replication_slot('spice_users_abc_def')"),
            "{message}"
        );
        assert!(message.contains(CONNECTOR_DOCS), "{message}");
        assert!(!message.contains('\n'), "{message}");
    }

    #[test]
    fn format_age_uses_the_largest_whole_unit() {
        assert_eq!(format_age(9), "9s");
        assert_eq!(format_age(59), "59s");
        assert_eq!(format_age(60), "1m");
        assert_eq!(format_age(3599), "59m");
        assert_eq!(format_age(3600), "1h");
        assert_eq!(format_age(7200), "2h");
    }
}
