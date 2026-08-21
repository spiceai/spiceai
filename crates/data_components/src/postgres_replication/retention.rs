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

//! What a replication slot costs while it exists, and what will remove it.
//!
//! An inactive logical replication slot retains WAL until something drops it,
//! and a full source disk is the universal `PostgreSQL`-CDC failure mode. Three
//! different things can end that retention, only one of which the operator has
//! to do by hand, so the operator needs to be told *which one applies to this
//! slot* rather than which ones exist.
//!
//! This module holds the two halves of answering that:
//!
//!   * [`SlotRetentionPosture`] — what the server does on its own, read from its
//!     settings during setup;
//!   * [`SlotRemoval`] and [`slot_lifetime_message`] — the resolved answer and
//!     the one templated line that states it.
//!
//! Spice reads these settings and never writes them.
//! `idle_replication_slot_timeout` is a server-wide GUC governing every slot on
//! the server, including other systems', and `ALTER SYSTEM` needs superuser — so
//! setting it is the operator's call, on their whole server, not a side effect of
//! starting a dataset.

use snafu::ResultExt;

use super::{Result, SetupExecSnafu};

/// `server_version_num` for `PostgreSQL` 18.0, the first release with
/// `idle_replication_slot_timeout`.
const PG_18: i32 = 180_000;

/// The value recommended where `PostgreSQL` could retire idle slots but is not
/// configured to, in seconds.
///
/// Concrete on purpose: a parameter name plus "size it yourself" is not something
/// an operator can act on. With rebuilding automatic and correct, reclaiming too
/// eagerly costs a re-read that recovers on its own, while reclaiming too late
/// costs a full disk — so the recommendation sits at the low end of the range
/// that survives a routine restart.
const RECOMMENDED_IDLE_TIMEOUT_SECONDS: u64 = 120;

/// What the source server will do, unprompted, about a replication slot nobody is
/// consuming.
///
/// Read once during setup rather than sampled: these are server settings, and a
/// change to them is a server restart or reload away, which is also a Spice
/// restart in every deployment that cares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotRetentionPosture {
    /// `server_version_num`, e.g. `170004` or `180000`.
    pub server_version_num: i32,
    /// `idle_replication_slot_timeout`, normalised to seconds.
    ///
    /// `None` on a server without the setting at all (pre-18), which is a
    /// different statement from `Some(0)` — the latter means the server *could*
    /// retire idle slots and has been left with it disabled, which is the
    /// default and is worth a recommendation. The former means there is nothing
    /// to recommend, and saying so would be noise.
    pub idle_timeout_seconds: Option<u64>,
}

impl SlotRetentionPosture {
    /// Whether the server retires slots left idle, on its own.
    #[must_use]
    pub fn retires_idle_slots(&self) -> bool {
        self.idle_timeout_seconds.is_some_and(|seconds| seconds > 0)
    }

    /// Whether the server *could* retire idle slots if configured to.
    #[must_use]
    pub fn can_retire_idle_slots(&self) -> bool {
        self.idle_timeout_seconds.is_some() || self.server_version_num >= PG_18
    }
}

/// Read the server's slot-retention posture.
///
/// `idle_replication_slot_timeout` is looked up through `pg_settings` rather than
/// `current_setting()`, so a server that has never heard of it returns no row
/// instead of raising `unrecognized_configuration_parameter` — no version gate
/// and no error to swallow. `pg_settings.unit` also makes the value unambiguous
/// rather than parsed out of a display string.
pub async fn read_posture(client: &tokio_postgres::Client) -> Result<SlotRetentionPosture> {
    let server_version_num: i32 = client
        .query_one("SELECT current_setting('server_version_num')::int4", &[])
        .await
        .context(SetupExecSnafu)?
        .get(0);

    let idle_timeout_seconds = client
        .query_opt(
            "SELECT setting, unit FROM pg_catalog.pg_settings \
             WHERE name = 'idle_replication_slot_timeout'",
            &[],
        )
        .await
        .context(SetupExecSnafu)?
        .and_then(|row| {
            let setting: String = row.get(0);
            let unit: Option<String> = row.get(1);
            guc_seconds(&setting, unit.as_deref())
        });

    Ok(SlotRetentionPosture {
        server_version_num,
        idle_timeout_seconds,
    })
}

/// Normalise a time-valued `pg_settings` row to seconds.
///
/// The unit is whatever the server reports for the parameter, and it has changed
/// across releases for this one, so it is read rather than assumed. Sub-second
/// units round *up* to 1s: this feeds a message about roughly how long a slot
/// survives, and reporting a configured-but-tiny timeout as `0` would render it
/// as "disabled" — the opposite of what is set.
fn guc_seconds(setting: &str, unit: Option<&str>) -> Option<u64> {
    let value: u64 = setting.trim().parse().ok()?;
    match unit.unwrap_or("s") {
        "s" | "" => Some(value),
        "min" => Some(value.saturating_mul(60)),
        "h" => Some(value.saturating_mul(60 * 60)),
        "d" => Some(value.saturating_mul(24 * 60 * 60)),
        "ms" => Some(if value == 0 {
            0
        } else {
            value.div_ceil(1000).max(1)
        }),
        // An unrecognised unit is not a number we can compare against a
        // duration, and guessing would put a wrong figure in front of an
        // operator. Absent is the honest answer.
        _ => None,
    }
}

/// What will remove a replication slot Spice created.
///
/// Resolved from two facts, and exactly one of these applies to any given slot,
/// which is what makes a single templated line possible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotRemoval {
    /// Spice drops it at shutdown, because the acceleration it feeds starts empty
    /// on every restart and re-runs its initial snapshot — so the slot has no
    /// resume value to preserve.
    SpiceAtShutdown,
    /// The server retires it on its own once nothing consumes it for this long.
    ServerIdleTimeout { after_seconds: u64 },
    /// Nothing. Spice reuses the slot across restarts and never drops it, so
    /// removing Spice leaves it retaining WAL until an operator drops it.
    Nothing {
        /// Whether the server could retire it if `idle_replication_slot_timeout`
        /// were set, which decides whether there is anything to recommend.
        server_can_retire_idle_slots: bool,
    },
}

impl SlotRemoval {
    /// Resolve which one applies. `slot_is_disposable` is the caller's
    /// `ReplicationParams::slot_is_disposable`.
    ///
    /// Spice dropping the slot takes precedence over the server's idle timeout
    /// deliberately: where both could apply, Spice's drop is what actually
    /// happens, and it happens at shutdown rather than after an idle period the
    /// slot never reaches. Naming the timeout there would describe a mechanism
    /// that never fires.
    #[must_use]
    pub fn resolve(slot_is_disposable: bool, posture: SlotRetentionPosture) -> Self {
        if slot_is_disposable {
            return Self::SpiceAtShutdown;
        }
        match posture.idle_timeout_seconds {
            Some(after_seconds) if after_seconds > 0 => Self::ServerIdleTimeout { after_seconds },
            _ => Self::Nothing {
                server_can_retire_idle_slots: posture.can_retire_idle_slots(),
            },
        }
    }
}

/// The one line stating what a slot costs and what removes it.
///
/// Emitted when the slot is created and again at graceful shutdown. The repeat is
/// the point: creation time is when an operator can do least about it, and by the
/// time Spice is being decommissioned — which is when an abandoned slot starts
/// costing disk — that line is behind log rotation.
///
/// Every duration is stated as approximate, because the effective one is:
/// `PostgreSQL` rounds `idle_replication_slot_timeout` to the nearest minute and
/// only acts on it at a checkpoint (`checkpoint_timeout`, 5 minutes by default),
/// so the configured number is a floor rather than a deadline.
#[must_use]
pub fn slot_lifetime_message(slot_name: &str, removal: SlotRemoval) -> String {
    let removed_by = match removal {
        SlotRemoval::SpiceAtShutdown => {
            "Spice drops it at shutdown and creates a replacement at startup, so it retains WAL \
             only while Spice is running."
                .to_string()
        }
        SlotRemoval::ServerIdleTimeout { after_seconds } => format!(
            "PostgreSQL will invalidate it once nothing has consumed it for roughly {after_seconds}s \
             (idle_replication_slot_timeout; the effective delay is longer, because PostgreSQL rounds \
             it to the nearest minute and only invalidates at a checkpoint), and Spice rebuilds this \
             acceleration from the source if it was down longer than that."
        ),
        SlotRemoval::Nothing {
            server_can_retire_idle_slots: true,
        } => format!(
            "Nothing removes it: Spice reuses it across restarts and never drops it, so removing \
             Spice leaves it retaining WAL until you drop it with \
             `SELECT pg_drop_replication_slot('{slot_name}');`. To have PostgreSQL retire abandoned \
             slots instead, set idle_replication_slot_timeout = '{RECOMMENDED_IDLE_TIMEOUT_SECONDS}s' \
             on the source — it is server-wide and applies to every slot on it, the effective delay \
             is longer and fuzzier than the value (PostgreSQL rounds to the nearest minute and only \
             invalidates at a checkpoint), and Spice rebuilds any acceleration whose slot it \
             invalidates, so set it above your longest planned downtime if an occasional full \
             re-read of the source is unacceptable."
        ),
        SlotRemoval::Nothing {
            server_can_retire_idle_slots: false,
        } => format!(
            "Nothing removes it: Spice reuses it across restarts and never drops it, and this \
             server is too old to retire idle slots itself (PostgreSQL 18 added \
             idle_replication_slot_timeout), so removing Spice leaves it retaining WAL until you \
             drop it with `SELECT pg_drop_replication_slot('{slot_name}');`. Until then, \
             max_slot_wal_keep_size is the only bound on what it retains."
        ),
    };
    format!(
        "Replication slot `{slot_name}` retains WAL on the source for as long as it exists, so the \
         source's disk grows whenever Spice is not consuming it. {removed_by} Every slot Spice \
         creates is named `spice_…`: `SELECT slot_name, active, wal_status, \
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained FROM \
         pg_replication_slots WHERE slot_name LIKE 'spice_%';` lists them, including after Spice is \
         uninstalled."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRE_18: SlotRetentionPosture = SlotRetentionPosture {
        server_version_num: 170_004,
        idle_timeout_seconds: None,
    };
    const PG18_DISABLED: SlotRetentionPosture = SlotRetentionPosture {
        server_version_num: PG_18,
        idle_timeout_seconds: Some(0),
    };
    const PG18_ENABLED: SlotRetentionPosture = SlotRetentionPosture {
        server_version_num: PG_18,
        idle_timeout_seconds: Some(120),
    };

    #[test]
    fn a_setting_is_normalised_from_whatever_unit_the_server_reports() {
        assert_eq!(guc_seconds("120", Some("s")), Some(120));
        assert_eq!(guc_seconds("2", Some("min")), Some(120));
        assert_eq!(guc_seconds("1", Some("h")), Some(3600));
        assert_eq!(guc_seconds("0", Some("s")), Some(0));
        // No unit reported: `idle_replication_slot_timeout` is a time value, so
        // a bare number is seconds.
        assert_eq!(guc_seconds("60", None), Some(60));
        // A configured-but-sub-second timeout must not render as "disabled".
        assert_eq!(guc_seconds("500", Some("ms")), Some(1));
        assert_eq!(guc_seconds("0", Some("ms")), Some(0));
        // Nothing comparable to a duration: absent beats a guessed figure.
        assert_eq!(guc_seconds("8", Some("MB")), None);
        assert_eq!(guc_seconds("on", Some("s")), None);
    }

    #[test]
    fn a_pre_18_server_has_nothing_to_recommend() {
        // The distinction that keeps the recommendation off servers that cannot
        // act on it: absent is not the same as present-and-disabled.
        assert!(!PRE_18.can_retire_idle_slots());
        assert!(!PRE_18.retires_idle_slots());
        assert!(PG18_DISABLED.can_retire_idle_slots());
        assert!(!PG18_DISABLED.retires_idle_slots());
        assert!(PG18_ENABLED.retires_idle_slots());
    }

    #[test]
    fn a_server_without_the_setting_is_still_recognised_by_version() {
        // A server new enough to have it, whose `pg_settings` read came back
        // empty, must not be told it is too old to have it.
        let posture = SlotRetentionPosture {
            server_version_num: PG_18,
            idle_timeout_seconds: None,
        };
        assert!(posture.can_retire_idle_slots());
    }

    #[test]
    fn each_removal_clause_is_selected_for_its_own_configuration() {
        // A disposable slot is dropped by Spice whatever the server would do,
        // because that is what actually happens to it.
        assert_eq!(
            SlotRemoval::resolve(true, PG18_ENABLED),
            SlotRemoval::SpiceAtShutdown
        );
        assert_eq!(
            SlotRemoval::resolve(true, PRE_18),
            SlotRemoval::SpiceAtShutdown
        );
        assert_eq!(
            SlotRemoval::resolve(false, PG18_ENABLED),
            SlotRemoval::ServerIdleTimeout { after_seconds: 120 }
        );
        assert_eq!(
            SlotRemoval::resolve(false, PG18_DISABLED),
            SlotRemoval::Nothing {
                server_can_retire_idle_slots: true
            }
        );
        assert_eq!(
            SlotRemoval::resolve(false, PRE_18),
            SlotRemoval::Nothing {
                server_can_retire_idle_slots: false
            }
        );
    }

    #[test]
    fn every_message_names_the_slot_and_stays_on_one_line() {
        for removal in [
            SlotRemoval::SpiceAtShutdown,
            SlotRemoval::ServerIdleTimeout { after_seconds: 120 },
            SlotRemoval::Nothing {
                server_can_retire_idle_slots: true,
            },
            SlotRemoval::Nothing {
                server_can_retire_idle_slots: false,
            },
        ] {
            let message = slot_lifetime_message("spice_orders_1a2b", removal);
            assert!(message.contains("spice_orders_1a2b"), "{message}");
            assert!(!message.contains('\n'), "{message}");
            // The query that finds abandoned slots is the whole point of the
            // prefix, so it travels with every form of the message.
            assert!(message.contains("LIKE 'spice_%'"), "{message}");
        }
    }

    #[test]
    fn only_the_configuration_that_can_act_on_it_gets_the_recommendation() {
        let recommended = slot_lifetime_message(
            "s",
            SlotRemoval::Nothing {
                server_can_retire_idle_slots: true,
            },
        );
        assert!(
            recommended.contains(&format!(
                "idle_replication_slot_timeout = '{RECOMMENDED_IDLE_TIMEOUT_SECONDS}s'"
            )),
            "{recommended}"
        );
        // Recommending a setting a server does not have is noise, so the
        // pre-18 clause says what bounds retention there instead.
        let too_old = slot_lifetime_message(
            "s",
            SlotRemoval::Nothing {
                server_can_retire_idle_slots: false,
            },
        );
        assert!(
            !too_old.contains("idle_replication_slot_timeout = "),
            "{too_old}"
        );
        assert!(too_old.contains("max_slot_wal_keep_size"), "{too_old}");
        // A slot the server is already retiring needs no recommendation either.
        let already =
            slot_lifetime_message("s", SlotRemoval::ServerIdleTimeout { after_seconds: 120 });
        assert!(
            !already.contains("idle_replication_slot_timeout = "),
            "{already}"
        );
    }

    #[test]
    fn every_stated_delay_is_approximate() {
        // PostgreSQL rounds the timeout to the nearest minute and only acts at a
        // checkpoint, so any clause quoting the number has to say the effective
        // delay is longer — otherwise it reads as a deadline.
        for removal in [
            SlotRemoval::ServerIdleTimeout { after_seconds: 120 },
            SlotRemoval::Nothing {
                server_can_retire_idle_slots: true,
            },
        ] {
            let message = slot_lifetime_message("s", removal);
            assert!(message.contains("nearest minute"), "{message}");
            assert!(message.contains("checkpoint"), "{message}");
        }
    }
}
