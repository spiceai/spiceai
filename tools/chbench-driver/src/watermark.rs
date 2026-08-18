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

//! In-process `_bench_ts` high-water marks for staleness probing.
//!
//! The HTAP staleness probe and the drain gate both need
//! `MAX(_bench_ts)` for the *source* side of a comparison against Spice.
//! Reading it as `SELECT MAX(_bench_ts) FROM <table>` is a full clustered-index
//! scan (`_bench_ts` is unindexed), measured at ~48s on `order_line` at 300M
//! rows — so a nominal 5s probe cycle actually took ~50s and the 1s drain poll
//! was throttled to the same degree.
//!
//! The same driver process runs the OLTP workload *and* answers the probe, so
//! the maximum is known without asking the server: each transaction stamps
//! `_bench_ts` itself and, after its COMMIT succeeds, folds that stamp into a
//! per-table [`Watermarks`] entry. The probe then reads an atomic instead of
//! scanning a table.
//!
//! Two properties make this equal to the true source maximum rather than an
//! approximation:
//!
//! * The stamp is generated already rounded to the column's precision
//!   ([`BenchTs`]), so the value the server stores is bit-identical to the value
//!   recorded here. `DATETIME(3)` *rounds* rather than truncates on store, so an
//!   un-rounded microsecond value could land above the recorded watermark and
//!   the gate's `source_ts == spice_ts` could never hold.
//! * Only tables that a committed transaction actually wrote are recorded (see
//!   [`Touched`]). Recording a table a transaction merely could have written
//!   pushes the watermark above the true maximum, permanently — the gate would
//!   then never converge.
//!
//! [`DELETE_BEARING_TABLES`] is the one exception: `MAX` over surviving rows can
//! *decrease* when a row is deleted, which a monotone watermark cannot follow,
//! so those tables are always answered from the server.

use std::sync::atomic::{AtomicI64, Ordering};

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Timelike, Utc};

use crate::Result;

/// Tables mutated by TPC-C transactions. `_bench_ts` is added only to these;
/// `item`, `nation`, `region`, and `supplier` are static reference tables.
///
/// Shared by the `MySQL` schema module and [`Watermarks`], so the DDL and the
/// registry cannot drift apart. (The Postgres module still carries its own
/// list; it converges here when its watermark port lands.)
pub const MUTATED_TABLES: &[&str] = &[
    "warehouse",
    "district",
    "customer",
    "history",
    "new_order",
    "oorder",
    "order_line",
    "stock",
];

/// Tables whose `MAX(_bench_ts)` can *decrease*, because rows are deleted.
///
/// The delivery transaction deletes the order it delivers from `new_order`, and
/// it picks the lowest `no_o_id` — normally the oldest, lowest-`_bench_ts` row,
/// but when a district's queue holds exactly one row that row is both the oldest
/// and the newest, so deleting it drops the table's maximum. A [`Watermarks`]
/// entry is monotone by construction and can never follow that down, so these
/// tables are answered by [`crate::ChBenchDriver::max_bench_ts_exact`] instead.
pub const DELETE_BEARING_TABLES: &[&str] = &["new_order"];

/// Whether `table`'s maximum `_bench_ts` must be read from the server rather
/// than from the in-memory watermark. See [`DELETE_BEARING_TABLES`].
#[must_use]
pub fn is_delete_bearing(table: &str) -> bool {
    DELETE_BEARING_TABLES.contains(&table)
}

/// A TPC-C table that carries a `_bench_ts` column.
///
/// Indexes [`Watermarks`] directly, so recording a stamp is an array index and
/// an atomic max — no hashing, and no "unknown table" error path on the write
/// side of a hot transaction loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutatedTable {
    Warehouse,
    District,
    Customer,
    History,
    NewOrder,
    Oorder,
    OrderLine,
    Stock,
}

impl MutatedTable {
    /// All variants, in [`MUTATED_TABLES`] order.
    pub const ALL: [Self; 8] = [
        Self::Warehouse,
        Self::District,
        Self::Customer,
        Self::History,
        Self::NewOrder,
        Self::Oorder,
        Self::OrderLine,
        Self::Stock,
    ];

    /// The table's SQL name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warehouse => "warehouse",
            Self::District => "district",
            Self::Customer => "customer",
            Self::History => "history",
            Self::NewOrder => "new_order",
            Self::Oorder => "oorder",
            Self::OrderLine => "order_line",
            Self::Stock => "stock",
        }
    }

    /// Index of this table's slot in [`Watermarks`].
    const fn slot(self) -> usize {
        self as usize
    }

    /// Resolve a SQL table name, or `None` if the table carries no `_bench_ts`.
    #[must_use]
    pub fn from_name(table: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.as_str() == table)
    }
}

/// The set of `_bench_ts`-carrying tables a transaction has actually written.
///
/// Accumulated as the transaction runs and applied to [`Watermarks`] only after
/// its COMMIT returns `Ok`. Tracking what was *written* rather than what the
/// transaction type *could* write is load-bearing: a delivery whose ten
/// districts were all empty, or a payment that found no customer and
/// early-committed, writes fewer tables than its statement list suggests.
/// Recording an unwritten table pushes that watermark above the true source
/// maximum with no way back down, and the drain gate then waits out `max_wait`
/// on a table that is in fact fully replicated.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Touched(u8);

impl Touched {
    /// An empty set.
    #[must_use]
    pub const fn new() -> Self {
        Self(0)
    }

    /// Record that `table` was written.
    pub const fn add(&mut self, table: MutatedTable) {
        self.0 |= 1u8 << table.slot();
    }

    /// Whether `table` is in the set.
    #[must_use]
    pub const fn contains(self, table: MutatedTable) -> bool {
        self.0 & (1u8 << table.slot()) != 0
    }

    /// Whether no table was written.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The tables in the set.
    pub fn iter(self) -> impl Iterator<Item = MutatedTable> {
        MutatedTable::ALL
            .into_iter()
            .filter(move |t| self.contains(*t))
    }
}

/// Sentinel for a watermark that has not been seeded yet.
///
/// Not a valid low watermark: [`Watermarks::get`] maps it to an error rather
/// than to `Ok(None)`, because `Ok(None)` already means "the table is empty" and
/// both consumers treat that as benign — the staleness probe only samples the
/// `(Some, Some)` case and the drain gate keeps polling until `max_wait`. A
/// seeding bug would otherwise present as silently absent samples or a slow
/// timeout instead of a reported error.
const UNSEEDED: i64 = i64::MIN;

/// Per-table high-water mark of `_bench_ts`, in microseconds since the Unix
/// epoch.
///
/// Shared by every OLTP terminal and by the probe. The slots are fixed at
/// construction and only their values change, so `&self` is enough to record a
/// stamp and no lock is involved.
#[derive(Debug)]
pub struct Watermarks([AtomicI64; MutatedTable::ALL.len()]);

impl Default for Watermarks {
    fn default() -> Self {
        Self::new()
    }
}

impl Watermarks {
    /// A registry with every table unseeded.
    #[must_use]
    pub fn new() -> Self {
        Self(std::array::from_fn(|_| AtomicI64::new(UNSEEDED)))
    }

    /// Fold `micros` into every table's watermark.
    ///
    /// Used on the fresh-prepare path, where every seed row was stamped with the
    /// same load timestamp by the column default, so the initial maximum is
    /// known without querying anything.
    pub fn seed_all(&self, micros: i64) {
        for slot in &self.0 {
            slot.fetch_max(micros, Ordering::Relaxed);
        }
    }

    /// Seed one table's watermark, overwriting the unseeded sentinel.
    ///
    /// Used on the `--skip-prepare` path, where the value comes from a
    /// `SELECT MAX(_bench_ts)` against the restored source. `fetch_max` rather
    /// than `store` so a concurrent terminal write can never be lost, though the
    /// callers seed before any terminal is spawned.
    pub fn seed(&self, table: MutatedTable, micros: i64) {
        self.0[table.slot()].fetch_max(micros, Ordering::Relaxed);
    }

    /// Record a committed transaction's stamp against every table it wrote.
    ///
    /// Call only after COMMIT returns `Ok`: TPC-C rolls back ~1% of new-order
    /// transactions, and a watermark advanced by a rolled-back transaction
    /// exceeds the true source maximum, so the gate could never converge.
    pub fn record(&self, touched: Touched, ts: BenchTs) {
        for table in touched.iter() {
            self.0[table.slot()].fetch_max(ts.micros(), Ordering::Relaxed);
        }
    }

    /// The recorded maximum for `table`, in microseconds since the Unix epoch.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::UnknownWatermarkTable`] if `table` carries no
    /// `_bench_ts` column, and [`crate::Error::UnseededWatermark`] if the
    /// registry was never initialized for it.
    pub fn get(&self, table: &str) -> Result<Option<i64>> {
        let Some(t) = MutatedTable::from_name(table) else {
            return Err(crate::Error::UnknownWatermarkTable {
                table: table.to_string(),
            });
        };
        match self.0[t.slot()].load(Ordering::Relaxed) {
            UNSEEDED => Err(crate::Error::UnseededWatermark {
                table: table.to_string(),
            }),
            micros => Ok(Some(micros)),
        }
    }
}

/// A `_bench_ts` stamp, pre-rounded to its column's precision.
///
/// Both the value bound into the statement and the value folded into
/// [`Watermarks`] come from this one instant, and the instant carries no
/// precision the column cannot hold — so the server stores it verbatim and the
/// watermark equals the stored value exactly. Construct with [`BenchTs::now_mysql`]
/// for `DATETIME(3)` or [`BenchTs::now_postgres`] for `TIMESTAMPTZ`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BenchTs(DateTime<Utc>);

impl BenchTs {
    /// Now, rounded down to milliseconds for `MySQL`'s `DATETIME(3)`.
    ///
    /// `MySQL` *rounds* fractional seconds when storing into `DATETIME(3)`, so a
    /// microsecond-precision value can land strictly above what was recorded
    /// here. Truncating to the column's precision first makes the store exact.
    #[must_use]
    pub fn now_mysql() -> Self {
        Self::truncate_mysql(Utc::now())
    }

    /// Now, rounded down to microseconds for Postgres' `TIMESTAMPTZ`.
    #[must_use]
    pub fn now_postgres() -> Self {
        Self::truncate_postgres(Utc::now())
    }

    /// Round `at` down to millisecond precision.
    #[must_use]
    pub fn truncate_mysql(at: DateTime<Utc>) -> Self {
        Self(Self::truncate_nanos(at, 1_000_000))
    }

    /// Round `at` down to microsecond precision.
    #[must_use]
    pub fn truncate_postgres(at: DateTime<Utc>) -> Self {
        Self(Self::truncate_nanos(at, 1_000))
    }

    /// Round `at`'s sub-second part down to a multiple of `unit` nanoseconds.
    fn truncate_nanos(at: DateTime<Utc>, unit: u32) -> DateTime<Utc> {
        // `with_nanosecond` rejects only values >= 2_000_000_000 (leap-second
        // territory); truncating a nanosecond count downwards can never produce
        // one, so the input is returned unchanged if it somehow did.
        at.with_nanosecond((at.nanosecond() / unit) * unit)
            .unwrap_or(at)
    }

    /// Microseconds since the Unix epoch — the [`Watermarks`] representation and
    /// the unit both consumers compare in.
    #[must_use]
    pub fn micros(self) -> i64 {
        self.0.timestamp_micros()
    }

    /// The value to bind to a `MySQL` `DATETIME(3)` parameter.
    ///
    /// Naive because `DATETIME` carries no zone; terminal sessions are pinned to
    /// UTC (see `set_mysql_utc`), so a UTC-naive value is stored verbatim.
    #[must_use]
    pub fn mysql_value(self) -> NaiveDateTime {
        self.0.naive_utc()
    }

    /// The value to bind to a Postgres `TIMESTAMPTZ` parameter.
    #[must_use]
    pub fn postgres_value(self) -> DateTime<Utc> {
        self.0
    }

    /// A quoted `TIMESTAMPTZ` literal, for the one Postgres write path that
    /// builds SQL text instead of binding parameters (the new-order phase-2
    /// `batch_execute`).
    #[must_use]
    pub fn postgres_literal(self) -> String {
        format!("'{}'", self.0.to_rfc3339_opts(SecondsFormat::Micros, true))
    }

    /// A quoted `DATETIME(3)` literal, for use as a column `DEFAULT` at load
    /// time (see the schema modules' `add_bench_ts_columns`).
    #[must_use]
    pub fn mysql_literal(self) -> String {
        format!("'{}'", self.0.format("%Y-%m-%d %H:%M:%S%.3f"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stamp must survive a round-trip through its column's precision
    /// unchanged, or the watermark sits below the stored maximum forever and
    /// `source_ts == spice_ts` can never hold.
    #[test]
    fn stamps_carry_no_precision_the_column_cannot_hold() {
        // 123_456_789ns past the second: sub-millisecond and sub-microsecond
        // digits are both present, so truncation is observable at either width.
        let raw = DateTime::from_timestamp_nanos(1_753_000_000_123_456_789);

        let ms = BenchTs::truncate_mysql(raw);
        assert_eq!(ms.micros() % 1_000, 0, "ms stamp must have no µs remainder");
        assert_eq!(ms.mysql_value().and_utc().timestamp_micros(), ms.micros());
        assert_eq!(ms.mysql_literal(), "'2025-07-20 08:26:40.123'");

        let us = BenchTs::truncate_postgres(raw);
        assert_eq!(us.micros(), 1_753_000_000_123_456);
        assert_eq!(us.postgres_value().timestamp_micros(), us.micros());
        assert_eq!(us.postgres_literal(), "'2025-07-20T08:26:40.123456Z'");
    }

    /// Truncation must round *down*: a stamp above the stored value is the
    /// non-converging failure mode this guards.
    #[test]
    fn truncation_never_rounds_up() {
        // .999999999s rounds up to the next second under round-half-up.
        let raw = DateTime::from_timestamp_nanos(1_753_000_000_999_999_999);
        assert_eq!(BenchTs::truncate_mysql(raw).micros(), 1_753_000_000_999_000);
        assert_eq!(
            BenchTs::truncate_postgres(raw).micros(),
            1_753_000_000_999_999
        );
    }

    #[test]
    fn mutated_table_names_match_the_ddl_list() {
        let names: Vec<&str> = MutatedTable::ALL.iter().map(|t| t.as_str()).collect();
        assert_eq!(names, MUTATED_TABLES);
        for table in MUTATED_TABLES {
            assert_eq!(
                MutatedTable::from_name(table).map(MutatedTable::as_str),
                Some(*table)
            );
        }
        assert_eq!(MutatedTable::from_name("item"), None);
    }

    #[test]
    fn touched_records_only_added_tables() {
        let mut touched = Touched::new();
        assert!(touched.is_empty());
        touched.add(MutatedTable::Stock);
        touched.add(MutatedTable::OrderLine);
        touched.add(MutatedTable::Stock); // idempotent
        assert!(!touched.is_empty());
        assert_eq!(
            touched.iter().collect::<Vec<_>>(),
            vec![MutatedTable::OrderLine, MutatedTable::Stock]
        );
        assert!(!touched.contains(MutatedTable::Customer));
    }

    #[test]
    fn unseeded_watermark_is_an_error_not_an_empty_table() {
        let w = Watermarks::new();
        assert!(matches!(
            w.get("stock"),
            Err(crate::Error::UnseededWatermark { .. })
        ));
        assert!(matches!(
            w.get("item"),
            Err(crate::Error::UnknownWatermarkTable { .. })
        ));
    }

    #[test]
    fn watermark_advances_only_for_touched_tables_and_never_regresses() {
        let w = Watermarks::new();
        w.seed_all(1_000);
        assert_eq!(w.get("stock").expect("seeded"), Some(1_000));
        assert_eq!(w.get("customer").expect("seeded"), Some(1_000));

        let mut touched = Touched::new();
        touched.add(MutatedTable::Stock);
        w.record(
            touched,
            BenchTs(DateTime::from_timestamp_micros(5_000).expect("valid")),
        );
        assert_eq!(w.get("stock").expect("seeded"), Some(5_000));
        assert_eq!(
            w.get("customer").expect("seeded"),
            Some(1_000),
            "an untouched table must not advance"
        );

        // An out-of-order stamp (two terminals racing) must not pull the
        // watermark back down.
        w.record(
            touched,
            BenchTs(DateTime::from_timestamp_micros(2_000).expect("valid")),
        );
        assert_eq!(w.get("stock").expect("seeded"), Some(5_000));
    }
}
