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

//! Translation of `date` column filters into IMAP `SEARCH` criteria.
//!
//! A scan that cannot narrow server-side has to fetch every message in the
//! mailbox, so an acceleration refresh costs the whole mailbox each cycle rather
//! than the new mail since the last one (see #11548). Translating the filters
//! into `SEARCH` criteria lets the server pick the candidate messages, which is
//! what makes `refresh_mode: append` and `refresh_data_window` incremental for
//! this connector.

use chrono::{DateTime, Days, NaiveDate, Utc};
use datafusion::{
    logical_expr::{BinaryExpr, Expr, Operator},
    scalar::ScalarValue,
};

/// The message send-time column, parsed from the RFC822 `Date:` header.
const DATE_COLUMN: &str = "date";

/// Days of slack applied to every bound.
///
/// The IMAP `SENTSINCE`/`SENTBEFORE` keys compare the `Date:` header
/// "disregarding time and timezone" (RFC 3501 §6.4.4), while the `date` column
/// holds an absolute UTC instant. A header offset as far as ±14:00 from UTC puts
/// the header's own calendar day one day either side of the UTC day of the same
/// instant, so widening by a day keeps the server-side result a superset of the
/// predicate — never dropping a row the filter would have kept.
const SKEW_DAYS: u64 = 1;

/// Every message in the mailbox, as an IMAP identifier set.
pub(crate) const ALL_MESSAGES: &str = "1:*";

/// The IMAP `SEARCH` criteria matching a superset of the messages that satisfy
/// `filters`, or `None` when nothing could be translated and the whole mailbox
/// has to be fetched.
pub(crate) fn search_criteria<'a>(filters: impl IntoIterator<Item = &'a Expr>) -> Option<String> {
    let mut keys = Vec::new();
    for filter in filters {
        collect_keys(filter, &mut keys);
    }

    (!keys.is_empty()).then(|| keys.join(" "))
}

/// Collect the `SEARCH` keys implied by `expr`.
///
/// An expression shape that isn't understood contributes no key, which only
/// widens the fetch. Narrowing it wrongly would drop rows, so anything but a
/// conjunction of recognized `date` comparisons is left to the caller's filter.
fn collect_keys(expr: &Expr, keys: &mut Vec<String>) {
    match expr {
        // `SEARCH` ANDs its keys together, so each side of a conjunction can
        // contribute independently.
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            collect_keys(left, keys);
            collect_keys(right, keys);
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            collect_date_keys(left, *op, right, keys);
        }
        _ => {}
    }
}

/// Collect the keys bounding `date` for a `date <op> <timestamp>` comparison
/// (or its mirror image, `<timestamp> <op> date`).
fn collect_date_keys(left: &Expr, op: Operator, right: &Expr, keys: &mut Vec<String>) {
    let (value, op) = match (left, right) {
        (Expr::Column(column), Expr::Literal(value, _)) if column.name == DATE_COLUMN => {
            (value, op)
        }
        (Expr::Literal(value, _), Expr::Column(column)) if column.name == DATE_COLUMN => {
            (value, mirror(op))
        }
        _ => return,
    };

    let Some(day) = epoch_millis(value).and_then(utc_day) else {
        return;
    };

    // A lower bound becomes `SENTSINCE`, an upper bound `SENTBEFORE`, and an
    // equality bounds the day from both sides.
    if matches!(op, Operator::Gt | Operator::GtEq | Operator::Eq) {
        keys.extend(
            day.checked_sub_days(Days::new(SKEW_DAYS))
                .map(|since| format!("SENTSINCE {}", imap_date(since))),
        );
    }
    if matches!(op, Operator::Lt | Operator::LtEq | Operator::Eq) {
        // `SENTBEFORE` is exclusive, so the day itself needs one more day on top
        // of the skew allowance to stay included.
        keys.extend(
            day.checked_add_days(Days::new(SKEW_DAYS + 1))
                .map(|before| format!("SENTBEFORE {}", imap_date(before))),
        );
    }
}

/// The operator with its operands swapped: `5 < date` is `date > 5`.
fn mirror(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// A timestamp or date literal as milliseconds since the Unix epoch.
///
/// Sub-millisecond units floor towards negative infinity so that instants before
/// 1970 land on the calendar day that contains them rather than the one after.
fn epoch_millis(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampSecond(Some(seconds), _) => seconds.checked_mul(1_000),
        // `Date64` also counts milliseconds since the epoch.
        ScalarValue::TimestampMillisecond(Some(millis), _) | ScalarValue::Date64(Some(millis)) => {
            Some(*millis)
        }
        ScalarValue::TimestampMicrosecond(Some(micros), _) => Some(micros.div_euclid(1_000)),
        ScalarValue::TimestampNanosecond(Some(nanos), _) => Some(nanos.div_euclid(1_000_000)),
        ScalarValue::Date32(Some(days)) => i64::from(*days).checked_mul(86_400_000),
        _ => None,
    }
}

/// The UTC calendar day containing `millis`.
fn utc_day(millis: i64) -> Option<NaiveDate> {
    DateTime::<Utc>::from_timestamp_millis(millis).map(|instant| instant.date_naive())
}

/// Format as the RFC 3501 `date-text` a `SEARCH` key expects, e.g. `01-Jul-2026`.
fn imap_date(date: NaiveDate) -> String {
    date.format("%d-%b-%Y").to_string()
}

/// Collapse ascending message identifiers into an IMAP identifier set such as
/// `2,5:7,9`, or `None` when there are none to fetch.
///
/// Contiguous runs collapse into ranges, which keeps the `UID FETCH` command
/// short for the usual case of matching a recent stretch of the mailbox.
pub(crate) fn id_set(ids: &[u32]) -> Option<String> {
    let mut runs: Vec<String> = Vec::new();
    let mut ids = ids.iter().copied();
    let mut start = ids.next()?;
    let mut end = start;

    for id in ids {
        if id == end || id == end.saturating_add(1) {
            end = id;
            continue;
        }

        runs.push(run(start, end));
        start = id;
        end = id;
    }
    runs.push(run(start, end));

    Some(runs.join(","))
}

fn run(start: u32, end: u32) -> String {
    if start == end {
        start.to_string()
    } else {
        format!("{start}:{end}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    /// `2026-07-01T12:00:00Z`, midday so a day's widening is unambiguous.
    const JULY_FIRST: i64 = 1_782_907_200_000;

    /// The same day as [`JULY_FIRST`], as days since the epoch.
    const JULY_FIRST_DAY32: i32 = 20_635;

    fn timestamp(millis: i64) -> Expr {
        lit(ScalarValue::TimestampMillisecond(Some(millis), None))
    }

    #[test]
    fn lower_bound_becomes_a_widened_sentsince() {
        // `SENTSINCE` compares the `Date:` header's own calendar day, which can
        // trail the UTC day by one for a far-west timezone offset, so the bound
        // is the day before — a superset the caller then filters exactly.
        for op in [Operator::Gt, Operator::GtEq] {
            let filter = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("date")),
                op,
                Box::new(timestamp(JULY_FIRST)),
            ));
            assert_eq!(
                search_criteria([&filter]).as_deref(),
                Some("SENTSINCE 30-Jun-2026"),
                "unexpected criteria for {op}"
            );
        }
    }

    #[test]
    fn upper_bound_becomes_a_widened_sentbefore() {
        // `SENTBEFORE` is exclusive, so covering all of 1 July needs 2 July, plus
        // one more day of timezone slack.
        for op in [Operator::Lt, Operator::LtEq] {
            let filter = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("date")),
                op,
                Box::new(timestamp(JULY_FIRST)),
            ));
            assert_eq!(
                search_criteria([&filter]).as_deref(),
                Some("SENTBEFORE 03-Jul-2026"),
                "unexpected criteria for {op}"
            );
        }
    }

    #[test]
    fn equality_bounds_the_day_from_both_sides() {
        let filter = col("date").eq(timestamp(JULY_FIRST));
        assert_eq!(
            search_criteria([&filter]).as_deref(),
            Some("SENTSINCE 30-Jun-2026 SENTBEFORE 03-Jul-2026")
        );
    }

    #[test]
    fn literal_on_the_left_mirrors_the_operator() {
        // `'2026-07-01' < date` is the same lower bound as `date > '2026-07-01'`.
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(timestamp(JULY_FIRST)),
            Operator::Lt,
            Box::new(col("date")),
        ));
        assert_eq!(
            search_criteria([&filter]).as_deref(),
            Some("SENTSINCE 30-Jun-2026")
        );
    }

    #[test]
    fn conjunction_contributes_both_bounds() {
        // The window an append refresh with `refresh_data_window` produces.
        let filter = col("date")
            .gt(timestamp(JULY_FIRST))
            .and(col("date").lt(timestamp(JULY_FIRST + 86_400_000)));
        assert_eq!(
            search_criteria([&filter]).as_deref(),
            Some("SENTSINCE 30-Jun-2026 SENTBEFORE 04-Jul-2026")
        );
    }

    #[test]
    fn separate_filters_are_all_applied() {
        let lower = col("date").gt(timestamp(JULY_FIRST));
        let upper = col("date").lt(timestamp(JULY_FIRST));
        assert_eq!(
            search_criteria([&lower, &upper]).as_deref(),
            Some("SENTSINCE 30-Jun-2026 SENTBEFORE 03-Jul-2026")
        );
    }

    #[test]
    fn every_timestamp_unit_resolves_to_the_same_day() {
        let seconds = JULY_FIRST / 1_000;
        for value in [
            ScalarValue::TimestampSecond(Some(seconds), None),
            ScalarValue::TimestampMillisecond(Some(JULY_FIRST), None),
            ScalarValue::TimestampMicrosecond(Some(JULY_FIRST * 1_000), None),
            ScalarValue::TimestampNanosecond(Some(JULY_FIRST * 1_000_000), None),
            ScalarValue::Date64(Some(JULY_FIRST)),
            ScalarValue::Date32(Some(JULY_FIRST_DAY32)),
        ] {
            let filter = col("date").gt(lit(value.clone()));
            assert_eq!(
                search_criteria([&filter]).as_deref(),
                Some("SENTSINCE 30-Jun-2026"),
                "unexpected criteria for {value:?}"
            );
        }
    }

    #[test]
    fn timezone_carrying_timestamp_is_still_an_absolute_instant() {
        // A `Timestamp(_, Some(tz))` literal still counts epoch milliseconds, so
        // the bound must match the timezone-less case rather than be skipped.
        let filter = col("date").gt(lit(ScalarValue::TimestampMillisecond(
            Some(JULY_FIRST),
            Some("America/New_York".into()),
        )));
        assert_eq!(
            search_criteria([&filter]).as_deref(),
            Some("SENTSINCE 30-Jun-2026")
        );
    }

    #[test]
    fn instants_before_the_epoch_land_on_the_containing_day() {
        // Flooring matters here: truncating towards zero would report 1 January
        // 1970 and wrongly exclude the last day of 1969.
        let filter = col("date").gt(lit(ScalarValue::TimestampNanosecond(
            Some(-1_000_000),
            None,
        )));
        assert_eq!(
            search_criteria([&filter]).as_deref(),
            Some("SENTSINCE 30-Dec-1969")
        );
    }

    #[test]
    fn untranslatable_filters_yield_no_criteria() {
        // Each of these must fetch wide and let the caller filter, rather than
        // narrow on a guess: a disjunction, another column, a null bound, a
        // non-temporal literal, an inequality, and a bare column reference.
        let disjunction = col("date")
            .gt(timestamp(JULY_FIRST))
            .or(col("date").lt(timestamp(0)));
        let other_column = col("subject").eq(lit("hello"));
        let null_bound = col("date").gt(lit(ScalarValue::TimestampMillisecond(None, None)));
        let not_a_timestamp = col("date").gt(lit(7_i64));
        let not_equal = col("date").not_eq(timestamp(JULY_FIRST));
        let bare_column = col("date");

        for filter in [
            disjunction,
            other_column,
            null_bound,
            not_a_timestamp,
            not_equal,
            bare_column,
        ] {
            assert_eq!(
                search_criteria([&filter]),
                None,
                "unexpectedly narrowed on {filter}"
            );
        }
    }

    #[test]
    fn conjunction_keeps_the_translatable_half() {
        // A conjunction is safe to narrow on one side alone: the caller still
        // applies both predicates.
        let filter = col("date")
            .gt(timestamp(JULY_FIRST))
            .and(col("subject").eq(lit("hello")));
        assert_eq!(
            search_criteria([&filter]).as_deref(),
            Some("SENTSINCE 30-Jun-2026")
        );
    }

    #[test]
    fn id_set_collapses_runs() {
        assert_eq!(id_set(&[1, 2, 3]).as_deref(), Some("1:3"));
        assert_eq!(id_set(&[7]).as_deref(), Some("7"));
        assert_eq!(id_set(&[1, 2, 3, 5, 8, 9]).as_deref(), Some("1:3,5,8:9"));
        assert_eq!(id_set(&[2, 4, 6]).as_deref(), Some("2,4,6"));
    }

    #[test]
    fn id_set_of_nothing_is_none() {
        // No match must mean no fetch at all — `1:0` is not a valid identifier set.
        assert_eq!(id_set(&[]), None);
    }

    #[test]
    fn id_set_handles_the_largest_representable_id() {
        // `end + 1` must not overflow when a UID reaches `u32::MAX`.
        assert_eq!(
            id_set(&[u32::MAX - 1, u32::MAX]).as_deref(),
            Some("4294967294:4294967295")
        );
    }
}
