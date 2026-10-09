// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The reduced ClickBench `hits` fixture every oracle lane loads when
//! `CLICKBENCH_HITS_PARQUET` does not point at the real dump.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};

/// ClickBench-like hits table with **unique top-K ranking keys**.
///
/// Group-by dimensions used in `ORDER BY count DESC LIMIT N` queries
/// (`RegionID`, `SearchPhrase`, `URL`, `Title`, `ClientIP`, `WatchID`) are
/// assigned power-law frequencies so every group has a distinct count. That
/// makes top-K order deterministic across Cayenne / DataFusion / DuckDB —
/// content equality is a real correctness check, not tie-break noise.
#[must_use]
pub fn make_reduced_hits(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("WatchID", DataType::Int64, false),
        Field::new("UserID", DataType::Int64, false),
        Field::new("CounterID", DataType::Int64, false),
        Field::new("AdvEngineID", DataType::Int64, false),
        Field::new("RegionID", DataType::Int64, false),
        Field::new("ResolutionWidth", DataType::UInt32, false),
        Field::new("EventDate", DataType::Int64, false),
        Field::new("EventTime", DataType::Int64, false),
        Field::new("IsRefresh", DataType::Int64, false),
        Field::new("DontCountHits", DataType::Int64, false),
        Field::new("SearchPhrase", DataType::Utf8, false),
        Field::new("URL", DataType::Utf8, false),
        Field::new("Title", DataType::Utf8, false),
        Field::new("Referer", DataType::Utf8, false),
        Field::new("TraficSourceID", DataType::Int64, false),
        Field::new("SearchEngineID", DataType::Int64, false),
        Field::new("IsLink", DataType::Int64, false),
        Field::new("IsDownload", DataType::Int64, false),
        Field::new("ClientIP", DataType::Int64, false),
        Field::new("MobilePhone", DataType::Int64, false),
        Field::new("MobilePhoneModel", DataType::Utf8, false),
        Field::new("URLHash", DataType::Int64, false),
        Field::new("RefererHash", DataType::Int64, false),
        Field::new("WindowClientWidth", DataType::UInt32, false),
        Field::new("WindowClientHeight", DataType::UInt32, false),
    ]));

    // Power-law: group `g` has `row_count[g]` rows and `distinct_users[g]`
    // distinct UserIDs — both strictly decreasing in g so:
    //   ORDER BY COUNT(*) DESC          and
    //   ORDER BY COUNT(DISTINCT UserID) DESC
    // yield a unique top-K with no ties.
    let n_groups = 40usize;
    let mut row_count: Vec<usize> = (0..n_groups).map(|g| n_groups - g).collect();
    let base_sum: usize = row_count.iter().sum();
    let scale = (rows / base_sum).max(1);
    for c in &mut row_count {
        *c *= scale;
    }
    let assigned: usize = row_count.iter().sum();
    if assigned < rows {
        row_count[0] += rows - assigned;
    }
    // Distinct users per group: unique COUNT(DISTINCT UserID) per group key.
    // Group g has (n_groups - g) distinct users.
    let distinct_users: Vec<usize> = (0..n_groups).map(|g| n_groups - g).collect();

    // Per-user row multiplicity must also be unique for q19-style
    // GROUP BY (UserID, minute, phrase) ORDER BY COUNT(*) — assign each
    // (group, local_user) a unique global weight so no COUNT(*) ties in top-K.
    // Weight for (g, u) = (n_groups - g) * 100 + (distinct_users[g] - u) ensures
    // uniqueness; we then emit min(weight, remaining_in_group) rows carefully.
    // Simpler: one primary user per group gets ALL of that group's rows (so
    // COUNT(*) by UserID equals row_count[g] — unique), and additional distinct
    // users appear once each for COUNT(DISTINCT) without disturbing the primary
    // user's dominant count.
    let mut group_of_row = Vec::with_capacity(rows);
    let mut user_in_group = Vec::with_capacity(rows); // 0..distinct_users[g]
    for (g, &count) in row_count.iter().enumerate() {
        let du = distinct_users[g].max(1);
        // Reserve (du - 1) singleton rows for secondary users; primary user 0
        // gets the rest (strictly more rows than any other user in any group
        // with smaller g because row_count is strictly decreasing and
        // secondary users only get 1 row).
        let secondary = du.saturating_sub(1).min(count.saturating_sub(1));
        let primary_rows = count - secondary;
        for _ in 0..primary_rows {
            if group_of_row.len() >= rows {
                break;
            }
            group_of_row.push(g);
            user_in_group.push(0); // primary user
        }
        for u in 1..=secondary {
            if group_of_row.len() >= rows {
                break;
            }
            group_of_row.push(g);
            user_in_group.push(u);
        }
    }
    group_of_row.truncate(rows);
    user_in_group.truncate(rows);
    while group_of_row.len() < rows {
        group_of_row.push(0);
        user_in_group.push(0);
    }

    let mut watch = Vec::with_capacity(rows);
    let mut user = Vec::with_capacity(rows);
    let mut counter = Vec::with_capacity(rows);
    let mut adv = Vec::with_capacity(rows);
    let mut region = Vec::with_capacity(rows);
    let mut res_w = Vec::with_capacity(rows);
    let mut event_date = Vec::with_capacity(rows);
    let mut event_time = Vec::with_capacity(rows);
    let mut is_refresh = Vec::with_capacity(rows);
    let mut dont_count = Vec::with_capacity(rows);
    let mut phrase = Vec::with_capacity(rows);
    let mut url = Vec::with_capacity(rows);
    let mut title = Vec::with_capacity(rows);
    let mut referer = Vec::with_capacity(rows);
    let mut traffic = Vec::with_capacity(rows);
    let mut search_eng = Vec::with_capacity(rows);
    let mut is_link = Vec::with_capacity(rows);
    let mut is_dl = Vec::with_capacity(rows);
    let mut client_ip = Vec::with_capacity(rows);
    let mut mobile = Vec::with_capacity(rows);
    let mut mobile_model = Vec::with_capacity(rows);
    let mut url_hash = Vec::with_capacity(rows);
    let mut ref_hash = Vec::with_capacity(rows);
    let mut win_w = Vec::with_capacity(rows);
    let mut win_h = Vec::with_capacity(rows);

    // EventDate as days since epoch around mid-2013 for ClickBench-like filters.
    let base_day = 15_896i64; // ~2013-07-01
    // Fixed EventTime base so extract(minute) is stable per (user, phrase) group
    // for q19-style rankings (COUNT(*) over UserID, minute, SearchPhrase).
    let base_event_time = 1_373_000_000i64;
    for (i, (&g, &u_local)) in group_of_row.iter().zip(user_in_group.iter()).enumerate() {
        let i64 = i as i64;
        let g64 = g as i64;
        // WatchID shared per group → unique COUNT(*) by WatchID.
        watch.push(g64);
        // UserID unique per (group, local user index) → unique COUNT(DISTINCT UserID)
        // per RegionID / SearchPhrase (which equal group).
        user.push(100_000 + g64 * 1_000 + u_local as i64);
        counter.push(if g == 0 { 62 } else { 1 + (g64 % 5) });
        adv.push(g64 % 3);
        region.push(g64);
        res_w.push(800 + (g % 400) as u32);
        event_date.push(base_day + (g64 % 30));
        // Minute = g % 60 so (UserID, minute, phrase) groups get power-law counts
        // when UserID is also group-scoped: use one EventTime per group for the
        // primary ranking path, then light variation that stays in the same minute.
        let minute = (g % 60) as i64;
        event_time.push(base_event_time + minute * 60 + (i64 % 50));
        is_refresh.push(if g == 0 { 0 } else { g64 % 20 });
        dont_count.push(0);
        phrase.push(format!("phrase_{g:02}"));
        url.push(format!("https://example.com/page_{g:02}"));
        title.push(format!("title_{g:02}"));
        referer.push(format!("https://ref.example/r_{g:02}"));
        traffic.push(g64 % 10);
        search_eng.push(g64 % 5);
        is_link.push(g64 % 2);
        is_dl.push(0);
        client_ip.push(1000 + g64);
        mobile.push(g64 % 3);
        mobile_model.push(if g % 3 == 0 {
            "Android".into()
        } else {
            String::new()
        });
        url_hash.push(g64.wrapping_mul(31));
        ref_hash.push(g64.wrapping_mul(17));
        win_w.push(1024);
        win_h.push(768);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(watch)),
            Arc::new(Int64Array::from(user)),
            Arc::new(Int64Array::from(counter)),
            Arc::new(Int64Array::from(adv)),
            Arc::new(Int64Array::from(region)),
            Arc::new(UInt32Array::from(res_w)),
            Arc::new(Int64Array::from(event_date)),
            Arc::new(Int64Array::from(event_time)),
            Arc::new(Int64Array::from(is_refresh)),
            Arc::new(Int64Array::from(dont_count)),
            Arc::new(StringArray::from(phrase)),
            Arc::new(StringArray::from(url)),
            Arc::new(StringArray::from(title)),
            Arc::new(StringArray::from(referer)),
            Arc::new(Int64Array::from(traffic)),
            Arc::new(Int64Array::from(search_eng)),
            Arc::new(Int64Array::from(is_link)),
            Arc::new(Int64Array::from(is_dl)),
            Arc::new(Int64Array::from(client_ip)),
            Arc::new(Int64Array::from(mobile)),
            Arc::new(StringArray::from(mobile_model)),
            Arc::new(Int64Array::from(url_hash)),
            Arc::new(Int64Array::from(ref_hash)),
            Arc::new(UInt32Array::from(win_w)),
            Arc::new(UInt32Array::from(win_h)),
        ],
    )
    .expect("hits batch")
}

/// Rows in the reduced `hits` fixture every lane builds.
pub const REDUCED_HITS_ROWS: usize = 50_000;

/// A directory holding `hits.parquet` — the dump `CLICKBENCH_HITS_PARQUET`
/// names, or the reduced fixture — for a lane to load both sides from.
#[must_use]
pub fn hits_fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("hits fixture dir");
    let target = dir.path().join("hits.parquet");
    match std::env::var_os("CLICKBENCH_HITS_PARQUET") {
        Some(dump) => {
            let dump = std::path::PathBuf::from(dump);
            assert!(
                dump.exists(),
                "CLICKBENCH_HITS_PARQUET set but file missing: {}",
                dump.display()
            );
            std::os::unix::fs::symlink(&dump, &target)
                .unwrap_or_else(|e| panic!("link {}: {e}", dump.display()));
        }
        None => super::write_parquet(&make_reduced_hits(REDUCED_HITS_ROWS), &target),
    }
    dir
}
