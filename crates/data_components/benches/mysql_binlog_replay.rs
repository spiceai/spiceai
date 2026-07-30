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

//! Replay a real `MySQL` binlog file through the same `mysql_common` event
//! parser the shared replication pump uses, in two phases:
//!
//! 1. **Characteristics report** (full file): event-type histogram,
//!    rows-per-event and per-transaction distributions, per-table volume —
//!    the ground truth for sizing pump work and for validating any synthetic
//!    event generator against real traffic (real rows events pack many rows
//!    per event; transactions interleave several tables).
//! 2. **Criterion throughput arms** (capped prefix): parse+checksum and
//!    `read_data()` classification (the shared pump's per-event floor), then
//!    the production row decode both ways — the `buffer_rows_event` walk and
//!    the `TableMapRowDecoder` fast path — so the prepared decoder's win and any
//!    regression to it are measured on the exact code the consumer runs.
//!
//! Before the arms run, every rows event in the slice is decoded through BOTH
//! production paths and the resulting change buffers compared; any divergence
//! panics (skip with `MYSQL_BINLOG_REPLAY_SKIP_VERIFY=1`).
//!
//! The fixture is intentionally not committed (a representative capture is
//! often large). Point `MYSQL_BINLOG_REPLAY_FILE` at a `MySQL` binlog file;
//! without it the bench prints capture instructions and exits successfully, so
//! it is safe under `cargo bench` in CI.

#![expect(
    clippy::expect_used,
    reason = "bench-only: the fixture is trusted input; a malformed event should abort loudly"
)]

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, Throughput};
use data_components::mysql_replication::binlog::{
    TableMapRowDecoder, buffer_rows_event, buffer_rows_event_fast,
};
use data_components::mysql_replication::metrics::MetricsCollector;
use data_components::mysql_replication::rows::TransactionBuffer;
use data_components::mysql_replication::setup::{SourceColumn, TableLayout};
use mysql_async::binlog::BinlogVersion;
use mysql_async::binlog::events::{
    Event, EventData, FormatDescriptionEvent, RowsEventData, TableMapEvent,
};

const FILE_HEADER_LEN: usize = 4; // [0xfe, b'b', b'i', b'n']
const EVENT_HEADER_LEN: usize = 19;
/// Offset of the little-endian `event_size` field inside a v4 event header.
const EVENT_SIZE_OFFSET: usize = 9;
/// Criterion arms replay at most this much of the file so an iteration stays
/// in the hundreds of milliseconds; the characteristics report always scans
/// the whole file.
const DEFAULT_BENCH_BYTES: usize = 256 * 1024 * 1024;

fn main() {
    let Some(path) = std::env::var_os("MYSQL_BINLOG_REPLAY_FILE") else {
        print_instructions("MYSQL_BINLOG_REPLAY_FILE is not set");
        return;
    };
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) => {
            print_instructions(&format!("failed to read {}: {e}", path.to_string_lossy()));
            return;
        }
    };
    if bytes.len() < FILE_HEADER_LEN || bytes[..FILE_HEADER_LEN] != [0xfe, b'b', b'i', b'n'] {
        print_instructions(&format!(
            "{} does not start with the binlog magic [0xfe 'b' 'i' 'n']",
            path.to_string_lossy()
        ));
        return;
    }

    println!(
        "== mysql binlog characteristics: {} ({:.1} MiB) ==",
        path.to_string_lossy(),
        to_mib(bytes.len()),
    );
    let stats = collect_stats(&bytes);
    stats.print();

    let cap = std::env::var("MYSQL_BINLOG_REPLAY_BENCH_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BENCH_BYTES)
        .min(bytes.len());
    let slice = &bytes[..cap];

    if std::env::var_os("MYSQL_BINLOG_REPLAY_SKIP_VERIFY").is_none() {
        verify_fast_decoder(slice);
    }

    println!(
        "\n== throughput arms over the first {:.1} MiB ==",
        to_mib(cap)
    );

    let mut c = Criterion::default()
        .configure_from_args()
        .sample_size(10)
        .measurement_time(Duration::from_secs(15));
    bench_stages(&mut c, slice);
    c.final_summary();
}

fn print_instructions(reason: &str) {
    println!(
        "mysql_binlog_replay: skipped ({reason}).\n\
         \n\
         This bench replays a real MySQL binlog. To produce one:\n\
         \n\
         1. Start MySQL with row-based binlogging enabled and drive representative writes.\n\
         2. Find a finished file:   mysql -h... -e 'SHOW BINARY LOGS;'\n\
         3. Copy it out:            docker cp <mysql-container>:/var/lib/mysql/binlog.000002 /tmp/\n\
         4. Run the bench:\n\
            MYSQL_BINLOG_REPLAY_FILE=/tmp/binlog.000002 \\\n\
              cargo bench -p data_components --bench mysql_binlog_replay --features mysql\n\
         \n\
         Optional: MYSQL_BINLOG_REPLAY_BENCH_BYTES caps how much of the file the criterion\n\
         arms replay (default 256 MiB); the characteristics report always scans it all.\n\
         Optional: MYSQL_BINLOG_REPLAY_SKIP_VERIFY=1 skips the decoder equivalence gate."
    );
}

/// Iterator over the events of an in-memory binlog file, maintaining the
/// format-description event exactly as `mysql_common::binlog::EventStreamReader`
/// does (checksum handling rides on the FDE footer). Stops cleanly at a
/// truncated tail, so a byte-capped prefix is a valid input.
struct EventCursor<'a> {
    fde: FormatDescriptionEvent<'static>,
    buf: &'a [u8],
    pos: usize,
}

impl<'a> EventCursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self {
            fde: FormatDescriptionEvent::new(BinlogVersion::Version4),
            buf,
            pos: FILE_HEADER_LEN.min(buf.len()),
        }
    }
}

impl Iterator for EventCursor<'_> {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let remaining = &self.buf[self.pos..];
        if remaining.len() < EVENT_HEADER_LEN {
            return None;
        }
        let size_bytes: [u8; 4] = remaining[EVENT_SIZE_OFFSET..EVENT_SIZE_OFFSET + 4]
            .try_into()
            .expect("4-byte event_size slice");
        let event_size = usize::try_from(u32::from_le_bytes(size_bytes)).expect("event size");
        if event_size < EVENT_HEADER_LEN || remaining.len() < event_size {
            return None;
        }
        let event = Event::read(&self.fde, &remaining[..event_size]).expect("parse binlog event");
        self.pos += event_size;
        if let Ok(Some(EventData::FormatDescriptionEvent(fde))) = event.read_data() {
            self.fde = fde.into_owned().with_footer(event.footer());
        }
        Some(event)
    }
}

/// Synthesize the `TableLayout` the production decode expects from a
/// `TableMapEvent`. Names and type strings are placeholders: both decode
/// paths see the same layout and route values through the same
/// `normalize_binlog_value`, which only changes behavior for ENUM/SET
/// columns. Keep using this helper only for captures whose schema does not
/// require layout-derived ENUM/SET normalization.
fn layout_for(tme: &TableMapEvent<'_>) -> TableLayout {
    let n = usize::try_from(tme.columns_count()).expect("columns_count fits usize");
    TableLayout {
        columns: (0..n)
            .map(|i| SourceColumn {
                name: format!("c{i}"),
                column_type: "bench".to_string(),
                enum_variants: None,
                set_variants: None,
                is_primary_key: false,
            })
            .collect(),
    }
}

/// One-time equivalence gate for the prepared decoder: decode every rows event in
/// the slice through BOTH production paths — the `buffer_rows_event` walk and
/// `buffer_rows_event_fast` — and require identical change buffers. Runs
/// before the criterion arms (skip with `MYSQL_BINLOG_REPLAY_SKIP_VERIFY=1`);
/// panics on divergence.
fn verify_fast_decoder(slice: &[u8]) {
    let metrics = MetricsCollector::new();
    let mut routes: HashMap<u64, (TableMapEvent<'static>, TableMapRowDecoder, TableLayout)> =
        HashMap::new();
    let mut events_checked: u64 = 0;
    let mut changes_checked: u64 = 0;
    for event in EventCursor::new(slice) {
        match event.read_data() {
            Ok(Some(EventData::TableMapEvent(tme))) => {
                let owned = tme.into_owned();
                let decoder =
                    TableMapRowDecoder::try_new(&owned).expect("decoder builds for table map");
                let layout = layout_for(&owned);
                routes.insert(owned.table_id(), (owned, decoder, layout));
            }
            Ok(Some(EventData::RowsEvent(rows_data))) => {
                if matches!(rows_data, RowsEventData::PartialUpdateRowsEvent(_)) {
                    continue;
                }
                let Some((tme, decoder, layout)) = routes.get(&rows_data.table_id()) else {
                    continue;
                };
                let mut walk = TransactionBuffer::new();
                buffer_rows_event(&rows_data, tme, layout, &[], &mut walk, &metrics)
                    .expect("walk decodes");
                let mut fast = TransactionBuffer::new();
                buffer_rows_event_fast(&rows_data, decoder, layout, &[], &mut fast, &metrics)
                    .expect("fast decodes");
                assert_eq!(
                    fast.changes, walk.changes,
                    "prepared decoder diverged from the walk at rows event #{events_checked}"
                );
                events_checked += 1;
                changes_checked += fast.changes.len() as u64;
            }
            _ => {}
        }
    }
    println!(
        "fast-decoder verification: {events_checked} rows events, {changes_checked} decoded \
         changes — identical to the mysql_common rows() walk"
    );
}

#[derive(Default)]
struct Distribution(Vec<u64>);

impl Distribution {
    fn push(&mut self, v: u64) {
        self.0.push(v);
    }

    fn summary(&mut self) -> String {
        if self.0.is_empty() {
            return "n=0".to_string();
        }
        self.0.sort_unstable();
        let n = self.0.len();
        let pct = |p: usize| self.0[(n - 1) * p / 100];
        let mean = self.0.iter().sum::<u64>() / n as u64;
        format!(
            "n={n} mean={mean} p50={} p95={} p99={} max={}",
            pct(50),
            pct(95),
            pct(99),
            self.0[n - 1],
        )
    }
}

#[derive(Default)]
struct TableStats {
    rows_events: u64,
    rows: u64,
    bytes: u64,
}

#[derive(Default)]
struct Stats {
    events: u64,
    bytes: u64,
    first_ts: Option<u32>,
    last_ts: u32,
    by_type: HashMap<String, (u64, u64)>, // count, bytes
    table_map_events: u64,
    rows_events: u64,
    inserts: u64,
    updates: u64,
    deletes: u64,
    total_rows: u64,
    row_decode_errors: u64,
    rows_per_event: Distribution,
    rows_event_bytes: Distribution,
    per_table: HashMap<String, TableStats>,
    transactions: u64,
    txn_events: Distribution,
    txn_rows: Distribution,
    txn_tables: Distribution,
    txn_bytes: Distribution,
    begin_queries: u64,
    commit_queries: u64,
    other_queries: u64,
    gtid_events: u64,
    anonymous_gtid_events: u64,
}

#[derive(Default)]
struct OpenTxn {
    events: u64,
    rows: u64,
    tables: std::collections::HashSet<u64>,
    bytes: u64,
}

fn collect_stats(bytes: &[u8]) -> Stats {
    let mut stats = Stats::default();
    let mut tmes: HashMap<u64, TableMapEvent<'static>> = HashMap::new();
    let mut txn: Option<OpenTxn> = None;

    let close_txn = |stats: &mut Stats, txn: &mut Option<OpenTxn>| {
        if let Some(t) = txn.take() {
            stats.transactions += 1;
            stats.txn_events.push(t.events);
            stats.txn_rows.push(t.rows);
            stats.txn_tables.push(t.tables.len() as u64);
            stats.txn_bytes.push(t.bytes);
        }
    };

    for event in EventCursor::new(bytes) {
        let header = event.header();
        let size = u64::from(header.event_size());
        stats.events += 1;
        stats.bytes += size;
        if header.timestamp() != 0 {
            stats.first_ts.get_or_insert(header.timestamp());
            stats.last_ts = header.timestamp();
        }
        let type_name = header.event_type().map_or_else(
            |_| format!("raw({})", header.event_type_raw()),
            |t| format!("{t:?}"),
        );
        let slot = stats.by_type.entry(type_name).or_default();
        slot.0 += 1;
        slot.1 += size;
        if let Some(t) = txn.as_mut() {
            t.events += 1;
            t.bytes += size;
        }

        let Ok(Some(data)) = event.read_data() else {
            continue;
        };
        match data {
            EventData::TableMapEvent(tme) => {
                stats.table_map_events += 1;
                tmes.insert(tme.table_id(), tme.into_owned());
            }
            EventData::RowsEvent(rows_data) => {
                stats.rows_events += 1;
                stats.rows_event_bytes.push(size);
                match &rows_data {
                    RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => {
                        stats.inserts += 1;
                    }
                    RowsEventData::UpdateRowsEvent(_)
                    | RowsEventData::UpdateRowsEventV1(_)
                    | RowsEventData::PartialUpdateRowsEvent(_) => stats.updates += 1,
                    RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => {
                        stats.deletes += 1;
                    }
                }
                let table_id = rows_data.table_id();
                if let Some(tme) = tmes.get(&table_id) {
                    let mut rows_in_event: u64 = 0;
                    for row in rows_data.rows(tme) {
                        if row.is_ok() {
                            rows_in_event += 1;
                        } else {
                            stats.row_decode_errors += 1;
                        }
                    }
                    stats.total_rows += rows_in_event;
                    stats.rows_per_event.push(rows_in_event);
                    let key = format!("{}.{}", tme.database_name(), tme.table_name());
                    let per_table = stats.per_table.entry(key).or_default();
                    per_table.rows_events += 1;
                    per_table.rows += rows_in_event;
                    per_table.bytes += size;
                    if let Some(t) = txn.as_mut() {
                        t.rows += rows_in_event;
                        t.tables.insert(table_id);
                    }
                }
            }
            EventData::GtidEvent(_) => {
                stats.gtid_events += 1;
                close_txn(&mut stats, &mut txn); // a group missing its commit
                txn = Some(OpenTxn::default());
            }
            EventData::AnonymousGtidEvent(_) => {
                stats.anonymous_gtid_events += 1;
                close_txn(&mut stats, &mut txn);
                txn = Some(OpenTxn::default());
            }
            EventData::XidEvent(_) => close_txn(&mut stats, &mut txn),
            EventData::QueryEvent(query) => {
                let q = query.query();
                let q = q.trim();
                if q.eq_ignore_ascii_case("BEGIN") {
                    stats.begin_queries += 1;
                    if txn.is_none() {
                        txn = Some(OpenTxn::default());
                    }
                } else if q.eq_ignore_ascii_case("COMMIT") {
                    stats.commit_queries += 1;
                    close_txn(&mut stats, &mut txn);
                } else {
                    stats.other_queries += 1;
                }
            }
            _ => {}
        }
    }
    close_txn(&mut stats, &mut txn);
    stats
}

impl Stats {
    fn print(mut self) {
        let span_secs = self
            .first_ts
            .map_or(0, |first| u64::from(self.last_ts.saturating_sub(first)));
        println!(
            "events={} bytes={:.1} MiB span={span_secs}s",
            self.events,
            to_mib_u64(self.bytes)
        );
        if let (Some(events_rate), Some(rows_rate), Some(txn_rate)) = (
            self.events.checked_div(span_secs),
            self.total_rows.checked_div(span_secs),
            self.transactions.checked_div(span_secs),
        ) {
            println!(
                "original workload rate: {events_rate} events/s, {rows_rate} rows/s, {txn_rate} txn/s"
            );
        }

        println!("\nevent types (count / bytes):");
        let mut types: Vec<_> = self.by_type.iter().collect();
        types.sort_by_key(|(_, (count, _))| std::cmp::Reverse(*count));
        for (name, (count, bytes)) in types {
            println!("  {name:<28} {count:>10}  {:>9.1} MiB", to_mib_u64(*bytes));
        }

        println!(
            "\nrows events: {} (insert={} update={} delete={}), total rows={}, decode errors={}",
            self.rows_events,
            self.inserts,
            self.updates,
            self.deletes,
            self.total_rows,
            self.row_decode_errors,
        );
        println!(
            "table-map events: {} ({:.2} per rows event)",
            self.table_map_events,
            ratio(self.table_map_events, self.rows_events),
        );
        println!("rows/event:        {}", self.rows_per_event.summary());
        println!("rows-event bytes:  {}", self.rows_event_bytes.summary());

        println!(
            "\ntransactions: {} (gtid={} anonymous={} begin={} commit_query={} other_queries={})",
            self.transactions,
            self.gtid_events,
            self.anonymous_gtid_events,
            self.begin_queries,
            self.commit_queries,
            self.other_queries,
        );
        println!("events/txn:  {}", self.txn_events.summary());
        println!("rows/txn:    {}", self.txn_rows.summary());
        println!("tables/txn:  {}", self.txn_tables.summary());
        println!("bytes/txn:   {}", self.txn_bytes.summary());

        println!("\nper-table volume:");
        let mut tables: Vec<_> = self.per_table.iter().collect();
        tables.sort_by_key(|(_, t)| std::cmp::Reverse(t.rows));
        for (name, t) in tables {
            println!(
                "  {name:<32} rows={:>10} rows_events={:>9} bytes={:>9.1} MiB ({:.2} rows/event)",
                t.rows,
                t.rows_events,
                to_mib_u64(t.bytes),
                ratio(t.rows, t.rows_events),
            );
        }
    }
}

/// The successive per-event stages of the pump's hot path, each arm adding
/// the next stage so `--baseline` diffs isolate a stage's cost.
fn bench_stages(c: &mut Criterion, slice: &[u8]) {
    let mut group = c.benchmark_group("mysql_binlog_replay");
    group.throughput(Throughput::Bytes(slice.len() as u64));

    // Wire parse + checksum only — the floor `stream.next()` decoding pays.
    group.bench_function("parse", |b| {
        b.iter(|| {
            for event in EventCursor::new(slice) {
                black_box(event.header().event_size());
            }
        });
    });

    // + `read_data()` classification, as the pump's match on `EventData`.
    group.bench_function("parse_decode", |b| {
        b.iter(|| {
            for event in EventCursor::new(slice) {
                if let Ok(Some(data)) = event.read_data() {
                    black_box(std::mem::discriminant(&data));
                }
            }
        });
    });

    // The production row decode, walk path (`buffer_rows_event`): what the
    // consumer pays when the prepared decoder cannot be built for a table map.
    let metrics = MetricsCollector::new();
    group.bench_function("decode_walk", |b| {
        b.iter(|| {
            let mut routes: HashMap<u64, (TableMapEvent<'static>, TableLayout)> = HashMap::new();
            for event in EventCursor::new(slice) {
                match event.read_data() {
                    Ok(Some(EventData::TableMapEvent(tme))) => {
                        routes.entry(tme.table_id()).or_insert_with(|| {
                            let owned = tme.into_owned();
                            let layout = layout_for(&owned);
                            (owned, layout)
                        });
                    }
                    Ok(Some(EventData::RowsEvent(rows_data))) => {
                        if matches!(rows_data, RowsEventData::PartialUpdateRowsEvent(_)) {
                            continue;
                        }
                        if let Some((tme, layout)) = routes.get(&rows_data.table_id()) {
                            let mut buffer = TransactionBuffer::new();
                            buffer_rows_event(&rows_data, tme, layout, &[], &mut buffer, &metrics)
                                .expect("walk decodes");
                            black_box(buffer);
                        }
                    }
                    _ => {}
                }
            }
        });
    });

    // Same rows, fast path (`buffer_rows_event_fast`) — the delta vs
    // `decode_walk` is what hoisting the per-row-image metadata rebuild out of
    // the row loop is worth. The decoder is cached per table here; production
    // rebuilds it once per decoded transaction, which a replay fixture prices at
    // ~1µs against a ~30-row transaction decode.
    group.bench_function("decode_fast", |b| {
        b.iter(|| {
            let mut routes: HashMap<u64, (TableMapRowDecoder, TableLayout)> = HashMap::new();
            for event in EventCursor::new(slice) {
                match event.read_data() {
                    Ok(Some(EventData::TableMapEvent(tme))) => {
                        routes.entry(tme.table_id()).or_insert_with(|| {
                            let decoder = TableMapRowDecoder::try_new(&tme)
                                .expect("decoder builds for table map");
                            (decoder, layout_for(&tme))
                        });
                    }
                    Ok(Some(EventData::RowsEvent(rows_data))) => {
                        if matches!(rows_data, RowsEventData::PartialUpdateRowsEvent(_)) {
                            continue;
                        }
                        if let Some((decoder, layout)) = routes.get(&rows_data.table_id()) {
                            let mut buffer = TransactionBuffer::new();
                            buffer_rows_event_fast(
                                &rows_data,
                                decoder,
                                layout,
                                &[],
                                &mut buffer,
                                &metrics,
                            )
                            .expect("fast decodes");
                            black_box(buffer);
                        }
                    }
                    _ => {}
                }
            }
        });
    });

    group.finish();
}

fn to_mib(bytes: usize) -> f64 {
    #[expect(clippy::cast_precision_loss, reason = "display only")]
    let mib = bytes as f64 / (1024.0 * 1024.0);
    mib
}

fn to_mib_u64(bytes: u64) -> f64 {
    #[expect(clippy::cast_precision_loss, reason = "display only")]
    let mib = bytes as f64 / (1024.0 * 1024.0);
    mib
}

fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 {
        return 0.0;
    }
    #[expect(clippy::cast_precision_loss, reason = "display only")]
    let r = a as f64 / b as f64;
    r
}
