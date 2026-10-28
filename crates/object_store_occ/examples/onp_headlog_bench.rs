// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Head-log request and latency experiment. Run with no arguments for usage.
//! `ONP_SMOKE=1` labels tiny validation runs; it never silently changes an arm.

#![expect(clippy::expect_used, reason = "an experiment binary")]
#![expect(
    clippy::cast_precision_loss,
    reason = "approximate rates and milliseconds for a measurement report"
)]

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use object_store_occ::LocalConditionalPut;
use object_store_occ::headlog::{HeadLog, HeadLogConfig, Outcome, Poll, Reader, Writer};
use object_store_occ::sim::latency::Delay;
use object_store_occ::sim::{LatencyProfile, LatencyStore};
use parking_lot::Mutex;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::fmt::{self, Write as _};
use std::sync::Arc;
use std::time::{Duration, Instant};

const KIB: usize = 1024;

fn payload(size: usize, tag: u64) -> Bytes {
    // Distinct bytes per write, so no store can deduplicate or repeat an ETag.
    let mut bytes = vec![0_u8; size];
    let mut state = tag.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let word = state.to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
    Bytes::from(bytes)
}

fn build_store(spec: &str, run: &str) -> (String, Arc<dyn ObjectStore>) {
    let mut parts = spec.split(':');
    let kind = parts.next().expect("store kind");
    match kind {
        "rustfs" => {
            let endpoint = std::env::var("ONP_S3_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:19000".into());
            let bucket = std::env::var("ONP_S3_BUCKET").unwrap_or_else(|_| "onp".into());
            let s3 = AmazonS3Builder::new()
                .with_endpoint(endpoint)
                .with_allow_http(true)
                .with_bucket_name(bucket)
                .with_region("us-east-1")
                .with_access_key_id(
                    std::env::var("ONP_S3_KEY").unwrap_or_else(|_| "onpadmin".into()),
                )
                .with_secret_access_key(
                    std::env::var("ONP_S3_SECRET").unwrap_or_else(|_| "onpadmin123".into()),
                )
                .build()
                .expect("build S3 store");
            let prefixed = PrefixStore::new(s3, format!("headlog-bench/{run}"));
            ("rustfs".into(), Arc::new(prefixed))
        }
        "local-durable" | "local" => {
            let dir = std::path::PathBuf::from(parts.next().expect("local:<dir>")).join(run);
            std::fs::create_dir_all(&dir).expect("create local dir");
            let store = if kind == "local-durable" {
                LocalConditionalPut::new_durable(&dir)
            } else {
                LocalConditionalPut::new(&dir)
            }
            .expect("local store");
            (kind.into(), Arc::new(store))
        }
        "latency" => {
            let median: u64 = parts.next().expect("median ms").parse().expect("median");
            let p99: u64 = parts.next().expect("p99 ms").parse().expect("p99");
            let profile = LatencyProfile::uniform(Delay::new(
                Duration::from_millis(median),
                Duration::from_millis(p99),
            ));
            let store = LatencyStore::new(Arc::new(InMemory::new()), profile, 20_261_027);
            (
                format!("latency-{median}ms-p99-{p99}ms (simulated latency)"),
                Arc::new(store),
            )
        }
        other => panic!("unknown store {other}"),
    }
}

// Request attempts include duplicate immutable creates and failed conditional GETs.
// Readers consume every successful GET body, so its returned range is downloaded bytes.
#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    creates: u64,
    cas: u64,
    gets: u64,
    wal: u64,
    wal_created: u64,
    unchanged: u64,
    bytes: u64,
    cas_time: Duration,
}
const CHECKPOINT_BATCHES: usize = 64;

impl Counts {
    fn plus(self, other: Self) -> Self {
        Self {
            creates: self.creates + other.creates,
            cas: self.cas + other.cas,
            gets: self.gets + other.gets,
            wal: self.wal + other.wal,
            wal_created: self.wal_created + other.wal_created,
            unchanged: self.unchanged + other.unchanged,
            bytes: self.bytes + other.bytes,
            cas_time: self.cas_time + other.cas_time,
        }
    }
    fn since(self, before: Self) -> Self {
        Self {
            creates: self.creates - before.creates,
            cas: self.cas - before.cas,
            gets: self.gets - before.gets,
            wal: self.wal - before.wal,
            wal_created: self.wal_created - before.wal_created,
            unchanged: self.unchanged - before.unchanged,
            bytes: self.bytes - before.bytes,
            cas_time: self
                .cas_time
                .checked_sub(before.cas_time)
                .expect("monotonic counter"),
        }
    }
}
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    counts: Mutex<Counts>,
}
impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            counts: Mutex::new(Counts::default()),
        })
    }
    fn snapshot(&self) -> Counts {
        *self.counts.lock()
    }
}
impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}
#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let wal_create = matches!(opts.mode, PutMode::Create) && path.as_ref().contains("/wal/");
        let cas = matches!(opts.mode, PutMode::Update(_)) && path.as_ref().ends_with("/head");
        {
            let mut c = self.counts.lock();
            match opts.mode {
                PutMode::Create => {
                    c.creates += 1;
                    if path.as_ref().contains("/wal/") {
                        c.wal += 1;
                    }
                }
                PutMode::Update(_) => c.cas += 1,
                PutMode::Overwrite => {}
            }
        }
        let start = Instant::now();
        let result = self.inner.put_opts(path, payload, opts).await;
        if cas {
            self.counts.lock().cas_time += start.elapsed();
        }
        if wal_create && result.is_ok() {
            self.counts.lock().wal_created += 1;
        }
        result
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.counts.lock().gets += 1;
        let result = self.inner.get_opts(path, opts).await;
        let mut c = self.counts.lock();
        match &result {
            Ok(r) => c.bytes += r.range.end - r.range.start,
            Err(object_store::Error::NotModified { .. }) => c.unchanged += 1,
            _ => {}
        }
        result
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

fn percentile(values: &[Duration], permille: usize) -> String {
    let complement = 1000 - permille;
    if values.len() < 10_000 / complement {
        return "n/a".into();
    }
    let beyond = values.len() / 1000 * complement + values.len() % 1000 * complement / 1000;
    let rank = values.len() - beyond;
    format!("{:.3}", values[rank - 1].as_secs_f64() * 1000.0)
}
fn summary(name: &str, values: &mut [Duration]) -> String {
    values.sort();
    format!(
        "{name}: n={} P50={} ms P99={} ms P99.9={} ms max={} ms\n",
        values.len(),
        percentile(values, 500),
        percentile(values, 990),
        percentile(values, 999),
        values.last().map_or_else(
            || "n/a".into(),
            |d| format!("{:.3}", d.as_secs_f64() * 1000.0)
        )
    )
}
struct Output {
    dir: std::path::PathBuf,
    report: String,
    raw: String,
}
impl Output {
    fn flush(&self) {
        std::fs::write(self.dir.join("raw.csv"), &self.raw).expect("raw CSV");
        std::fs::write(self.dir.join("report.md"), &self.report).expect("report");
    }
}
async fn writer(store: Arc<dyn ObjectStore>, prefix: &Path, config: HeadLogConfig) -> Writer {
    HeadLog::create(Arc::clone(&store), prefix.clone(), config)
        .await
        .expect("create log");
    Writer::take_over(store, prefix.clone(), config, 1)
        .await
        .expect("take over")
}
async fn accepted(writer: &mut Writer, payloads: Vec<Bytes>) {
    // Volatile receipt sink isolates head-log/store cost. No recovery or caller
    // receipt durability is measured by this experiment.
    let result = writer
        .commit_group(payloads, |_| Ok(()))
        .await
        .expect("commit group");
    assert!(matches!(result, Outcome::Accepted { .. }), "{result:?}");
    assert!(
        writer.sealing_error().is_none(),
        "{:?}",
        writer.sealing_error()
    );
}
fn config_for(size: usize) -> HeadLogConfig {
    let mut config = HeadLogConfig::default();
    // Include group and record protobuf envelopes, including a full u64 generation.
    if size.checked_add(64).expect("record size") > config.max_record_bytes {
        config.max_record_bytes = size.checked_add(64).expect("record budget");
        config.batch_budget_bytes = config.max_record_bytes;
        config.head_budget_bytes = config
            .max_record_bytes
            .checked_mul(2)
            .and_then(|v| v.checked_add(64 * KIB))
            .expect("head budget");
    }
    config
}
fn pair(arm: &str) -> (usize, usize) {
    let (a, b) = arm.split_once('x').expect("arm AxB");
    (a.parse().expect("arm A"), b.parse().expect("arm B"))
}
fn num(args: &[String], i: usize) -> usize {
    args[i].parse().expect("integer argument")
}
fn seconds(args: &[String]) -> Duration {
    let value: f64 = args[3].parse().expect("seconds");
    assert!(value.is_finite() && value > 0.0, "positive finite seconds");
    Duration::from_secs_f64(value)
}

async fn commit(args: &[String], store: Arc<CountingStore>, out: &mut Output) {
    let samples = num(args, 3);
    assert!(samples > 0 && args.len() > 4);
    for arm in &args[4..] {
        let (kib, group) = pair(arm);
        assert!(kib > 0 && group > 0);
        let size = kib.checked_mul(KIB).expect("payload size");
        let config = config_for(
            size.checked_add(10)
                .and_then(|v| v.checked_mul(group))
                .expect("group size"),
        );
        writeln!(
            out.report,
            "arm {arm}: config={config:?}; budgets raised={}",
            config != HeadLogConfig::default()
        )
        .expect("report");
        let prefix = Path::from(format!("commit/{arm}"));
        let mut writer = writer(Arc::clone(&store) as Arc<dyn ObjectStore>, &prefix, config).await;
        let mut times = Vec::new();
        let mut cas_times = Vec::new();
        let mut checkpoints = 0_u64;
        let mut checkpoint_counts = Counts::default();
        let before = store.snapshot();
        let start = Instant::now();
        for i in 0..samples {
            // The batch index is bounded until a checkpoint trims it. Checkpoint
            // outside the timed commit, at the newest batch end, every
            // CHECKPOINT_BATCHES listed batches; the next commit publishes it.
            if let Some(&end) = writer.state().head.batches.last()
                && writer.state().head.batches.len() >= CHECKPOINT_BATCHES
            {
                let counts = store.snapshot();
                writer
                    .publish_checkpoint(end, Bytes::from_static(b"bench"))
                    .await
                    .expect("checkpoint");
                checkpoint_counts = checkpoint_counts.plus(store.snapshot().since(counts));
                checkpoints += 1;
            }
            let payloads = (0..group)
                .map(|j| payload(size, (i * group + j + 1) as u64))
                .collect();
            let counts = store.snapshot();
            let t = Instant::now();
            accepted(&mut writer, payloads).await;
            let elapsed = t.elapsed();
            let c = store.snapshot().since(counts);
            times.push(elapsed);
            cas_times.push(c.cas_time);
            writeln!(
                out.raw,
                "{arm},commit,{i},{},{},{},{},{},{},{},{}",
                elapsed.as_nanos(),
                c.cas_time.as_nanos(),
                c.creates,
                c.cas,
                c.gets,
                c.wal,
                c.wal_created,
                c.bytes
            )
            .expect("raw");
        }
        let elapsed = start.elapsed().as_secs_f64();
        let c = store.snapshot().since(before).since(checkpoint_counts);
        out.report.push_str(&summary("commit_group", &mut times));
        out.report.push_str(&summary(
            "head CAS (sum of attempts per commit)",
            &mut cas_times,
        ));
        writeln!(out.report, "logical payloads/s={:.3}; requests/commit: creates={:.3} CAS={:.3} GETs={:.3}; wal create attempts={} successful creates={} (per-commit in CSV)\n",
            (samples*group) as f64/elapsed, c.creates as f64/samples as f64, c.cas as f64/samples as f64, c.gets as f64/samples as f64, c.wal, c.wal_created).expect("report");
        writeln!(out.report, "checkpoints every {CHECKPOINT_BATCHES} listed batches, untimed and excluded above: {checkpoints}; their creates={} GETs={} downloaded bytes={}\n",
            checkpoint_counts.creates, checkpoint_counts.gets, checkpoint_counts.bytes).expect("report");
        out.flush();
    }
}

async fn offered(args: &[String], store: Arc<CountingStore>, out: &mut Output) {
    let duration = seconds(args);
    let rate = num(args, 4);
    let size = num(args, 5).checked_mul(KIB).expect("size");
    assert!(rate > 0 && size > 0);
    let interval = Duration::from_secs_f64(1.0 / rate as f64);
    assert!(!interval.is_zero());
    let limit = usize::try_from(duration.as_nanos().div_ceil(interval.as_nanos()))
        .expect("offered sample count");
    let config = config_for(
        size.checked_add(10)
            .and_then(|v| v.checked_mul(limit))
            .expect("maximum queued group"),
    );
    writeln!(out.report, "commit-offered: seconds={} offered/s={rate} payload KiB={} config={config:?}; capacity reserves all {limit} offered payloads in one group", duration.as_secs_f64(), size/KIB).expect("report");
    let mut writer = writer(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        &Path::from("offered"),
        config,
    )
    .await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let started = Instant::now();
    let producer = tokio::spawn(async move {
        let first = tokio::time::Instant::now();
        for i in 0..limit {
            tokio::time::sleep_until(first + interval.mul_f64(i as f64)).await;
            let bytes = payload(size, i as u64 + 1);
            tx.send((Instant::now(), bytes)).expect("enqueue");
        }
    });
    let mut acks = Vec::new();
    let mut commits = Vec::new();
    let mut cas = Vec::new();
    while let Some(first) = rx.recv().await {
        let mut queued = vec![first];
        while let Ok(next) = rx.try_recv() {
            queued.push(next);
        }
        let n = queued.len();
        let enqueue: Vec<_> = queued.iter().map(|(t, _)| *t).collect();
        let payloads = queued.into_iter().map(|(_, b)| b).collect();
        let before = store.snapshot();
        let t = Instant::now();
        accepted(&mut writer, payloads).await;
        let done = Instant::now();
        let elapsed = done - t;
        let c = store.snapshot().since(before);
        for at in enqueue {
            let ack = done - at;
            writeln!(
                out.raw,
                "offered,ack,{},{},,,,,,,",
                acks.len(),
                ack.as_nanos()
            )
            .expect("raw");
            acks.push(ack);
        }
        writeln!(
            out.raw,
            "offered,commit-{n}-payloads,{},{},{},{},{},{},{},{},{}",
            commits.len(),
            elapsed.as_nanos(),
            c.cas_time.as_nanos(),
            c.creates,
            c.cas,
            c.gets,
            c.wal,
            c.wal_created,
            c.bytes
        )
        .expect("raw");
        commits.push(elapsed);
        cas.push(c.cas_time);
    }
    producer.await.expect("producer");
    let elapsed = started.elapsed().max(duration).as_secs_f64();
    writeln!(
        out.report,
        "groups/s={:.3}; mean payloads/group={:.3}; logical payloads/s={:.3}",
        commits.len() as f64 / elapsed,
        acks.len() as f64 / commits.len() as f64,
        acks.len() as f64 / elapsed
    )
    .expect("report");
    out.report
        .push_str(&summary("ack (enqueue to Accepted)", &mut acks));
    out.report.push_str(&summary("commit_group", &mut commits));
    out.report.push_str(&summary("head CAS", &mut cas));
    out.flush();
}

async fn open(args: &[String], store: Arc<CountingStore>, out: &mut Output) {
    let samples = num(args, 3);
    assert!(samples > 0 && args.len() > 4);
    let config = HeadLogConfig {
        batch_max_records: 8,
        ..HeadLogConfig::default()
    };
    let record_size = 8160;
    let concurrency = std::env::var("ONP_READ_CONCURRENCY")
        .map_or(object_store_occ::headlog::DEFAULT_READ_CONCURRENCY, |v| {
            v.parse().expect("ONP_READ_CONCURRENCY")
        });
    writeln!(
        out.report,
        "read concurrency (ONP_READ_CONCURRENCY): {concurrency}"
    )
    .expect("report");
    for arm in &args[4..] {
        let (tail, ckpt) = pair(arm);
        let prefix = Path::from(format!("open/{arm}"));
        let mut writer = writer(Arc::clone(&store) as Arc<dyn ObjectStore>, &prefix, config).await;
        let mut tag = 0;
        let root = payload(ckpt.checked_mul(1024 * KIB).expect("checkpoint size"), 99);
        if ckpt > 0 {
            for _ in 0..=config.batch_max_records {
                tag += 1;
                accepted(&mut writer, vec![payload(record_size, tag)]).await;
            }
            let boundary = writer.state().head.batches[0];
            writer
                .publish_checkpoint(boundary, root.clone())
                .await
                .expect("publish checkpoint");
            tag += 1;
            accepted(&mut writer, vec![payload(record_size, tag)]).await;
        }
        while writer.state().head.batches.len() < tail {
            tag += 1;
            accepted(&mut writer, vec![payload(record_size, tag)]).await;
        }
        assert_eq!(writer.state().head.batches.len(), tail);
        let expected = writer.state().head.commit - writer.state().head.floor;
        writeln!(out.report, "arm {arm}: config={config:?}; payload bytes={record_size}; batch ~64 KiB (K=8); listed batches={tail}; floor={}; window records={}", writer.state().head.floor, writer.state().head.window.len()).expect("report");
        let mut times = Vec::new();
        let before = store.snapshot();
        for i in 0..samples {
            let c = store.snapshot();
            let start = Instant::now();
            let (_, opened) = Reader::open_with_concurrency(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                prefix.clone(),
                concurrency,
            )
            .await
            .expect("open");
            let elapsed = start.elapsed();
            let c = store.snapshot().since(c);
            assert_eq!(opened.records.len() as u64, expected);
            assert_eq!(opened.checkpoint_root.as_ref(), (ckpt > 0).then_some(&root));
            writeln!(
                out.raw,
                "{arm},open,{i},{},,{},{},{},{},{},{}",
                elapsed.as_nanos(),
                c.creates,
                c.cas,
                c.gets,
                c.wal,
                c.wal_created,
                c.bytes
            )
            .expect("raw");
            times.push(elapsed);
        }
        let c = store.snapshot().since(before);
        out.report.push_str(&summary("Reader::open", &mut times));
        writeln!(
            out.report,
            "GETs/open={:.3}; bytes/open={:.3}\n",
            c.gets as f64 / samples as f64,
            c.bytes as f64 / samples as f64
        )
        .expect("report");
        out.flush();
    }
}

async fn poll(args: &[String], store: Arc<CountingStore>, out: &mut Output) {
    let duration = seconds(args);
    let datasets = num(args, 4);
    let staleness = num(args, 5);
    let change = num(args, 6);
    assert!(datasets > 0 && staleness > 0 && change > 0);
    let mut writers = Vec::new();
    let mut readers = Vec::new();
    for i in 0..datasets {
        let prefix = Path::from(format!("poll/{i}"));
        let mut writer = writer(Arc::clone(&store.inner), &prefix, HeadLogConfig::default()).await;
        for tag in 1..=3 {
            accepted(&mut writer, vec![payload(128, tag)]).await;
        }
        readers.push(
            Reader::open(Arc::clone(&store) as Arc<dyn ObjectStore>, prefix)
                .await
                .expect("initial open")
                .0,
        );
        writers.push(writer);
    }
    let before = store.snapshot();
    let start = Instant::now();
    let end = tokio::time::Instant::now() + duration;
    let mut tasks = Vec::new();
    let mut rng = StdRng::seed_from_u64(20_261_028);
    for (i, mut reader) in readers.into_iter().enumerate() {
        let jitter = Duration::from_millis(rng.random_range(0..staleness) as u64);
        tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + jitter,
                Duration::from_millis(staleness as u64),
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut values = Vec::new();
            loop {
                let tick = tokio::select! { tick = interval.tick() => tick, () = tokio::time::sleep_until(end) => break };
                if tick >= end || tokio::time::Instant::now() >= end {
                    break;
                }
                let t = Instant::now();
                let result = reader.poll().await.expect("poll");
                let kind = match result {
                    Poll::Unchanged => "Unchanged",
                    Poll::Records(_) => "Records",
                    Poll::Reopen => panic!("unexpected reopen without checkpoints"),
                };
                values.push((i, kind, t.elapsed()));
            }
            values
        }));
    }
    let writer_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_millis(change as u64),
            Duration::from_millis(change as u64),
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut tag = 4;
        loop {
            let tick = tokio::select! { tick = interval.tick() => tick, () = tokio::time::sleep_until(end) => break };
            if tick >= end || tokio::time::Instant::now() >= end {
                break;
            }
            let i = rng.random_range(0..writers.len());
            accepted(&mut writers[i], vec![payload(128, tag)]).await;
            tag += 1;
        }
    });
    let mut unchanged = Vec::new();
    let mut records = Vec::new();
    for task in tasks {
        for (i, kind, time) in task.await.expect("reader task") {
            writeln!(
                out.raw,
                "dataset-{i},{kind},{},{},,,,,,,",
                unchanged.len() + records.len(),
                time.as_nanos()
            )
            .expect("raw");
            if kind == "Unchanged" {
                unchanged.push(time);
            } else {
                records.push(time);
            }
        }
    }
    writer_task.await.expect("writer task");
    let c = store.snapshot().since(before);
    let total = unchanged.len() + records.len();
    // Requests finish in flight after the scheduling deadline; include their drain time.
    let elapsed = start.elapsed().as_secs_f64();
    writeln!(out.report, "poll: datasets={datasets} seconds={} staleness_ms={staleness} change_every_ms={change}; completed polls={total}; total poll requests={} (GETs); polls/s/node={:.3}; 304 fraction={:.6}; downloaded bytes/s={:.3}; elapsed including drain={elapsed:.6}s", duration.as_secs_f64(), c.gets, total as f64/elapsed, if total == 0 { 0.0 } else { c.unchanged as f64/total as f64 }, c.bytes as f64/elapsed).expect("report");
    out.report.push_str(&summary("Unchanged", &mut unchanged));
    out.report.push_str(&summary("Records", &mut records));
    out.flush();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let valid = match args.first().map(String::as_str) {
        Some("commit" | "open") => args.len() >= 5,
        Some("commit-offered") => args.len() == 6,
        Some("poll") => args.len() == 7,
        _ => false,
    };
    assert!(
        valid,
        "usage:\ncommit <out> <store> <samples> <payload_kib>x<group>...\ncommit-offered <out> <store> <seconds> <offered_per_sec> <payload_kib>\nopen <out> <store> <samples> <tail_batches>x<ckpt_mib>...\npoll <out> <store> <seconds> <datasets> <staleness_ms> <change_every_ms>\nstores: rustfs | local-durable:<dir> | local:<dir> | latency:<median_ms>:<p99_ms>"
    );
    let run = uuid::Uuid::now_v7().simple().to_string();
    let (label, store) = build_store(&args[2], &run);
    let mut out = Output { dir: args[1].clone().into(), report: format!("# Head-log benchmark\n\nstore: {label}\ncommand: {}\nONP_SMOKE={}\nReceipt persistence: volatile no-op; caller receipt fsync excluded. CAS timing: store put_opts call only, summed across retries. wal counts are create attempts (including duplicates).\n\n", args.join(" "), std::env::var("ONP_SMOKE").unwrap_or_else(|_| "0".into())), raw: "arm,operation,sample,nanoseconds,cas_nanoseconds,creates,cas,gets,wal_create_attempts,wal_created,downloaded_bytes\n".into() };
    std::fs::create_dir_all(&out.dir).expect("output directory");
    let store = CountingStore::new(store);
    match args[0].as_str() {
        "commit" => commit(&args, store, &mut out).await,
        "commit-offered" => offered(&args, store, &mut out).await,
        "open" => open(&args, store, &mut out).await,
        "poll" => poll(&args, store, &mut out).await,
        _ => unreachable!(),
    }
    println!("{}", out.report);
}
