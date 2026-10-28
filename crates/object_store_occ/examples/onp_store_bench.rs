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

//! What a store gives the object-native metastore: the latency of the
//! requests a commit, a poll and a cold open make, and whether the store
//! enforces the conditions the protocol depends on.
//!
//! ```sh
//! cargo run --release -p object_store_occ --example onp_store_bench -- \
//!     <out_dir> <store>...
//! ```
//!
//! Stores: `rustfs` (`ONP_S3_ENDPOINT`, `ONP_S3_BUCKET`, `ONP_S3_KEY`,
//! `ONP_S3_SECRET`), `local-durable:<dir>`, `local:<dir>` (no fsync, a
//! reference only), and `latency:<median_ms>:<p99_ms>` (in-memory behind the
//! latency wrapper: simulated latency). Sample counts: `ONP_N_SMALL`,
//! `ONP_N_1M`, `ONP_N_8M`, `ONP_N_64M`.

#![expect(
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "an experiment binary computing percentiles and rates"
)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, UpdateVersion};
use object_store_occ::sim::latency::Delay;
use object_store_occ::sim::{LatencyProfile, LatencyStore};
use object_store_occ::{
    ConditionalWriteError, Expected, LocalConditionalPut, conditional_put,
    probe_conditional_writes,
};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

fn env_count(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

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

fn version_of(result: &object_store::PutResult) -> UpdateVersion {
    UpdateVersion {
        e_tag: result.e_tag.clone(),
        version: result.version.clone(),
    }
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
                .with_access_key_id(std::env::var("ONP_S3_KEY").unwrap_or_else(|_| "onpadmin".into()))
                .with_secret_access_key(
                    std::env::var("ONP_S3_SECRET").unwrap_or_else(|_| "onpadmin123".into()),
                )
                .build()
                .expect("build S3 store");
            let prefixed = PrefixStore::new(s3, format!("phase-a/{run}"));
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
            (format!("latency-{median}ms-p99-{p99}ms (simulated latency)"), Arc::new(store))
        }
        other => panic!("unknown store {other}"),
    }
}

/// One condition check: what was expected, what was seen, and whether they
/// agree.
struct Check {
    name: &'static str,
    expected: String,
    seen: String,
    pass: bool,
}

fn describe<T>(result: &Result<T, ConditionalWriteError>) -> String {
    match result {
        Ok(_) => "accepted".into(),
        Err(ConditionalWriteError::Conflict { .. }) => "refused (Conflict)".into(),
        Err(err) => format!("error: {err}"),
    }
}

async fn condition_checks(store: &Arc<dyn ObjectStore>) -> Vec<Check> {
    let mut checks = Vec::new();
    let probe = probe_conditional_writes(store.as_ref(), &Path::from("probe")).await;
    checks.push(Check {
        name: "probe_conditional_writes",
        expected: "enforced".into(),
        seen: format!("{probe:?}"),
        pass: probe.is_enforced(),
    });

    // A second create of one name is refused.
    let path = Path::from("checks/create-once");
    let first = conditional_put(store.as_ref(), &path, payload(64, 1).into(), &Expected::Absent).await;
    let second =
        conditional_put(store.as_ref(), &path, payload(64, 2).into(), &Expected::Absent).await;
    let kept = store.get(&path).await.expect("get").bytes().await.expect("bytes");
    checks.push(Check {
        name: "second create refused, first bytes kept",
        expected: "accepted, refused (Conflict), first bytes".into(),
        seen: format!(
            "{}, {}, {}",
            describe(&first),
            describe(&second),
            if kept == payload(64, 1) { "first bytes" } else { "OTHER bytes" }
        ),
        pass: first.is_ok()
            && matches!(second, Err(ConditionalWriteError::Conflict { .. }))
            && kept == payload(64, 1),
    });

    // A CAS on a stale version (a lost update) is refused.
    let head = Path::from("checks/head");
    let v1 = conditional_put(store.as_ref(), &head, payload(64, 10).into(), &Expected::Absent)
        .await
        .expect("create head");
    let v2 = conditional_put(
        store.as_ref(),
        &head,
        payload(64, 11).into(),
        &Expected::Version(version_of(&v1)),
    )
    .await
    .expect("cas v1 -> v2");
    let stale = conditional_put(
        store.as_ref(),
        &head,
        payload(64, 12).into(),
        &Expected::Version(version_of(&v1)),
    )
    .await;
    let current = store.get(&head).await.expect("get").bytes().await.expect("bytes");
    checks.push(Check {
        name: "stale CAS (lost update) refused, winner kept",
        expected: "refused (Conflict), v2 bytes".into(),
        seen: format!(
            "{}, {}",
            describe(&stale),
            if current == payload(64, 11) { "v2 bytes" } else { "OTHER bytes" }
        ),
        pass: matches!(stale, Err(ConditionalWriteError::Conflict { .. }))
            && current == payload(64, 11),
    });

    // If-None-Match: the current ETag is 304, a stale one returns the body.
    let unchanged = store
        .get_opts(
            &head,
            GetOptions {
                if_none_match: v2.e_tag.clone(),
                ..GetOptions::default()
            },
        )
        .await;
    let changed = store
        .get_opts(
            &head,
            GetOptions {
                if_none_match: v1.e_tag.clone(),
                ..GetOptions::default()
            },
        )
        .await;
    checks.push(Check {
        name: "conditional GET: current ETag not modified, stale ETag returns body",
        expected: "NotModified, body".into(),
        seen: format!(
            "{}, {}",
            match &unchanged {
                Err(object_store::Error::NotModified { .. }) => "NotModified".to_string(),
                Ok(_) => "body".to_string(),
                Err(err) => format!("error: {err}"),
            },
            match &changed {
                Ok(_) => "body".to_string(),
                Err(err) => format!("error: {err}"),
            }
        ),
        pass: matches!(unchanged, Err(object_store::Error::NotModified { .. })) && changed.is_ok(),
    });

    // Does a version's ETag come back when its bytes are written again? The
    // design rules this out by never repeating head bytes; this records
    // whether the store would otherwise let a delayed CAS apply (ABA).
    let aba = Path::from("checks/aba");
    let a1 = conditional_put(store.as_ref(), &aba, Bytes::from_static(b"same").into(), &Expected::Absent)
        .await
        .expect("aba create");
    let a2 = conditional_put(
        store.as_ref(),
        &aba,
        Bytes::from_static(b"other").into(),
        &Expected::Version(version_of(&a1)),
    )
    .await
    .expect("aba 2");
    let a3 = conditional_put(
        store.as_ref(),
        &aba,
        Bytes::from_static(b"same").into(),
        &Expected::Version(version_of(&a2)),
    )
    .await
    .expect("aba 3");
    let delayed = conditional_put(
        store.as_ref(),
        &aba,
        Bytes::from_static(b"delayed").into(),
        &Expected::Version(version_of(&a1)),
    )
    .await;
    checks.push(Check {
        name: "ETag on identical bytes rewritten (informational: design never repeats head bytes)",
        expected: "recorded".into(),
        seen: format!(
            "etag repeats: {}; delayed CAS on the first version: {}",
            a1.e_tag == a3.e_tag,
            describe(&delayed)
        ),
        pass: true,
    });

    // Many writers CAS the same version at once: exactly one wins, every
    // round.
    let race = Path::from("checks/race");
    let mut version = version_of(
        &conditional_put(store.as_ref(), &race, payload(64, 100).into(), &Expected::Absent)
            .await
            .expect("race create"),
    );
    let rounds = 200_u64;
    let writers = 8_u64;
    let mut bad_rounds = 0;
    let mut other_errors = 0;
    for round in 0..rounds {
        let attempts = (0..writers).map(|writer| {
            let store = Arc::clone(store);
            let race = race.clone();
            let expected = Expected::Version(version.clone());
            async move {
                conditional_put(
                    store.as_ref(),
                    &race,
                    payload(64, 1_000 + round * writers + writer).into(),
                    &expected,
                )
                .await
            }
        });
        let results = futures::future::join_all(attempts).await;
        let winners: Vec<_> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
        other_errors += results
            .iter()
            .filter(|r| matches!(r, Err(e) if !e.is_conflict()))
            .count();
        if winners.len() == 1 {
            version = version_of(winners[0]);
        } else {
            bad_rounds += 1;
            if let Some(winner) = winners.first() {
                version = version_of(winner);
            } else {
                let meta = store.head(&race).await.expect("head race");
                version = UpdateVersion {
                    e_tag: meta.e_tag,
                    version: meta.version,
                };
            }
        }
    }
    checks.push(Check {
        name: "8 concurrent CAS on one version: exactly one wins (200 rounds)",
        expected: "0 rounds with != 1 winner, 0 non-conflict errors".into(),
        seen: format!("{bad_rounds} bad rounds, {other_errors} non-conflict errors"),
        pass: bad_rounds == 0 && other_errors == 0,
    });
    checks
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Bench {
    Create4K,
    Cas4K,
    PollUnchanged,
    PollChanged4K,
    Get4K,
    Get64K,
    Get1M,
    Put1M,
    Put8M,
    Put64M,
}

impl Bench {
    fn name(self) -> &'static str {
        match self {
            Bench::Create4K => "create 4 KiB (If-None-Match: *)",
            Bench::Cas4K => "CAS 4 KiB (If-Match)",
            Bench::PollUnchanged => "poll, unchanged (If-None-Match -> 304)",
            Bench::PollChanged4K => "poll, changed 4 KiB head (If-None-Match -> 200)",
            Bench::Get4K => "GET 4 KiB head",
            Bench::Get64K => "GET 64 KiB head",
            Bench::Get1M => "GET 1 MiB head",
            Bench::Put1M => "PUT 1 MiB batch (create)",
            Bench::Put8M => "PUT 8 MiB batch (create)",
            Bench::Put64M => "PUT 64 MiB batch (create)",
        }
    }
}

struct Fixture {
    head: Path,
    head_version: UpdateVersion,
    stale_poll_etag: Option<String>,
    poll_target: Path,
    poll_etag: Option<String>,
    sized: BTreeMap<usize, Path>,
    blobs: BTreeMap<usize, Bytes>,
    counter: u64,
}

async fn setup(store: &Arc<dyn ObjectStore>) -> Fixture {
    let head = Path::from("bench/head");
    let created = conditional_put(store.as_ref(), &head, payload(4 * KIB, 0).into(), &Expected::Absent)
        .await
        .expect("bench head");
    let poll_target = Path::from("bench/poll");
    let p1 = conditional_put(store.as_ref(), &poll_target, payload(4 * KIB, 1).into(), &Expected::Absent)
        .await
        .expect("poll v1");
    let p2 = conditional_put(
        store.as_ref(),
        &poll_target,
        payload(4 * KIB, 2).into(),
        &Expected::Version(version_of(&p1)),
    )
    .await
    .expect("poll v2");
    let mut sized = BTreeMap::new();
    for size in [4 * KIB, 64 * KIB, MIB] {
        let path = Path::from(format!("bench/get-{size}"));
        store
            .put(&path, payload(size, size as u64).into())
            .await
            .expect("sized get object");
        sized.insert(size, path);
    }
    let mut blobs = BTreeMap::new();
    for size in [MIB, 8 * MIB, 64 * MIB] {
        blobs.insert(size, payload(size, 7 + size as u64));
    }
    Fixture {
        head,
        head_version: version_of(&created),
        stale_poll_etag: p1.e_tag,
        poll_target,
        poll_etag: p2.e_tag,
        sized,
        blobs,
        counter: 0,
    }
}

async fn run_one(store: &Arc<dyn ObjectStore>, fixture: &mut Fixture, bench: Bench) -> Duration {
    fixture.counter += 1;
    let n = fixture.counter;
    match bench {
        Bench::Create4K => {
            let path = Path::from(format!("bench/create/{n:020}"));
            let bytes = payload(4 * KIB, n);
            let start = Instant::now();
            conditional_put(store.as_ref(), &path, bytes.into(), &Expected::Absent)
                .await
                .expect("create");
            start.elapsed()
        }
        Bench::Cas4K => {
            let bytes = payload(4 * KIB, n);
            let expected = Expected::Version(fixture.head_version.clone());
            let start = Instant::now();
            let result = conditional_put(store.as_ref(), &fixture.head, bytes.into(), &expected)
                .await
                .expect("cas");
            let elapsed = start.elapsed();
            fixture.head_version = version_of(&result);
            elapsed
        }
        Bench::PollUnchanged => {
            let options = GetOptions {
                if_none_match: fixture.poll_etag.clone(),
                ..GetOptions::default()
            };
            let start = Instant::now();
            let result = store.get_opts(&fixture.poll_target, options).await;
            let elapsed = start.elapsed();
            assert!(
                matches!(result, Err(object_store::Error::NotModified { .. })),
                "expected NotModified"
            );
            elapsed
        }
        Bench::PollChanged4K => {
            let options = GetOptions {
                if_none_match: fixture.stale_poll_etag.clone(),
                ..GetOptions::default()
            };
            let start = Instant::now();
            let body = store
                .get_opts(&fixture.poll_target, options)
                .await
                .expect("changed poll")
                .bytes()
                .await
                .expect("changed poll bytes");
            let elapsed = start.elapsed();
            assert_eq!(body.len(), 4 * KIB);
            elapsed
        }
        Bench::Get4K | Bench::Get64K | Bench::Get1M => {
            let size = match bench {
                Bench::Get4K => 4 * KIB,
                Bench::Get64K => 64 * KIB,
                _ => MIB,
            };
            let path = fixture.sized[&size].clone();
            let start = Instant::now();
            let body = store.get(&path).await.expect("get").bytes().await.expect("bytes");
            let elapsed = start.elapsed();
            assert_eq!(body.len(), size);
            elapsed
        }
        Bench::Put1M | Bench::Put8M | Bench::Put64M => {
            let size = match bench {
                Bench::Put1M => MIB,
                Bench::Put8M => 8 * MIB,
                _ => 64 * MIB,
            };
            // Distinct leading bytes per object; the rest is shared and not
            // regenerated, which would dominate the timing.
            let blob = fixture.blobs[&size].clone();
            let mut head_bytes = n.to_le_bytes().to_vec();
            head_bytes.extend_from_slice(&blob[8..]);
            let bytes = Bytes::from(head_bytes);
            let path = Path::from(format!("bench/batch-{size}/{n:020}"));
            let start = Instant::now();
            conditional_put(store.as_ref(), &path, bytes.into(), &Expected::Absent)
                .await
                .expect("batch put");
            let elapsed = start.elapsed();
            // Bound the disk the run uses; the delete is not timed.
            store.delete(&path).await.expect("delete batch");
            elapsed
        }
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Option<Duration> {
    // A percentile counts only with at least 10 samples beyond it.
    let beyond = (sorted.len() as f64) * (1.0 - p);
    if beyond < 10.0 {
        return None;
    }
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    Some(sorted[rank - 1])
}

fn ms(duration: Option<Duration>) -> String {
    duration.map_or_else(
        || "n/a (too few samples)".into(),
        |d| format!("{:.3}", d.as_secs_f64() * 1e3),
    )
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let out_dir = std::path::PathBuf::from(&args[0]);
    std::fs::create_dir_all(&out_dir).expect("out dir");
    let run = format!("{}", uuid::Uuid::now_v7().simple());
    let counts: BTreeMap<Bench, usize> = [
        (Bench::Create4K, env_count("ONP_N_SMALL", 10_000)),
        (Bench::Cas4K, env_count("ONP_N_SMALL", 10_000)),
        (Bench::PollUnchanged, env_count("ONP_N_SMALL", 10_000)),
        (Bench::PollChanged4K, env_count("ONP_N_SMALL", 10_000)),
        (Bench::Get4K, env_count("ONP_N_SMALL", 10_000)),
        (Bench::Get64K, env_count("ONP_N_SMALL", 10_000)),
        (Bench::Get1M, env_count("ONP_N_SMALL", 10_000)),
        (Bench::Put1M, env_count("ONP_N_1M", 10_000)),
        (Bench::Put8M, env_count("ONP_N_8M", 1_000)),
        (Bench::Put64M, env_count("ONP_N_64M", 1_000)),
    ]
    .into_iter()
    .collect();

    let mut report = String::new();
    for spec in &args[1..] {
        let (label, store) = build_store(spec, &run);
        eprintln!("== {label}: condition checks");
        let checks = condition_checks(&store).await;
        writeln!(report, "## {label}\n\nrun prefix `{run}`\n\n| check | expected | seen | result |\n|---|---|---|---|").expect("write");
        for check in &checks {
            writeln!(
                report,
                "| {} | {} | {} | {} |",
                check.name,
                check.expected,
                check.seen,
                if check.pass { "PASS" } else { "FAIL" }
            )
            .expect("write");
        }

        eprintln!("== {label}: latency");
        let mut fixture = setup(&store).await;
        let mut samples: BTreeMap<Bench, Vec<Duration>> = BTreeMap::new();
        // Interleave the operations in blocks so drift affects them alike.
        let block = 50;
        let mut remaining = counts.clone();
        let started = Instant::now();
        while remaining.values().any(|left| *left > 0) {
            for (bench, left) in &mut remaining {
                for _ in 0..block.min(*left) {
                    let elapsed = run_one(&store, &mut fixture, *bench).await;
                    samples.entry(*bench).or_default().push(elapsed);
                }
                *left -= block.min(*left);
            }
        }
        eprintln!("   took {:?}", started.elapsed());
        let mut raw = String::from("op,micros\n");
        writeln!(
            report,
            "\n| operation | n | P50 ms | P99 ms | P99.9 ms | max ms |\n|---|---|---|---|---|---|"
        )
        .expect("write");
        for (bench, values) in &mut samples {
            for value in values.iter() {
                writeln!(raw, "{:?},{}", bench, value.as_micros()).expect("raw");
            }
            values.sort();
            writeln!(
                report,
                "| {} | {} | {} | {} | {} | {} |",
                bench.name(),
                values.len(),
                ms(percentile(values, 0.5)),
                ms(percentile(values, 0.99)),
                ms(percentile(values, 0.999)),
                ms(values.last().copied())
            )
            .expect("write");
        }
        report.push('\n');
        let safe = label.replace([' ', '(', ')', '/'], "_");
        std::fs::write(out_dir.join(format!("raw-{safe}.csv")), raw).expect("raw csv");
        std::fs::write(out_dir.join("report.md"), &report).expect("report");
    }
    println!("{report}");
}
