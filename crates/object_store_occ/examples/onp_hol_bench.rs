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

//! Head-of-line blocking on one store root: a writer CASes a 4 KiB head
//! back to back (with a short pause) while N tasks upload immutable objects
//! of size S into the same root, as a commit's uploads and other datasets'
//! uploads would. Reports the CAS latency tail per arm.
//!
//! ```sh
//! cargo run --release -p object_store_occ --example onp_hol_bench -- \
//!     <out_dir> <store> <samples> <arm>...
//! ```
//!
//! Store: `local-durable:<dir>` or `rustfs`. Arm: `<uploaders>x<size_kib>`,
//! e.g. `0x0 4x1024 16x1024 4x65536`.

#![expect(clippy::expect_used, reason = "an experiment binary")]

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{ObjectStore, ObjectStoreExt, UpdateVersion};
use object_store_occ::{Expected, LocalConditionalPut, conditional_put};

fn store(spec: &str, run: &str) -> Arc<dyn ObjectStore> {
    if spec == "rustfs" {
        let s3 = AmazonS3Builder::new()
            .with_endpoint("http://127.0.0.1:19000")
            .with_allow_http(true)
            .with_bucket_name("onp")
            .with_region("us-east-1")
            .with_access_key_id("onpadmin")
            .with_secret_access_key("onpadmin123")
            .build()
            .expect("s3");
        return Arc::new(PrefixStore::new(s3, format!("hol/{run}")));
    }
    let dir = spec.strip_prefix("local-durable:").expect("local-durable:<dir>");
    let dir = std::path::PathBuf::from(dir).join(run);
    std::fs::create_dir_all(&dir).expect("dir");
    Arc::new(LocalConditionalPut::new_durable(&dir).expect("local"))
}

fn percentile(sorted: &[Duration], p: f64) -> String {
    let beyond = sorted.len() as f64 * (1.0 - p);
    if beyond < 10.0 {
        return "n/a".into();
    }
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    format!("{:.3}", sorted[rank - 1].as_secs_f64() * 1e3)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let out = std::path::PathBuf::from(&args[0]);
    std::fs::create_dir_all(&out).expect("out");
    let spec = args[1].clone();
    let samples: usize = args[2].parse().expect("samples");
    let mut report = format!(
        "store `{spec}`, {samples} CAS samples per arm\n\n| arm (uploaders x KiB) | n | CAS P50 ms | P99 ms | P99.9 ms | max ms | uploads | upload MiB/s |\n|---|---|---|---|---|---|---|---|\n"
    );
    let mut raw = String::from("arm,micros\n");
    for arm in &args[3..] {
        let (uploaders, kib) = arm.split_once('x').expect("arm NxKiB");
        let uploaders: usize = uploaders.parse().expect("uploaders");
        let size = kib.parse::<usize>().expect("kib") * 1024;
        let run = uuid::Uuid::now_v7().simple().to_string();
        let store = store(&spec, &run);
        let stop = Arc::new(AtomicBool::new(false));
        let uploaded = Arc::new(AtomicU64::new(0));
        let blob = Bytes::from(vec![7_u8; size.max(1)]);
        let mut tasks = Vec::new();
        for u in 0..uploaders {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            let uploaded = Arc::clone(&uploaded);
            let blob = blob.clone();
            tasks.push(tokio::spawn(async move {
                let mut n = 0_u64;
                while !stop.load(Ordering::Relaxed) {
                    let path = Path::from(format!("data/{u}/{n:020}"));
                    conditional_put(store.as_ref(), &path, blob.clone().into(), &Expected::Absent)
                        .await
                        .expect("upload");
                    store.delete(&path).await.expect("delete");
                    uploaded.fetch_add(1, Ordering::Relaxed);
                    n += 1;
                }
            }));
        }
        // Let the uploaders reach steady state first.
        tokio::time::sleep(Duration::from_millis(if uploaders == 0 { 0 } else { 2000 })).await;
        let head = Path::from("head");
        let created = conditional_put(store.as_ref(), &head, Bytes::from(vec![0_u8; 4096]).into(), &Expected::Absent)
            .await
            .expect("head");
        let mut version = UpdateVersion {
            e_tag: created.e_tag,
            version: created.version,
        };
        let uploads_before = uploaded.load(Ordering::Relaxed);
        let started = Instant::now();
        let mut latencies = Vec::with_capacity(samples);
        for i in 0..samples {
            let mut bytes = vec![0_u8; 4096];
            bytes[..8].copy_from_slice(&(i as u64 + 1).to_le_bytes());
            let start = Instant::now();
            let result = conditional_put(
                store.as_ref(),
                &head,
                Bytes::from(bytes).into(),
                &Expected::Version(version.clone()),
            )
            .await
            .expect("cas");
            latencies.push(start.elapsed());
            version = UpdateVersion {
                e_tag: result.e_tag,
                version: result.version,
            };
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let elapsed = started.elapsed();
        let uploads = uploaded.load(Ordering::Relaxed) - uploads_before;
        stop.store(true, Ordering::Relaxed);
        for task in tasks {
            task.await.expect("uploader");
        }
        for l in &latencies {
            writeln!(raw, "{arm},{}", l.as_micros()).expect("raw");
        }
        latencies.sort();
        let mib_s = (uploads as f64 * size as f64 / 1_048_576.0) / elapsed.as_secs_f64();
        writeln!(
            report,
            "| {arm} | {} | {} | {} | {} | {:.3} | {uploads} | {mib_s:.1} |",
            latencies.len(),
            percentile(&latencies, 0.5),
            percentile(&latencies, 0.99),
            percentile(&latencies, 0.999),
            latencies.last().expect("samples").as_secs_f64() * 1e3,
        )
        .expect("report");
        eprintln!("{arm} done in {elapsed:?}");
        std::fs::write(out.join("report.md"), &report).expect("report");
        std::fs::write(out.join("raw.csv"), &raw).expect("raw");
    }
    println!("{report}");
}
