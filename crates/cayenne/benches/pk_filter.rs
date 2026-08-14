/*
Copyright 2026 The Spice.ai OSS Authors

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

//! A/B lanes for the primary-key existence filter, at the scales and key shapes
//! the CDC upsert path and the cold-tier promotion actually present.
//!
//! # Arms, chosen so a win can be attributed rather than just observed
//!
//! | arm | isolates |
//! |---|---|
//! | `shipping` | today's `PkBloom`: 7 scattered probes, **two** full FNV-1a passes per key |
//! | `scattered_xxh3` | the hash alone — same layout and `k`, one XXH3 split in half |
//! | `blocked_256/512/1024` | cache-line width: a line is 64 B on x86-64, 128 B on Apple silicon |
//! | `split_block` | SIMD: Parquet/Impala 8x`u32` lanes, one bit per lane, branch-free |
//! | `fastbloom` | the maintained crate — if it wins, use it instead of hand-rolling |
//! | `fuse8` | the static solve-at-build family (ribbon / XOR / fuse), cold tier only |
//!
//! Reading the arms in order attributes any difference: `shipping` to
//! `scattered_xxh3` is the hash function, `scattered_xxh3` to `blocked_*` is
//! locality, `blocked_*` across widths is the cache line, and `blocked_512` to
//! `split_block` is branch-free lane parallelism.
//!
//! # Two lanes, because the two call sites have different contracts
//!
//! * **`resident/`** — the PK existence index the CDC apply grows key-by-key and
//!   probes on every upsert. It must accept an insert after construction, which
//!   is what excludes every static filter from this lane.
//! * **`cold_tier/`** — the per-file bloom built once at promotion and thereafter
//!   only probed. `build` here is bulk, not incremental, so a static filter can
//!   compete; `fuse8` stands in for that family.
//!
//! The cold-tier lane measures build as a **whole-set** operation for every arm,
//! including the blooms, so the comparison is like-for-like. That flatters the
//! static filter relative to production, where today's build streams a scan and
//! inserts as it goes with no key set resident: `fuse8` needs every key hash in
//! memory at once to solve. At the 32 MiB per-file cap (~26M keys) that is
//! ~208 MiB of hashes plus the solver's own working set — a real cost that no
//! timing here shows, and one that belongs beside any size win.
//!
//! # Shapes
//!
//! * **Scales** — 10K to 10M keys. The small end sits in cache and measures
//!   instruction cost; the large end exceeds L2/L3 and measures what scattered
//!   probes cost in misses. The shipping budget (~1/32 of host RAM, clamped
//!   256 MiB–8 GiB) admits filters far past cache, so the large end is the
//!   operating point rather than an extreme.
//! * **Keys** — 16 bytes, the `RowConverter` encoding of a composite primary
//!   key. Byte-at-a-time hashing scales with this length; a 4-byte key would
//!   flatter the shipping arm.
//! * **Probes** — `hit` and `miss` separately. Upsert conflict detection is
//!   miss-dominated on a growing table and hit-dominated on a churning one, and
//!   they cost differently: a miss usually exits on the first zero bit, a hit
//!   always reads all `k`.
//!
//! Every arm is sized to the same bit count, so timings compare equal memory.
//! Accuracy is not equal at equal size, so the size/FPR table printed before the
//! timings is part of the result: a filter that is faster and less accurate has
//! not necessarily won, because a false positive costs a redundant tombstone on
//! the write path and a needlessly dirty file on the cold path.

use std::hint::black_box;

use cayenne::provider::pk_filter::{
    BlockedBloom, ScatteredBloomXxh3, ShippingBloom, SplitBlockBloom,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use twox_hash::XxHash3_64;
use xorf::{BinaryFuse8, Filter};

const SCALES: &[usize] = &[10_000, 100_000, 1_000_000, 10_000_000];

/// Held equal across arms, matching `PkBloom::with_expected_keys`. Note that the
/// power-of-two sizing rounds this DOWN — see the size table, which reports what
/// each arm actually received.
const BITS_PER_KEY: usize = 10;

/// Probe batch size: the apply path's per-batch order of magnitude.
const PROBE_BATCH: usize = 4096;

/// 16-byte keys: the composite-PK `RowConverter` encoding.
fn make_keys(count: usize, salt: u128) -> Vec<[u8; 16]> {
    (0..count)
        .map(|i| {
            let n = salt.wrapping_add(u128::try_from(i).unwrap_or(0));
            // Mix so keys are not densely sequential in the low bits, which
            // would flatter any filter that masks them for placement.
            n.wrapping_mul(0x9e37_79b9_7f4a_7c15_9e37_79b9_7f4a_7c15)
                .to_le_bytes()
        })
        .collect()
}

/// `fuse8` keys are `u64`, so the byte keys are hashed first — the same XXH3 the
/// other candidate arms use, so the hash cost is charged to every arm alike.
fn hash64(key: &[u8]) -> u64 {
    XxHash3_64::oneshot(key)
}

/// One filter under test, reduced to what the lanes need.
trait Arm {
    fn name() -> &'static str;
    fn build(bits: usize, keys: &[[u8; 16]]) -> Self;
    fn probe(&self, key: &[u8; 16]) -> bool;
    fn size_bytes(&self) -> usize;
}

macro_rules! bloom_arm {
    ($ty:ty, $name:literal) => {
        impl Arm for $ty {
            fn name() -> &'static str {
                $name
            }
            fn build(bits: usize, keys: &[[u8; 16]]) -> Self {
                let mut f = <$ty>::with_num_bits_pow2(bits);
                for key in keys {
                    f.insert(key);
                }
                f
            }
            fn probe(&self, key: &[u8; 16]) -> bool {
                self.maybe_contains(key)
            }
            fn size_bytes(&self) -> usize {
                self.size_bytes()
            }
        }
    };
}

bloom_arm!(ShippingBloom, "shipping");
bloom_arm!(ScatteredBloomXxh3, "scattered_xxh3");
bloom_arm!(BlockedBloom<256>, "blocked_256");
bloom_arm!(BlockedBloom<512>, "blocked_512");
bloom_arm!(BlockedBloom<1024>, "blocked_1024");
bloom_arm!(SplitBlockBloom, "split_block");

/// The maintained crate, sized by bit count so it sits at the same memory as the
/// hand-rolled arms.
///
/// Configured with `ahash` rather than the default hasher. fastbloom defaults to
/// SipHash-1-3, which is a keyed, DoS-resistant hash and costs several times what
/// a non-cryptographic one does — measuring the crate on that default would
/// repeat the mistake this benchmark already made once with streaming XXH3, and
/// would blame the filter for its hash.
struct FastBloomArm(fastbloom::BloomFilter<ahash::RandomState>);

impl Arm for FastBloomArm {
    fn name() -> &'static str {
        "fastbloom"
    }
    fn build(bits: usize, keys: &[[u8; 16]]) -> Self {
        let mut f = fastbloom::BloomFilter::with_num_bits(bits)
            .hasher(ahash::RandomState::new())
            .expected_items(keys.len());
        for key in keys {
            f.insert(key);
        }
        Self(f)
    }
    fn probe(&self, key: &[u8; 16]) -> bool {
        self.0.contains(key)
    }
    fn size_bytes(&self) -> usize {
        self.0.num_bits() / 8
    }
}

/// The static family. Construction solves for the whole key set, so it cannot
/// take an insert afterwards — hence cold tier only.
struct Fuse8Arm(BinaryFuse8);

impl Arm for Fuse8Arm {
    fn name() -> &'static str {
        "fuse8"
    }
    /// `bits` is ignored: a fuse filter's size is a property of the key count
    /// (~9.1 bits/key), not a parameter. That is the comparison, not a flaw.
    fn build(_bits: usize, keys: &[[u8; 16]]) -> Self {
        let hashes: Vec<u64> = keys.iter().map(|k| hash64(k)).collect();
        Self(BinaryFuse8::try_from(&hashes).expect("fuse8 construction failed"))
    }
    fn probe(&self, key: &[u8; 16]) -> bool {
        self.0.contains(&hash64(key))
    }
    fn size_bytes(&self) -> usize {
        // Fingerprints dominate; one byte each.
        self.0.fingerprints.len()
    }
}

/// Incremental insert: the CDC apply path's cost per key recorded.
fn bench_insert<A: Arm>(c: &mut Criterion) {
    let mut group = c.benchmark_group("pk_filter/resident/insert");
    for &scale in SCALES {
        let keys = make_keys(scale, 0);
        let bits = scale * BITS_PER_KEY;
        group.throughput(Throughput::Elements(u64::try_from(scale).unwrap_or(0)));
        group.bench_with_input(BenchmarkId::new(A::name(), scale), &keys, |b, keys| {
            b.iter(|| black_box(A::build(bits, keys).size_bytes()));
        });
    }
    group.finish();
}

/// Probe throughput, split by outcome.
fn bench_probe<A: Arm>(c: &mut Criterion, present: bool) {
    let label = if present { "hit" } else { "miss" };
    let mut group = c.benchmark_group(format!("pk_filter/resident/probe_{label}"));
    for &scale in SCALES {
        let inserted = make_keys(scale, 0);
        let probe_count = PROBE_BATCH.min(scale);
        let probes: Vec<[u8; 16]> = if present {
            inserted
                .iter()
                .step_by((scale / probe_count).max(1))
                .copied()
                .take(probe_count)
                .collect()
        } else {
            make_keys(probe_count, 1 << 100)
        };
        let filter = A::build(scale * BITS_PER_KEY, &inserted);

        group.throughput(Throughput::Elements(
            u64::try_from(probes.len()).unwrap_or(0),
        ));
        group.bench_with_input(BenchmarkId::new(A::name(), scale), &probes, |b, probes| {
            b.iter(|| {
                let mut found = 0usize;
                for key in probes {
                    found += usize::from(filter.probe(black_box(key)));
                }
                black_box(found)
            });
        });
    }
    group.finish();
}

/// Cold tier: build is a whole-set operation, so a static filter can compete.
fn bench_cold_build<A: Arm>(c: &mut Criterion) {
    let mut group = c.benchmark_group("pk_filter/cold_tier/build");
    // Per-file key counts: the 32 MiB cap admits ~26M keys, but promotion
    // row-caps output files well below that.
    for &scale in &[100_000usize, 1_000_000] {
        let keys = make_keys(scale, 0);
        let bits = scale * BITS_PER_KEY;
        group.throughput(Throughput::Elements(u64::try_from(scale).unwrap_or(0)));
        group.bench_with_input(BenchmarkId::new(A::name(), scale), &keys, |b, keys| {
            b.iter(|| black_box(A::build(bits, keys).size_bytes()));
        });
    }
    group.finish();
}

/// Size and measured false-positive rate, printed before the timings.
fn report_size_and_fpr() {
    println!("\n=== pk_filter: size and false-positive rate ===");
    println!(
        "(blooms sized at {BITS_PER_KEY} bits/key before power-of-two rounding; fuse8 sizes itself)"
    );
    println!(
        "{:>10}  {:<16} {:>12} {:>10} {:>9}",
        "keys", "arm", "bytes", "bits/key", "fpr"
    );

    fn row<A: Arm>(scale: usize, inserted: &[[u8; 16]], absent: &[[u8; 16]]) {
        let filter = A::build(scale * BITS_PER_KEY, inserted);
        let hits = absent.iter().filter(|k| filter.probe(k)).count();
        let bytes = filter.size_bytes();
        #[expect(clippy::cast_precision_loss, reason = "reporting only")]
        {
            println!(
                "{:>10}  {:<16} {:>12} {:>10.2} {:>8.3}%",
                scale,
                A::name(),
                bytes,
                (bytes * 8) as f64 / scale as f64,
                100.0 * hits as f64 / absent.len() as f64,
            );
        }
    }

    for &scale in &[100_000usize, 1_000_000] {
        let inserted = make_keys(scale, 0);
        let absent = make_keys(100_000, 1 << 100);
        row::<ShippingBloom>(scale, &inserted, &absent);
        row::<ScatteredBloomXxh3>(scale, &inserted, &absent);
        row::<BlockedBloom<256>>(scale, &inserted, &absent);
        row::<BlockedBloom<512>>(scale, &inserted, &absent);
        row::<BlockedBloom<1024>>(scale, &inserted, &absent);
        row::<SplitBlockBloom>(scale, &inserted, &absent);
        row::<FastBloomArm>(scale, &inserted, &absent);
        row::<Fuse8Arm>(scale, &inserted, &absent);
        println!();
    }
}

fn benches(c: &mut Criterion) {
    report_size_and_fpr();

    bench_insert::<ShippingBloom>(c);
    bench_insert::<ScatteredBloomXxh3>(c);
    bench_insert::<BlockedBloom<256>>(c);
    bench_insert::<BlockedBloom<512>>(c);
    bench_insert::<BlockedBloom<1024>>(c);
    bench_insert::<SplitBlockBloom>(c);
    bench_insert::<FastBloomArm>(c);

    for present in [true, false] {
        bench_probe::<ShippingBloom>(c, present);
        bench_probe::<ScatteredBloomXxh3>(c, present);
        bench_probe::<BlockedBloom<256>>(c, present);
        bench_probe::<BlockedBloom<512>>(c, present);
        bench_probe::<BlockedBloom<1024>>(c, present);
        bench_probe::<SplitBlockBloom>(c, present);
        bench_probe::<FastBloomArm>(c, present);
        bench_probe::<Fuse8Arm>(c, present);
    }

    bench_cold_build::<ShippingBloom>(c);
    bench_cold_build::<BlockedBloom<512>>(c);
    bench_cold_build::<SplitBlockBloom>(c);
    bench_cold_build::<FastBloomArm>(c);
    bench_cold_build::<Fuse8Arm>(c);
}

criterion_group!(pk_filter, benches);
criterion_main!(pk_filter);
