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

//! The slab store's contract: bytes round-trip and survive a reopen, a
//! same-length corruption, a truncation or another block's entry is a miss
//! (never other bytes), interrupted writes leave nothing behind, a slab whose
//! files do not load is quarantined alone, and the rescan and reclaim paths.

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use object_store_cache::testing::{BlockKey, BlockStore, SlabStore, StoredMeta, entry_len};

use crate::support::reclaim_one;

const MIB: usize = 1 << 20;

async fn open(dir: &Path) -> Arc<dyn BlockStore> {
    SlabStore::open(dir, 8 * MIB as u64)
        .await
        .expect("open slab store")
}

/// True when `got` is exactly `want` (compares without printing 1 MiB values).
fn same(got: Option<&Bytes>, want: &Bytes) -> bool {
    got == Some(want)
}

fn key(i: u64) -> BlockKey {
    BlockKey {
        store: "s3://bucket".to_string(),
        path: format!("data/file-{}.vortex", i / 16),
        object_size: 16 * MIB as u64,
        block_size: MIB as u64,
        index: i % 16,
    }
}

/// Deterministic bytes for block `seed` (splitmix64).
fn block(seed: u64, len: usize) -> Bytes {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        out.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    out.truncate(len);
    Bytes::from(out)
}

/// The slab file and offset of the first data byte of `data`.
fn locate(dir: &Path, data: &[u8]) -> (PathBuf, u64) {
    search(&dir.join("slabs"), data)
}

/// Finds the only occurrence of `data`'s first 64 bytes under `dir`.
fn search(dir: &Path, data: &[u8]) -> (PathBuf, u64) {
    let needle = &data[..64];
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).expect("list") {
        let path = entry.expect("entry").path();
        let hay = std::fs::read(&path).expect("read");
        let mut at = 0;
        while let Some(pos) = hay[at..].windows(64).position(|w| w == needle) {
            found.push((path.clone(), (at + pos) as u64));
            at += pos + 1;
        }
    }
    assert_eq!(
        found.len(),
        1,
        "expected one copy of the block, found {found:?}"
    );
    found.remove(0)
}

fn flip_byte(path: &Path, offset: u64) {
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open");
    let mut b = [0u8; 1];
    f.read_exact_at(&mut b, offset).expect("read byte");
    b[0] ^= 0x5A;
    f.write_all_at(&b, offset).expect("write byte");
}

#[tokio::test]
async fn round_trip_survives_reopen() {
    let sizes = [1usize, 4095, 4096, 4097, MIB, 3 * MIB + 7];
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut expected = Vec::new();
        for (i, len) in sizes
            .iter()
            .copied()
            .chain(std::iter::repeat_n(MIB, 12))
            .enumerate()
        {
            expected.push((key(i as u64), block(i as u64, len)));
        }
        let store = open(tmp.path()).await;
        for (k, v) in &expected {
            store
                .put(k, v.clone(), &StoredMeta::NONE)
                .await
                .expect("put");
        }
        for (k, v) in &expected {
            assert!(
                same(store.get(k).await.expect("get").as_ref(), v),
                "{k:?} before reopen"
            );
        }
        assert!(store.get(&key(999)).await.expect("get").is_none());
        assert_eq!(store.len(), expected.len() as u64);
        let total: u64 = expected.iter().map(|(_, v)| v.len() as u64).sum();
        assert_eq!(store.resident_bytes(), total);
        store.close().await.expect("close");
        drop(store);

        let store = open(tmp.path()).await;
        for (k, v) in &expected {
            assert!(
                same(store.get(k).await.expect("get").as_ref(), v),
                "{k:?} after reopen"
            );
        }
        assert_eq!(store.len(), expected.len() as u64, "after reopen");
        assert_eq!(store.resident_bytes(), total, "after reopen");
        assert_eq!(store.counters().corrupt_reads, 0);
        store.close().await.expect("close");
    }
}

#[tokio::test]
async fn same_length_corruption_is_a_miss() {
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = open(tmp.path()).await;
        let entries: Vec<_> = (0..8).map(|i| (key(i), block(i, MIB))).collect();
        for (k, v) in &entries {
            store
                .put(k, v.clone(), &StoredMeta::NONE)
                .await
                .expect("put");
        }
        store.close().await.expect("close");
        drop(store);
        let store = open(tmp.path()).await;

        let (k3, v3) = &entries[3];
        let (path, data_at) = locate(tmp.path(), v3);
        flip_byte(&path, data_at + 777_777);

        assert!(
            store.get(k3).await.expect("get").is_none(),
            "corrupt block served"
        );
        assert!(
            store.get(k3).await.expect("get").is_none(),
            "corrupt block kept"
        );
        assert_eq!(store.counters().corrupt_reads, 1);
        for (i, (k, v)) in entries.iter().enumerate() {
            if i != 3 {
                assert!(
                    same(store.get(k).await.expect("get").as_ref(), v),
                    "key {i}"
                );
            }
        }
        store.close().await.expect("close");
    }
}

#[tokio::test]
async fn digest_collision_is_a_miss() {
    // Another block's whole entry where this key's entry is: the stored key
    // differs, so the entry is a miss, never the other block's bytes.
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("open");
    store
        .put(&key(1), block(1, 4096), &StoredMeta::NONE)
        .await
        .expect("put");
    store
        .put(&key(2), block(2, 4096), &StoredMeta::NONE)
        .await
        .expect("put");
    let (path1, data1) = store.data_location(&key(1)).expect("located");
    let (path2, data2) = store.data_location(&key(2)).expect("located");
    let entry_len = 4096 + 4096;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path1)
        .expect("open");
    let mut entry = vec![0u8; entry_len];
    f.read_exact_at(&mut entry, data1 - 4096)
        .expect("read entry 1");
    drop(f);
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&path2)
        .expect("open");
    f.write_all_at(&entry, data2 - 4096)
        .expect("overwrite entry 2");
    drop(f);
    assert!(store.get(&key(2)).await.expect("get").is_none());
    assert_eq!(store.counters().corrupt_reads, 1);
    assert!(same(
        store.get(&key(1)).await.expect("get").as_ref(),
        &block(1, 4096)
    ));
}

#[tokio::test]
async fn truncated_entry_is_a_miss() {
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = open(tmp.path()).await;
        let v = block(7, MIB);
        store
            .put(&key(7), v.clone(), &StoredMeta::NONE)
            .await
            .expect("put");
        let (path, data_at) = locate(tmp.path(), &v);
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open");
        f.set_len(data_at + MIB as u64 - 1).expect("truncate");
        assert!(store.get(&key(7)).await.expect("get").is_none());
        assert_eq!(store.len(), 0);
    }
}

#[tokio::test]
async fn interrupted_write_leftovers_are_removed_on_open() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = open(tmp.path()).await;
    store
        .put(&key(1), block(1, 4096), &StoredMeta::NONE)
        .await
        .expect("put");
    store.close().await.expect("close");
    std::fs::write(
        tmp.path().join("slabs").join("slab-00000009.idx.tmp"),
        b"torn",
    )
    .expect("tmp");
    let store = open(tmp.path()).await;
    assert!(
        !tmp.path()
            .join("slabs")
            .join("slab-00000009.idx.tmp")
            .exists()
    );
    assert_eq!(store.len(), 1);
}

#[tokio::test]
async fn unsealed_slab_is_rescanned_and_torn_tail_dropped() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("open");
    for i in 0..5 {
        store
            .put(&key(i), block(i, 100_000), &StoredMeta::NONE)
            .await
            .expect("put");
    }
    // No close: the active slab has no `.idx`, as after a crash. Tear the last
    // entry's data so its CRC fails (the header still verifies).
    let (path, at) = store.data_location(&key(4)).expect("located");
    drop(store);
    flip_byte(&path, at + 5);
    assert!(!tmp.path().join("slabs").join("slab-00000000.idx").exists());

    // The rescan checks every entry's data, so the torn entry is not listed
    // (and so never read as a corrupt entry).
    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("reopen");
    assert!(tmp.path().join("slabs").join("slab-00000000.idx").exists());
    assert_eq!(store.len(), 4);
    for i in 0..4 {
        assert!(same(
            store.get(&key(i)).await.expect("get").as_ref(),
            &block(i, 100_000)
        ));
    }
    assert!(store.get(&key(4)).await.expect("get").is_none());
    assert_eq!(store.counters().corrupt_reads, 0);
}

#[tokio::test]
async fn torn_tail_is_not_relisted() {
    // A torn tail in the slab active at a crash (no `.idx`), and a
    // same-length corruption in a sealed slab (listed by its `.idx`). Over
    // three reopens neither is served, and the corruption is counted once:
    // the first read finds it and tombstones it.
    let tmp = tempfile::tempdir().expect("tempdir");
    let slab = 3 * entry_len(100_000);
    let store = SlabStore::open(tmp.path(), slab).await.expect("open");
    for i in 0..5 {
        store
            .put(&key(i), block(i, 100_000), &StoredMeta::NONE)
            .await
            .expect("put");
    }
    // Keys 0, 1 and 2 fill slab 0, which the fourth put sealed; keys 3 and 4 are in
    // slab 1, left without an index.
    let (sealed_path, sealed_at) = store.data_location(&key(1)).expect("located");
    let (torn_path, torn_at) = store.data_location(&key(4)).expect("located");
    drop(store);
    assert!(tmp.path().join("slabs").join("slab-00000000.idx").exists());
    assert!(!tmp.path().join("slabs").join("slab-00000001.idx").exists());
    flip_byte(&sealed_path, sealed_at + 99);
    flip_byte(&torn_path, torn_at + 5);

    let mut corrupt = Vec::new();
    for round in 0..3 {
        let store = SlabStore::open(tmp.path(), slab).await.expect("reopen");
        let mut served = Vec::new();
        for i in 0..5 {
            if let Some(b) = store.get(&key(i)).await.expect("get") {
                assert!(same(Some(&b), &block(i, 100_000)), "round {round} key {i}");
                served.push(i);
            }
        }
        assert_eq!(served, vec![0, 2, 3], "round {round}");
        assert_eq!(store.len(), 3, "round {round}");
        corrupt.push(store.counters().corrupt_reads);
    }
    assert_eq!(corrupt, vec![1, 0, 0], "corrupt reads per reopen");
}

#[tokio::test]
async fn removed_entry_stays_removed_after_restart() {
    for sealed in [true, false] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
            .await
            .expect("open");
        for i in 0..6 {
            store
                .put(&key(i), block(i, 100_000), &StoredMeta::NONE)
                .await
                .expect("put");
        }
        if sealed {
            store.close().await.expect("close");
        }
        store.remove(&key(1)).await.expect("remove");
        store.remove(&key(4)).await.expect("remove");
        drop(store);
        for round in 0..2 {
            let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
                .await
                .expect("reopen");
            let mut served = Vec::new();
            for i in 0..6 {
                if let Some(b) = store.get(&key(i)).await.expect("get") {
                    assert!(same(Some(&b), &block(i, 100_000)), "key {i}");
                    served.push(i);
                }
            }
            assert_eq!(served, vec![0, 2, 3, 5], "sealed={sealed} round {round}");
            assert_eq!(store.len(), 4, "sealed={sealed} round {round}");
            assert_eq!(store.counters().corrupt_reads, 0);
        }
        // A removed key written again is listed again after a restart.
        let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
            .await
            .expect("reopen");
        store
            .put(&key(4), block(4, 100_000), &StoredMeta::NONE)
            .await
            .expect("put again");
        store.close().await.expect("close");
        drop(store);
        let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
            .await
            .expect("reopen");
        assert!(same(
            store.get(&key(4)).await.expect("get").as_ref(),
            &block(4, 100_000)
        ));
        assert_eq!(store.len(), 5, "sealed={sealed}: rewritten key");
    }
}

#[tokio::test]
async fn slab_reclaim_drops_the_emptiest_slab() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // 1 MiB slabs hold three 256 KiB entries each (4 KiB header + 256 KiB).
    let store = SlabStore::open(tmp.path(), MIB as u64).await.expect("open");
    for i in 0..12 {
        store
            .put(&key(i), block(i, 256 << 10), &StoredMeta::NONE)
            .await
            .expect("put");
    }
    store.close().await.expect("close");
    assert_eq!(store.slab_count(), 4);
    // Slab 1 holds keys 3, 4, 5: remove two, reclaim, the third goes with it.
    store.remove(&key(3)).await.expect("remove");
    store.remove(&key(4)).await.expect("remove");
    let freed = reclaim_one(&*store).await.expect("reclaim");
    assert_eq!(freed, Some(3 * ((256 << 10) + 4096)));
    assert_eq!(store.slab_count(), 3);
    assert!(store.get(&key(5)).await.expect("get").is_none());
    assert_eq!(store.len(), 9);
    // Removing every entry of a slab deletes it.
    for i in 0..3 {
        store.remove(&key(i)).await.expect("remove");
    }
    assert_eq!(store.slab_count(), 2);
    assert!(!tmp.path().join("slabs").join("slab-00000000.dat").exists());
    for i in 6..12 {
        assert!(same(
            store.get(&key(i)).await.expect("get").as_ref(),
            &block(i, 256 << 10)
        ));
    }
}

/// Sum of the lengths of every slab file under `dir`.
fn slab_file_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir.join("slabs"))
        .expect("list slabs")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "dat"))
        .map(|p| std::fs::metadata(&p).expect("stat").len())
        .sum()
}

fn slab_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir.join("slabs"))
        .expect("list slabs")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
}

#[tokio::test]
async fn holes_in_unsealed_slab_drop_later_entries_and_disk_matches_files() {
    // Two writes into the active slab failed after reserving their regions
    // (entries 2 and 3), later writes completed, and the process stopped
    // before the slab was sealed. A failed write leaves its region without a
    // header: zeros where a later write extended the file. The scan stops at
    // the first hole, so entry 4 is dropped with it (a later miss): past a
    // region that does not verify, entry boundaries cannot be trusted.
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("open");
    for i in 0..5 {
        store
            .put(&key(i), block(i, 100_000), &StoredMeta::NONE)
            .await
            .expect("put");
    }
    let mut holes = Vec::new();
    for i in [2, 3] {
        let (path, data_at) = store.data_location(&key(i)).expect("located");
        holes.push((path, data_at - 4096));
    }
    drop(store);
    let zeros = vec![0u8; usize::try_from(entry_len(100_000)).expect("len")];
    for (path, at) in &holes {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open slab");
        f.write_all_at(&zeros, *at)
            .expect("zero the failed write's region");
    }
    assert!(!tmp.path().join("slabs").join("slab-00000000.idx").exists());

    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("reopen");
    let mut recovered = Vec::new();
    for i in 0..5 {
        if let Some(b) = store.get(&key(i)).await.expect("get") {
            assert!(same(Some(&b), &block(i, 100_000)), "entry {i} bytes");
            recovered.push(i);
        }
    }
    assert_eq!(recovered, vec![0, 1], "entries recovered before the holes");
    assert_eq!(store.len(), 2);
    assert_eq!(
        store.disk_bytes(),
        slab_file_bytes(tmp.path()),
        "disk accounting vs slab files on disk"
    );
    assert_eq!(store.counters().corrupt_reads, 0);
}

/// The bytes of a complete slab entry for `k` holding `data`, as a slab store
/// writes it (header page, data, padding).
async fn encoded_entry(k: &BlockKey, data: &Bytes) -> Vec<u8> {
    let scratch = tempfile::tempdir().expect("tempdir");
    let store = SlabStore::open(scratch.path(), 8 * MIB as u64)
        .await
        .expect("open scratch store");
    store
        .put(k, data.clone(), &StoredMeta::NONE)
        .await
        .expect("put");
    let (path, data_at) = store.data_location(k).expect("located");
    drop(store);
    let file = std::fs::read(path).expect("read scratch slab");
    let start = usize::try_from(data_at - 4096).expect("start");
    let len = usize::try_from(entry_len(data.len() as u64)).expect("len");
    file[start..start + len].to_vec()
}

#[tokio::test]
async fn stray_header_in_a_hole_is_not_an_entry() {
    // 1. A complete entry for key 9 (header, key, data and CRC all valid)
    // sits inside entry 0's payload, and entry 0's own header is damaged. A
    // scan that looks past the damage finds key 9's entry, which was never
    // put, and would serve its bytes.
    let tmp = tempfile::tempdir().expect("tempdir");
    let fake = block(900, 4096);
    let mut payload = encoded_entry(&key(9), &fake).await;
    payload.extend_from_slice(&block(0, 100_000 - payload.len()));
    let payload = Bytes::from(payload);
    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("open");
    store
        .put(&key(0), payload.clone(), &StoredMeta::NONE)
        .await
        .expect("put 0");
    for i in 1..5 {
        store
            .put(&key(i), block(i, 100_000), &StoredMeta::NONE)
            .await
            .expect("put");
    }
    let (path, entry0_data) = store.data_location(&key(0)).expect("located");
    drop(store);
    flip_byte(&path, entry0_data - 4096);
    assert!(!tmp.path().join("slabs").join("slab-00000000.idx").exists());
    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("reopen");
    let served = store.get(&key(9)).await.expect("get 9");
    assert!(
        served.is_none(),
        "a key that was never put is served from inside another entry's payload ({} bytes, fabricated: {})",
        served.as_ref().map_or(0, Bytes::len),
        served.as_ref() == Some(&fake)
    );
    // Nothing past the damaged entry is listed: the scan stops there.
    assert_eq!(store.len(), 0);
    assert_eq!(store.disk_bytes(), slab_file_bytes(tmp.path()));
    drop(store);

    // 2. Entry 2's region holds no entry, but its second page holds a header
    // that verifies (a copy of entry 4's) with other data after it. The scan
    // stops at the hole: entries 0 and 1 are kept, nothing after it is.
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("open");
    for i in 0..5 {
        store
            .put(&key(i), block(i, 100_000), &StoredMeta::NONE)
            .await
            .expect("put");
    }
    let (path, entry2_data) = store.data_location(&key(2)).expect("located");
    let (_, entry4_data) = store.data_location(&key(4)).expect("located");
    drop(store);
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open slab");
    let mut header4 = vec![0u8; 4096];
    f.read_exact_at(&mut header4, entry4_data - 4096)
        .expect("read entry 4's header");
    let zeros = vec![0u8; usize::try_from(entry_len(100_000)).expect("len")];
    f.write_all_at(&zeros, entry2_data - 4096).expect("hole");
    f.write_all_at(&header4, entry2_data).expect("stray header");
    drop(f);

    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("reopen");
    let mut recovered = Vec::new();
    for i in 0..5 {
        if let Some(b) = store.get(&key(i)).await.expect("get") {
            assert!(same(Some(&b), &block(i, 100_000)), "entry {i} bytes");
            recovered.push(i);
        }
    }
    assert_eq!(recovered, vec![0, 1]);
    assert_eq!(store.len(), 2);
    assert_eq!(store.disk_bytes(), slab_file_bytes(tmp.path()));
}

#[tokio::test]
async fn torn_tombstone_tail_does_not_hide_later_removals() {
    // (1) An append torn while the store runs (a write that failed part way)
    // is cut off by the next append, so a removal after it is read back.
    // (2) A torn tail left when the process stopped makes the slab's
    // tombstones unverifiable: the slab is quarantined at the next open, so
    // no removed block can come back.
    for sealed in [true, false] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let del = tmp.path().join("slabs").join("slab-00000000.del");
        let tear = || {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&del)
                .expect("open tombstones");
            // A tombstone append torn after 5 of its 12 bytes.
            f.write_all(&[0xA5; 5]).expect("torn tail");
        };
        let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
            .await
            .expect("open");
        for i in 0..6 {
            store
                .put(&key(i), block(i, 100_000), &StoredMeta::NONE)
                .await
                .expect("put");
        }
        if sealed {
            store.close().await.expect("close");
        }
        store.remove(&key(1)).await.expect("remove 1");
        tear();
        store.remove(&key(4)).await.expect("remove 4");
        if !sealed {
            store.close().await.expect("close");
        }
        drop(store);
        for round in 0..2 {
            let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
                .await
                .expect("reopen");
            assert_eq!(store.counters().quarantined_slabs, 0, "sealed={sealed}");
            let mut served = Vec::new();
            for i in 0..6 {
                if let Some(b) = store.get(&key(i)).await.expect("get") {
                    assert!(same(Some(&b), &block(i, 100_000)), "key {i}");
                    served.push(i);
                }
            }
            assert_eq!(
                served,
                vec![0, 2, 3, 5],
                "sealed={sealed} round {round}: a removal after the torn tail came back"
            );
        }

        tear();
        let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
            .await
            .expect("reopen with a torn tail");
        assert_eq!(store.counters().quarantined_slabs, 1, "sealed={sealed}");
        assert_eq!(store.len(), 0, "sealed={sealed}");
        for i in 0..6 {
            assert!(
                store.get(&key(i)).await.expect("get").is_none(),
                "sealed={sealed}: key {i} served from a quarantined slab"
            );
        }
        assert!(
            slab_files(tmp.path()).is_empty(),
            "{:?}",
            slab_files(tmp.path())
        );
    }
}

fn object_block(path: &str, index: u64) -> BlockKey {
    BlockKey {
        store: "s3://bucket".to_string(),
        path: path.to_string(),
        object_size: 16 * MIB as u64,
        block_size: MIB as u64,
        index,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstone_racing_reclaim_leaves_no_sidecar() {
    // Slabs of three entries: slab 0 holds x0, y0, y1 and slab 1 holds x1,
    // x2, x3. Removing object x appends tombstones to slab 0 and then slab 1.
    // Slab 0's append waits on a named pipe; meanwhile slab 1, now empty, is
    // reclaimed. Slab 1's append then runs against a slab that is gone.
    let tmp = tempfile::tempdir().expect("tempdir");
    let len: usize = 100_000;
    let store = SlabStore::open(tmp.path(), 3 * entry_len(len as u64))
        .await
        .expect("open");
    let keys = [
        object_block("x", 0),
        object_block("y", 0),
        object_block("y", 1),
        object_block("x", 1),
        object_block("x", 2),
        object_block("x", 3),
    ];
    for (i, k) in keys.iter().enumerate() {
        store
            .put(k, block(i as u64, len), &StoredMeta::NONE)
            .await
            .expect("put");
    }
    store.close().await.expect("close");
    assert_eq!(store.slab_count(), 2);
    let slabs = tmp.path().join("slabs");
    let fifo = slabs.join("slab-00000000.del");
    let c_path = std::ffi::CString::new(fifo.to_str().expect("utf-8")).expect("path");
    // SAFETY: `c_path` is a valid NUL-terminated path that outlives the call.
    let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o644) };
    assert_eq!(rc, 0, "mkfifo: {}", std::io::Error::last_os_error());

    let remover = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let x = object_store_cache::testing::ObjectKey {
                store: Arc::from("s3://bucket"),
                path: Arc::from("x"),
                object_size: 16 * MIB as u64,
                block_size: MIB as u64,
            };
            store.remove_objects(&[x]).await
        })
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while store.len() != 2 {
        assert!(std::time::Instant::now() < deadline, "x was not forgotten");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // Slab 1 has nothing live left; slab 0 still holds y. Checked after the
    // pipe is read, so a failure does not leave the append blocked.
    let freed = reclaim_one(&*store).await;
    let slab1_gone = !slabs.join("slab-00000001.dat").exists();
    // Let slab 0's append through.
    let reader = tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut buf = Vec::new();
        std::fs::File::open(&fifo)
            .expect("open pipe")
            .read_to_end(&mut buf)
            .expect("read pipe");
        buf.len()
    });
    assert_eq!(reader.await.expect("join"), 12, "slab 0's tombstone");
    assert_eq!(freed.expect("reclaim"), Some(3 * entry_len(len as u64)));
    assert!(slab1_gone);
    let outcome = remover.await.expect("join");
    eprintln!("remove_objects: {outcome:?}");
    assert!(
        !slabs.join("slab-00000001.del").exists(),
        "tombstones of a reclaimed slab were recreated: {:?}",
        slab_files(tmp.path())
    );
    assert_eq!(
        slab_files(tmp.path()),
        vec![
            "slab-00000000.dat".to_string(),
            "slab-00000000.del".to_string(),
            "slab-00000000.idx".to_string()
        ]
    );
}

#[tokio::test]
async fn second_copy_of_a_key_stays_removed_after_restart() {
    for sealed in [true, false] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
            .await
            .expect("open");
        // Two physical copies of key 0 (a put written again), then key 1.
        store
            .put(&key(0), block(0, 100_000), &StoredMeta::NONE)
            .await
            .expect("put 0");
        store
            .put(&key(0), block(0, 100_000), &StoredMeta::NONE)
            .await
            .expect("put 0 again");
        store
            .put(&key(1), block(1, 100_000), &StoredMeta::NONE)
            .await
            .expect("put 1");
        if sealed {
            store.close().await.expect("close");
        }
        store.remove(&key(0)).await.expect("remove 0");
        assert!(store.get(&key(0)).await.expect("get").is_none());
        drop(store);
        for round in 0..2 {
            let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
                .await
                .expect("reopen");
            assert!(
                store.get(&key(0)).await.expect("get 0").is_none(),
                "sealed={sealed} round {round}: the removed key came back"
            );
            assert!(same(
                store.get(&key(1)).await.expect("get 1").as_ref(),
                &block(1, 100_000)
            ));
            assert_eq!(store.len(), 1, "sealed={sealed} round {round}");
        }
    }
}

/// How many descriptors this process holds open on files that are unlinked
/// and whose (device, inode) is in `inodes`.
fn open_unlinked(inodes: &std::collections::HashSet<(i128, i128)>) -> usize {
    let mut n = 0;
    for fd in 0..16_384 {
        let mut st = std::mem::MaybeUninit::<libc::stat>::zeroed();
        // SAFETY: `st` points to writable memory of the right type; `fstat`
        // fills it on success and fails with `EBADF` for a closed descriptor.
        if unsafe { libc::fstat(fd, st.as_mut_ptr()) } != 0 {
            continue;
        }
        // SAFETY: `fstat` returned 0, so it initialized `st`.
        let st = unsafe { st.assume_init() };
        let id = (i128::from(st.st_dev), i128::from(st.st_ino));
        if st.st_nlink == 0 && inodes.contains(&id) {
            n += 1;
        }
    }
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn reclaimed_slab_descriptor_is_not_cached() {
    use std::os::unix::fs::MetadataExt as _;
    // One entry per slab. Readers keep reading a slab's entry while it is
    // reclaimed; a reader that opens the slab file just before it is unlinked
    // must not leave that descriptor in the store's descriptor cache, where
    // it would keep the unlinked file's space allocated.
    let tmp = tempfile::tempdir().expect("tempdir");
    let len = 4096;
    let slabs = 256u64;
    {
        let store = SlabStore::open(tmp.path(), entry_len(len as u64))
            .await
            .expect("open");
        for i in 0..slabs {
            store
                .put(&key(i), block(i, len), &StoredMeta::NONE)
                .await
                .expect("put");
        }
        store.close().await.expect("close");
    }
    // Reopened, so no descriptor is cached yet.
    let store = SlabStore::open(tmp.path(), entry_len(len as u64))
        .await
        .expect("reopen");
    let inodes: std::collections::HashSet<(i128, i128)> =
        std::fs::read_dir(tmp.path().join("slabs"))
            .expect("list")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.extension().is_some_and(|e| e == "dat"))
            .map(|p| {
                let m = std::fs::metadata(&p).expect("stat");
                (i128::from(m.dev()), i128::from(m.ino()))
            })
            .collect();
    assert_eq!(inodes.len() as u64, slabs);
    for i in 0..slabs {
        let readers: Vec<_> = (0..6)
            .map(|_| {
                let store = Arc::clone(&store);
                tokio::spawn(async move {
                    for _ in 0..10_000 {
                        if store.get(&key(i)).await.expect("get").is_none() {
                            return;
                        }
                    }
                })
            })
            .collect();
        tokio::task::yield_now().await;
        // The oldest slab, `i`, is the one reclaimed.
        let freed = reclaim_one(&*store).await.expect("reclaim");
        assert_eq!(freed, Some(entry_len(len as u64)), "slab {i}");
        for r in readers {
            r.await.expect("reader");
        }
    }
    assert_eq!(store.slab_count(), 0);
    let stale = open_unlinked(&inodes);
    assert_eq!(
        stale, 0,
        "descriptors of reclaimed slab files still open (their space is never freed)"
    );
}

#[tokio::test]
async fn reopen_with_smaller_slab_size_accounts_whole_files() {
    for sealed in [true, false] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
            .await
            .expect("open");
        for i in 0..6 {
            store
                .put(&key(i), block(i, MIB), &StoredMeta::NONE)
                .await
                .expect("put");
        }
        if sealed {
            store.close().await.expect("close");
        }
        drop(store);

        // The slab size is now 2 MiB: one 1 MiB entry per new slab.
        let store = SlabStore::open(tmp.path(), 2 * MIB as u64)
            .await
            .expect("reopen");
        assert_eq!(store.len(), 6, "sealed={sealed}: entries listed");
        for i in 0..6 {
            assert!(
                same(
                    store.get(&key(i)).await.expect("get").as_ref(),
                    &block(i, MIB)
                ),
                "sealed={sealed}: entry {i}"
            );
        }
        assert_eq!(
            store.disk_bytes(),
            slab_file_bytes(tmp.path()),
            "sealed={sealed}: disk accounting vs slab files on disk"
        );
    }
}

#[tokio::test]
async fn failed_reclaim_keeps_the_slab_accounted_and_retries() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = SlabStore::open(tmp.path(), MIB as u64).await.expect("open");
    for i in 0..12 {
        store
            .put(&key(i), block(i, 256 << 10), &StoredMeta::NONE)
            .await
            .expect("put");
    }
    store.close().await.expect("close");
    assert_eq!(store.slab_count(), 4);
    let slabs = tmp.path().join("slabs");

    // Unlinking fails: the slab files stay on disk.
    set_mode(&slabs, 0o555);
    let failed = reclaim_one(&*store).await;
    set_mode(&slabs, 0o755);
    assert!(
        failed.is_err(),
        "reclaim with an unwritable directory: {failed:?}"
    );
    assert!(slabs.join("slab-00000000.dat").exists());
    assert_eq!(
        store.disk_bytes(),
        slab_file_bytes(tmp.path()),
        "a slab whose files could not be removed is still counted"
    );
    assert_eq!(store.slab_count(), 4);

    // The next reclaim retries that slab and removes its files.
    let freed = reclaim_one(&*store).await.expect("reclaim");
    assert_eq!(freed, Some(3 * ((256 << 10) + 4096)));
    assert!(!slabs.join("slab-00000000.dat").exists());
    assert!(!slabs.join("slab-00000000.idx").exists());
    assert_eq!(store.slab_count(), 3);
    assert_eq!(store.disk_bytes(), slab_file_bytes(tmp.path()));
}

#[tokio::test]
async fn failed_index_write_does_not_leak_the_reservation() {
    // One entry per slab, so the second put seals the first slab and writes
    // its index.
    let tmp = tempfile::tempdir().expect("tempdir");
    let len = 100_000;
    let store = SlabStore::open(tmp.path(), entry_len(len as u64))
        .await
        .expect("open");
    store
        .put(&key(0), block(0, len), &StoredMeta::NONE)
        .await
        .expect("put 0");

    // The index write of slab 0 fails: its temporary name is a directory.
    let blocker = tmp.path().join("slabs").join("slab-00000000.idx.tmp");
    std::fs::create_dir(&blocker).expect("block the index write");
    let second = store.put(&key(1), block(1, len), &StoredMeta::NONE).await;
    assert!(
        second.is_err(),
        "put with a failing index write: {second:?}"
    );
    std::fs::remove_dir(&blocker).expect("restore");

    store
        .put(&key(2), block(2, len), &StoredMeta::NONE)
        .await
        .expect("put 2");
    store.close().await.expect("close");

    // Every slab can be reclaimed: nothing is left reserved.
    while reclaim_one(&*store).await.expect("reclaim").is_some() {}
    assert_eq!(
        store.slab_count(),
        0,
        "slabs left: {:?}, disk_bytes {}",
        slab_files(tmp.path()),
        store.disk_bytes()
    );
    assert_eq!(store.disk_bytes(), 0);
    assert!(
        slab_files(tmp.path()).is_empty(),
        "{:?}",
        slab_files(tmp.path())
    );
}

#[tokio::test]
async fn failed_index_rename_leaves_no_temporary() {
    // One entry per slab, so each put seals the slab before it and writes
    // that slab's index. Three times, the index's final name is a directory:
    // the temporary is written and the rename fails. No temporary may stay
    // behind, uncounted, and once every slab is reclaimed nothing is left.
    let tmp = tempfile::tempdir().expect("tempdir");
    let len = 100_000;
    let store = SlabStore::open(tmp.path(), entry_len(len as u64))
        .await
        .expect("open");
    let slabs = tmp.path().join("slabs");
    store
        .put(&key(0), block(0, len), &StoredMeta::NONE)
        .await
        .expect("put 0");
    for i in 1..=3u64 {
        let blocker = slabs.join(format!("slab-{:08}.idx", i - 1));
        std::fs::create_dir(&blocker).expect("block the index rename");
        let sealing = store.put(&key(i), block(i, len), &StoredMeta::NONE).await;
        assert!(
            sealing.is_err(),
            "put {i} with a failing index: {sealing:?}"
        );
        std::fs::remove_dir(&blocker).expect("restore");
        let leftovers: Vec<String> = slab_files(tmp.path())
            .into_iter()
            .filter(|n| Path::new(n).extension().is_some_and(|e| e == "tmp"))
            .collect();
        assert!(leftovers.is_empty(), "after put {i}: {leftovers:?}");
    }
    store.close().await.expect("close");
    while reclaim_one(&*store).await.expect("reclaim").is_some() {}
    assert_eq!(store.disk_bytes(), 0);
    assert!(
        slab_files(tmp.path()).is_empty(),
        "{:?}",
        slab_files(tmp.path())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_put_does_not_leak_the_reservation() {
    use futures::FutureExt;
    let tmp = tempfile::tempdir().expect("tempdir");
    let len = MIB;
    let store = SlabStore::open(tmp.path(), entry_len(len as u64))
        .await
        .expect("open");
    store
        .put(&key(0), block(0, len), &StoredMeta::NONE)
        .await
        .expect("put 0");

    // Poll a put once and drop it while its write is running.
    let mut cancelled = false;
    for i in 1..20 {
        if store
            .put(&key(i), block(i, len), &StoredMeta::NONE)
            .now_or_never()
            .is_none()
        {
            cancelled = true;
            break;
        }
    }
    assert!(cancelled, "no put was still running after its first poll");
    store.close().await.expect("close");

    // Once the cancelled write has finished, every slab can be reclaimed.
    // Polled: the write runs on a blocking thread the test cannot join.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        while reclaim_one(&*store).await.expect("reclaim").is_some() {}
        if store.slab_count() == 0 || std::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        store.slab_count(),
        0,
        "slabs never reclaimable: {:?}, disk_bytes {}",
        slab_files(tmp.path()),
        store.disk_bytes()
    );
    assert_eq!(store.disk_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_write_racing_reclaim_leaves_no_orphan_index() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let len = 100_000;
    let store = SlabStore::open(tmp.path(), entry_len(len as u64))
        .await
        .expect("open");
    store
        .put(&key(0), block(0, len), &StoredMeta::NONE)
        .await
        .expect("put 0");

    // A named pipe at slab 0's temporary index name: the index write blocks
    // on opening it until the test reads it.
    let slabs = tmp.path().join("slabs");
    let fifo = slabs.join("slab-00000000.idx.tmp");
    let c_path = std::ffi::CString::new(fifo.to_str().expect("utf-8")).expect("path");
    // SAFETY: `c_path` is a valid NUL-terminated path that outlives the call.
    let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o644) };
    assert_eq!(rc, 0, "mkfifo: {}", std::io::Error::last_os_error());

    // The second put seals slab 0; its index write waits on the pipe.
    let writer = {
        let store = Arc::clone(&store);
        tokio::spawn(async move { store.put(&key(1), block(1, len), &StoredMeta::NONE).await })
    };
    // Reclaim slab 0 while its index write waits. Slab 0 is sealed once the
    // second put has started slab 1 (until then, slab 0 is the active slab,
    // which a reclaim would take as well, and no index would be written).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while store.slab_count() != 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "slab 0 was never sealed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // Checked after the pipe is read, so a failure does not leave the index
    // write blocked.
    let freed = reclaim_one(&*store).await;
    let slab0_gone = !slabs.join("slab-00000000.dat").exists();
    // Let the index write finish.
    let reader = tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut buf = Vec::new();
        std::fs::File::open(&fifo)
            .expect("open pipe")
            .read_to_end(&mut buf)
            .expect("read pipe");
        buf.len()
    });
    assert!(reader.await.expect("join") > 0, "the index was written");
    assert_eq!(
        freed.expect("reclaim"),
        Some(entry_len(len as u64)),
        "slab 0 reclaimed"
    );
    assert!(slab0_gone);
    let put = writer.await.expect("join");
    eprintln!("second put: {put:?}");

    assert!(
        !slabs.join("slab-00000000.idx").exists(),
        "an index without its slab: {:?}",
        slab_files(tmp.path())
    );
    // The second entry, in slab 1, is intact and served.
    assert!(
        same(
            store.get(&key(1)).await.expect("get 1").as_ref(),
            &block(1, len)
        ),
        "the second entry's bytes"
    );
    assert!(store.get(&key(0)).await.expect("get 0").is_none());
    assert_eq!(store.counters().corrupt_reads, 0);
}

#[tokio::test]
async fn slab_id_at_u32_max_does_not_overflow() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(tmp.path().join("slabs")).expect("mkdir");
    std::fs::write(tmp.path().join("slabs").join("slab-4294967295.dat"), b"").expect("plant");
    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("open with a slab numbered u32::MAX");
    // No slab number is left for a new slab: the put fails, nothing panics.
    let put = store.put(&key(1), block(1, 4096), &StoredMeta::NONE).await;
    assert!(
        matches!(&put, Err(object_store_cache::Error::NoSlabId { path }) if *path == tmp.path().join("slabs")),
        "{put:?}"
    );
    assert!(store.get(&key(1)).await.expect("get").is_none());
    assert_eq!(
        slab_files(tmp.path()),
        vec![
            "slab-4294967295.dat".to_string(),
            "slab-4294967295.idx".to_string()
        ]
    );
}

#[tokio::test]
async fn unreadable_slab_files_quarantine_only_their_slab() {
    // Three slabs of three entries. Slab 0's index cannot be read (a
    // directory is in its place) and slab 1 has no index and a `.dat` that
    // cannot be opened: each is dropped, and slab 2 opens as it was.
    let tmp = tempfile::tempdir().expect("tempdir");
    let len = 4096;
    let slab = 3 * entry_len(len as u64);
    {
        let store = SlabStore::open(tmp.path(), slab).await.expect("open");
        for i in 0..9 {
            store
                .put(&key(i), block(i, len), &StoredMeta::NONE)
                .await
                .expect("put");
        }
        store.close().await.expect("close");
    }
    let slabs = tmp.path().join("slabs");
    std::fs::remove_file(slabs.join("slab-00000000.idx")).expect("rm idx 0");
    std::fs::create_dir(slabs.join("slab-00000000.idx")).expect("idx 0 dir");
    std::fs::remove_file(slabs.join("slab-00000001.idx")).expect("rm idx 1");
    set_mode(&slabs.join("slab-00000001.dat"), 0o000);

    let store = SlabStore::open(tmp.path(), slab)
        .await
        .expect("open with two unreadable slabs");
    assert_eq!(store.counters().quarantined_slabs, 2);
    assert_eq!(store.len(), 3);
    for i in 0..6 {
        assert!(
            store.get(&key(i)).await.expect("get").is_none(),
            "block {i} of a quarantined slab was served"
        );
    }
    for i in 6..9 {
        assert!(
            same(
                store.get(&key(i)).await.expect("get").as_ref(),
                &block(i, len)
            ),
            "block {i}"
        );
    }
    assert!(!slabs.join("slab-00000000.dat").exists());
    assert!(!slabs.join("slab-00000001.dat").exists());
    // A new slab takes a new number.
    store
        .put(&key(20), block(20, len), &StoredMeta::NONE)
        .await
        .expect("put after quarantine");
    assert!(
        slabs.join("slab-00000003.dat").exists(),
        "{:?}",
        slab_files(tmp.path())
    );
}

#[tokio::test]
async fn untombstonable_superseded_copy_quarantines_only_its_slab() {
    // Two slabs list the same block (a write repeated before the older copy
    // was tombstoned). Open tombstones the older copy; when that append
    // fails, the older copy's slab is dropped instead of failing the open,
    // and the newer copy is served.
    let tmp = tempfile::tempdir().expect("tempdir");
    let v = block(1, 4096);
    {
        let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
            .await
            .expect("open");
        store
            .put(&key(1), v.clone(), &StoredMeta::NONE)
            .await
            .expect("put");
        store.close().await.expect("close");
    }
    let slabs = tmp.path().join("slabs");
    std::fs::copy(
        slabs.join("slab-00000000.dat"),
        slabs.join("slab-00000001.dat"),
    )
    .expect("copy dat");
    std::fs::copy(
        slabs.join("slab-00000000.idx"),
        slabs.join("slab-00000001.idx"),
    )
    .expect("copy idx");
    // Readable (no tombstones), not appendable.
    std::fs::write(slabs.join("slab-00000000.del"), b"").expect("del");
    set_mode(&slabs.join("slab-00000000.del"), 0o400);

    let store = SlabStore::open(tmp.path(), 8 * MIB as u64)
        .await
        .expect("open with an untombstonable older copy");
    assert_eq!(store.counters().quarantined_slabs, 1);
    assert_eq!(store.len(), 1);
    assert!(same(store.get(&key(1)).await.expect("get").as_ref(), &v));
    assert_eq!(
        slab_files(tmp.path()),
        vec![
            "slab-00000001.dat".to_string(),
            "slab-00000001.idx".to_string()
        ],
        "the older copy's slab is gone, its files with it"
    );
}

/// A newer copy of a block supersedes the older one, whose tombstone cannot be
/// written (a directory sits where its slab's tombstone file goes) while
/// another block keeps that slab live. Once the newer copy is removed, a
/// reopened store does not list the older one.
#[tokio::test]
async fn failed_superseded_copy_tombstone_never_brings_the_copy_back() {
    const LEN: usize = 10_000;
    // Two entries per slab: the first copy and another block fill slab 0, and
    // the second copy goes to slab 1.
    const SLAB: u64 = 2 * (4096 + 12_288);
    let tmp = tempfile::tempdir().expect("tempdir");
    let store = SlabStore::open(tmp.path(), SLAB)
        .await
        .expect("open slab store");
    let (k, other) = (key(1), key(2));
    store
        .put(&k, block(1, LEN), &StoredMeta::NONE)
        .await
        .expect("put the first copy");
    store
        .put(&other, block(3, LEN), &StoredMeta::NONE)
        .await
        .expect("put another block");
    let del = tmp.path().join("slabs").join("slab-00000000.del");
    std::fs::create_dir(&del).expect("block slab 0's tombstone file");
    let superseded = store.put(&k, block(2, LEN), &StoredMeta::NONE).await;
    store.remove(&k).await.expect("remove the newer copy");
    assert_eq!(store.get(&k).await.expect("get"), None);
    drop(store);
    std::fs::remove_dir(&del).expect("unblock");

    let reopened = SlabStore::open(tmp.path(), SLAB).await.expect("reopen");
    assert_eq!(
        reopened.get(&k).await.expect("get").map(|b| b.len()),
        None,
        "a copy came back"
    );
    // Slab 0 was dropped whole, the other block with it.
    assert_eq!(reopened.len(), 0);
    assert!(
        matches!(superseded, Ok(())),
        "the older copy's slab was dropped, so the put is durable: {superseded:?}"
    );
}
