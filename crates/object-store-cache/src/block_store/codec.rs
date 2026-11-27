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

//! The slab entry format: a 4 KiB header page followed by the block's
//! bytes, so the data starts page-aligned.
//!
//! ```text
//! 0..4    magic "OSCB"
//! 4..6    version (2)
//! 6..8    metadata length
//! 8..12   key length
//! 12..16  data length
//! 16..20  CRC32C of the data
//! 20..24  CRC32C of bytes 0..20, the key and the metadata
//! 24..    key (BlockKey::encode), then the object's metadata
//!         (StoredMeta::encode), zero padding to 4096
//! 4096..  data
//! ```
//!
//! The metadata lets a rescan (the slab active at a crash has no index)
//! restore an object's e-tag and modification time. Metadata that does not
//! fit beside the key is left out.

use snafu::ensure;

use crate::Result;
use crate::error::{BlockTooLargeSnafu, KeyTooLongSnafu};

/// CRC32C (Castagnoli, CRC-32/ISCSI) of `data`, the checksum every on-disk
/// header, entry and slab index carries.
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    crc_fast::crc32_iscsi(data)
}

/// CRC32C of `a` followed by `b`, without concatenating them.
fn crc32c_concat(a: &[u8], b: &[u8]) -> u32 {
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    digest.update(a);
    digest.update(b);
    // A CRC-32 result occupies the low 32 bits of the u64.
    let [b0, b1, b2, b3, ..] = digest.finalize().to_le_bytes();
    u32::from_le_bytes([b0, b1, b2, b3])
}

pub(crate) const HEADER_LEN: usize = 4096;
const MAGIC: [u8; 4] = *b"OSCB";
const VERSION: u16 = 2;
const FIXED: usize = 24;
pub(crate) const MAX_KEY: usize = HEADER_LEN - FIXED;

/// Round `n` up to a multiple of the 4 KiB page.
pub(crate) fn page_align(n: u64) -> u64 {
    n.div_ceil(HEADER_LEN as u64) * HEADER_LEN as u64
}

/// Bytes one entry of `data_len` bytes takes in a slab: the header page and
/// the data padded to whole pages.
#[must_use]
pub fn entry_len(data_len: u64) -> u64 {
    HEADER_LEN as u64 + page_align(data_len)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    pub key_len: usize,
    pub meta_len: usize,
    pub data_len: usize,
    pub data_crc: u32,
}

/// Checks that a key of `key_len` bytes and a block of `data_len` bytes fit
/// one entry, so [`encode_header`] cannot fail for them.
pub(crate) fn check_entry(key_len: usize, data_len: usize) -> Result<()> {
    ensure!(
        key_len <= MAX_KEY,
        KeyTooLongSnafu {
            len: key_len,
            max: MAX_KEY
        }
    );
    ensure!(
        u32::try_from(data_len).is_ok(),
        BlockTooLargeSnafu {
            len: data_len,
            max: u32::MAX as usize,
        }
    );
    Ok(())
}

/// The header page for `key`, the object metadata `meta` (left out when it
/// does not fit beside the key) and `data`.
pub(crate) fn encode_header(key: &[u8], meta: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    check_entry(key.len(), data.len())?;
    let data_len = u32::try_from(data.len()).unwrap_or(u32::MAX);
    let key_len = u32::try_from(key.len()).unwrap_or(u32::MAX);
    let meta = if key.len() + meta.len() <= MAX_KEY {
        meta
    } else {
        &[]
    };
    let meta_len = u16::try_from(meta.len()).unwrap_or(0);
    let meta = &meta[..usize::from(meta_len)];
    let mut h = vec![0u8; HEADER_LEN];
    h[0..4].copy_from_slice(&MAGIC);
    h[4..6].copy_from_slice(&VERSION.to_le_bytes());
    h[6..8].copy_from_slice(&meta_len.to_le_bytes());
    h[8..12].copy_from_slice(&key_len.to_le_bytes());
    h[12..16].copy_from_slice(&data_len.to_le_bytes());
    h[16..20].copy_from_slice(&crc32c(data).to_le_bytes());
    h[FIXED..FIXED + key.len()].copy_from_slice(key);
    h[FIXED + key.len()..FIXED + key.len() + meta.len()].copy_from_slice(meta);
    let header_crc = crc32c_concat(&h[0..20], &h[FIXED..FIXED + key.len() + meta.len()]);
    h[20..24].copy_from_slice(&header_crc.to_le_bytes());
    Ok(h)
}

/// The whole entry in one buffer: the header page, the data, and zero padding
/// up to the next 4 KiB boundary, so the entry is written with a single
/// positional write covering whole pages.
pub(crate) fn encode_entry(key: &[u8], meta: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut entry = encode_header(key, meta, data)?;
    let total = usize::try_from(entry_len(data.len() as u64)).unwrap_or(usize::MAX);
    entry.reserve_exact(total - HEADER_LEN);
    entry.extend_from_slice(data);
    entry.resize(total, 0);
    Ok(entry)
}

fn u32_at(buf: &[u8], at: usize) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&buf[at..at + 4]);
    u32::from_le_bytes(b)
}

/// Parses and checks a header page (magic, version, header CRC). Says nothing
/// about the data, which [`verify_entry`] checks.
pub(crate) fn decode_header(page: &[u8]) -> Option<Header> {
    if page.len() < HEADER_LEN || page[0..4] != MAGIC {
        return None;
    }
    let version = u16::from_le_bytes([page[4], page[5]]);
    let meta_len = usize::from(u16::from_le_bytes([page[6], page[7]]));
    let key_len = u32_at(page, 8) as usize;
    if version != VERSION || key_len + meta_len > MAX_KEY {
        return None;
    }
    let crc = crc32c_concat(&page[0..20], &page[FIXED..FIXED + key_len + meta_len]);
    if crc != u32_at(page, 20) {
        return None;
    }
    Some(Header {
        key_len,
        meta_len,
        data_len: u32_at(page, 12) as usize,
        data_crc: u32_at(page, 16),
    })
}

/// The key bytes stored in a header page that [`decode_header`] accepted.
pub(crate) fn header_key(page: &[u8], header: &Header) -> Vec<u8> {
    page[FIXED..FIXED + header.key_len].to_vec()
}

/// The metadata bytes stored in a header page that [`decode_header`]
/// accepted (empty when none were stored).
pub(crate) fn header_meta<'a>(page: &'a [u8], header: &Header) -> &'a [u8] {
    &page[FIXED + header.key_len..FIXED + header.key_len + header.meta_len]
}

/// True when `entry` (the header page followed by exactly the data) holds
/// `key` and data whose length and CRC32C match its header.
pub(crate) fn verify_entry(entry: &[u8], key: &[u8]) -> bool {
    let Some(h) = decode_header(entry) else {
        return false;
    };
    if h.key_len != key.len()
        || entry[FIXED..FIXED + h.key_len] != *key
        || entry.len() != HEADER_LEN + h.data_len
    {
        return false;
    }
    crc32c(&entry[HEADER_LEN..]) == h.data_crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksums_are_crc32c() {
        // The CRC-32/ISCSI check value from the CRC catalogue.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c_concat(b"1234", b"56789"), 0xE306_9283);
        assert_eq!(crc32c_concat(b"", b""), 0);
        // RFC 3720 (iSCSI) B.4 test vector: 32 bytes of zeros.
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
    }

    #[test]
    fn round_trip_and_single_byte_flips_are_detected() {
        use crate::block_store::StoredMeta;
        let key = b"store/path/1/2/3".to_vec();
        let meta = StoredMeta {
            e_tag: Some("\"0a1b2c\"".to_string()),
            last_modified: chrono::DateTime::from_timestamp(1_761_955_200, 123_456_789),
        };
        let meta_bytes = meta.encode();
        let data: Vec<u8> = (0..10_000u32).map(|i| (i * 31 % 251) as u8).collect();
        let mut entry = encode_header(&key, &meta_bytes, &data).expect("header");
        let h = decode_header(&entry).expect("decode");
        assert_eq!(
            StoredMeta::decode(header_meta(&entry, &h)),
            Some((meta, meta_bytes.len()))
        );
        entry.extend_from_slice(&data);
        assert!(verify_entry(&entry, &key));
        assert!(!verify_entry(&entry, b"store/path/1/2/4"));
        // Every byte of the meaningful header (metadata included) and a
        // sample of data bytes.
        let positions: Vec<usize> = (0..FIXED + key.len() + meta_bytes.len())
            .chain((HEADER_LEN..entry.len()).step_by(97))
            .collect();
        for at in positions {
            let mut bad = entry.clone();
            bad[at] ^= 0x01;
            assert!(!verify_entry(&bad, &key), "flip at {at} not detected");
        }
        // Truncation.
        assert!(!verify_entry(&entry[..entry.len() - 1], &key));
        // Metadata that does not fit beside the key is left out.
        let long = vec![b'k'; MAX_KEY - 4];
        let h = decode_header(&encode_header(&long, &meta_bytes, &data).expect("header"))
            .expect("decode");
        assert_eq!((h.key_len, h.meta_len), (MAX_KEY - 4, 0));
    }

    #[test]
    fn metadata_with_an_unstorable_e_tag_reads_back_as_unknown() {
        use crate::block_store::StoredMeta;
        // An e-tag too long to store drops the modification time with it, so
        // the metadata reads back as unknown (the store is asked), never as
        // a known time with no e-tag.
        let meta = StoredMeta {
            e_tag: Some("e".repeat(70_000)),
            last_modified: chrono::DateTime::from_timestamp(1_761_955_200, 0),
        };
        let bytes = meta.encode();
        assert_eq!(StoredMeta::decode(&bytes), Some((StoredMeta::NONE, 1)));
        let fits = StoredMeta {
            e_tag: Some("e".repeat(usize::from(u16::MAX))),
            ..meta
        };
        let bytes = fits.encode();
        assert_eq!(StoredMeta::decode(&bytes), Some((fits, bytes.len())));
    }
}
