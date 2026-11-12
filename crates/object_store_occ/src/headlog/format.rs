// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use prost::Message;

use super::{Error, Result};

/// Protocol bounds; all writers of a log must use the same values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeadLogConfig {
    pub head_budget_bytes: usize,
    pub batch_budget_bytes: usize,
    pub batch_max_records: usize,
    /// Maximum ORIGINAL encoded record length, including its envelope.
    pub max_record_bytes: usize,
    /// Most batch ends the head lists before a checkpoint must trim the index.
    pub max_listed_batches: usize,
    /// A record whose encoding exceeds this stores its payload as an object and
    /// keeps only a reference in the window. At most `max_record_bytes`; equal
    /// to it, records are never spilled and a larger one is refused.
    pub spill_above_bytes: usize,
}

impl Default for HeadLogConfig {
    fn default() -> Self {
        Self {
            head_budget_bytes: 128 * 1024,
            batch_budget_bytes: 64 * 1024,
            batch_max_records: 64,
            max_record_bytes: 32 * 1024,
            max_listed_batches: 1024,
            spill_above_bytes: 16 * 1024,
        }
    }
}

const MAX_VARINT_BYTES: usize = 10;
const INCARNATION_BYTES: usize = 16;

fn varint_len(value: usize) -> usize {
    prost::encoding::encoded_len_varint(value as u64)
}

impl HeadLogConfig {
    /// H >= B + R + M: the head must hold a full batch and the record that
    /// makes it final beside everything else it carries.
    pub(super) fn validate(self) -> Result<()> {
        let invalid = |reason: String| Err(Error::Invalid { reason });
        if self.max_record_bytes == 0
            || self.batch_max_records == 0
            || self.max_record_bytes > self.batch_budget_bytes
            || self.spill_above_bytes > self.max_record_bytes
        {
            return invalid("infeasible head/batch/record budgets".into());
        }
        let required = self
            .batch_budget_bytes
            .checked_add(self.max_record_bytes)
            .and_then(|sum| sum.checked_add(self.head_overhead_bytes()?));
        if required.is_none_or(|required| self.head_budget_bytes < required) {
            return invalid(format!(
                "head budget {} is below B + R + M = {} + {} + {}; raise head_budget_bytes or lower the batch or record budget",
                self.head_budget_bytes,
                self.batch_budget_bytes,
                self.max_record_bytes,
                self.head_overhead_bytes()
                    .map_or_else(|| "overflow".into(), |m| m.to_string()),
            ));
        }
        Ok(())
    }

    /// M: the largest encoded head content outside the window's record bytes.
    /// Fixed fields at their widest, one window envelope for each of the K + 1
    /// records a full batch and its successor can hold, and the batch index at
    /// `max_listed_batches` entries.
    #[must_use]
    pub fn head_overhead_bytes(self) -> Option<usize> {
        let field = |payload: usize| 1 + payload;
        let fixed = field(5) // format: uint32
            + field(varint_len(INCARNATION_BYTES) + INCARNATION_BYTES)
            + 3 * field(MAX_VARINT_BYTES); // commit, floor, lease
        // WindowRecord { encoded } inside the head's repeated field 5.
        let inner = 1 + varint_len(self.max_record_bytes) + self.max_record_bytes;
        let envelope = 1 + varint_len(inner) + (inner - self.max_record_bytes);
        let window = self
            .batch_max_records
            .checked_add(1)?
            .checked_mul(envelope)?;
        // Packed repeated uint64: tag, length, then at most 10 bytes per entry.
        let entries = self.max_listed_batches.checked_mul(MAX_VARINT_BYTES)?;
        let index = 1 + varint_len(entries) + entries;
        fixed.checked_add(window)?.checked_add(index)
    }
}

/// The mutable commit point. Numbered protobuf fields are a persistent contract.
#[derive(Clone, PartialEq, Message)]
pub struct Head {
    #[prost(uint32, tag = "1")]
    pub format: u32,
    #[prost(string, repeated, tag = "2")]
    pub features: Vec<String>,
    #[prost(bytes = "vec", tag = "3")]
    pub incarnation: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub commit: u64,
    #[prost(message, repeated, tag = "5")]
    pub window: Vec<WindowRecord>,
    #[prost(uint64, repeated, tag = "6")]
    pub batches: Vec<u64>,
    #[prost(uint64, tag = "7")]
    pub floor: u64,
    #[prost(uint64, tag = "8")]
    pub lease: u64,
}

/// Original record bytes. Sealers copy these without re-encoding the record.
#[derive(Clone, PartialEq, Message)]
pub struct WindowRecord {
    #[prost(bytes = "vec", tag = "1")]
    pub encoded: Vec<u8>,
}

/// An accepted record, with an opaque caller payload.
#[derive(Clone, PartialEq, Message)]
pub struct Record {
    #[prost(bytes = "vec", tag = "1")]
    pub attempt_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub generation: u64,
    #[prost(bytes = "bytes", tag = "3")]
    pub payload: bytes::Bytes,
    /// Set when the payload is stored as its own object; `payload` is then empty.
    #[prost(message, optional, tag = "4")]
    pub spilled: Option<Spilled>,
}

/// A payload stored at `objects/<generation>/records/<blake3 hex>`.
#[derive(Clone, PartialEq, Message)]
pub struct Spilled {
    #[prost(bytes = "vec", tag = "1")]
    pub digest: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub len: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct Batch {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub records: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct Checkpoint {
    #[prost(bytes = "bytes", tag = "1")]
    pub root: bytes::Bytes,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub attempts: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "3")]
    pub incarnation: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub commit: u64,
    /// For each of `attempts`, the head's store time (Unix milliseconds) at the
    /// first checkpoint that listed it: no earlier than its acceptance.
    #[prost(uint64, repeated, tag = "5")]
    pub seen_ms: Vec<u64>,
}

/// A group commit payload containing the original caller payloads in queue order.
#[derive(Clone, PartialEq, Message)]
pub struct Group {
    #[prost(bytes = "bytes", repeated, tag = "1")]
    pub payloads: Vec<bytes::Bytes>,
}

impl Head {
    pub(super) fn window_start(&self) -> u64 {
        // validate() ensures the subtraction cannot underflow.
        self.commit - self.window.len() as u64 + 1
    }

    pub(super) fn validate(&self) -> Result<()> {
        if self.format != 1 || !self.features.is_empty() {
            return Err(Error::Invalid {
                reason: "unsupported head format or features".into(),
            });
        }
        if self.incarnation.len() != 16
            || self.floor > self.commit
            || self.window.len() as u64 > self.commit - self.floor
            || self.commit == u64::MAX
        {
            return Err(Error::Invalid {
                reason: "invalid head bounds or incarnation".into(),
            });
        }
        let mut end = self.floor;
        for &next in &self.batches {
            if next <= end || next >= self.window_start() {
                return Err(Error::Invalid {
                    reason: "invalid batch index".into(),
                });
            }
            end = next;
        }
        if end != self.window_start() - 1 {
            return Err(Error::Invalid {
                reason: "gap between batches and window".into(),
            });
        }
        for entry in &self.window {
            let record = decode_record(&entry.encoded)?;
            if record.generation > self.lease {
                return Err(Error::Invalid {
                    reason: "record newer than head lease".into(),
                });
            }
        }
        Ok(())
    }
}

pub(super) fn decode_record(encoded: &[u8]) -> Result<Record> {
    let record = Record::decode(encoded)?;
    if record.attempt_id.len() != 16 {
        return Err(Error::Invalid {
            reason: "invalid attempt id".into(),
        });
    }
    Ok(record)
}
