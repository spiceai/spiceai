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
}

impl Default for HeadLogConfig {
    fn default() -> Self {
        Self {
            head_budget_bytes: 128 * 1024,
            batch_budget_bytes: 64 * 1024,
            batch_max_records: 64,
            max_record_bytes: 64 * 1024,
        }
    }
}

impl HeadLogConfig {
    pub(super) fn validate(self) -> Result<()> {
        if self.max_record_bytes == 0
            || self.batch_max_records == 0
            || self.max_record_bytes > self.batch_budget_bytes
            || self
                .batch_budget_bytes
                .checked_add(self.max_record_bytes)
                .is_none_or(|sum| self.head_budget_bytes < sum)
        {
            return Err(Error::Invalid {
                reason: "infeasible head/batch/record budgets".into(),
            });
        }
        Ok(())
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
