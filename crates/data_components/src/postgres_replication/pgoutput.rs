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

//! Decoder for the `pgoutput` logical replication protocol (Postgres 10+).
//!
//! We implement the binary wire format directly since `pgwire-replication`
//! only delivers raw `XLogData` bytes — it doesn't interpret pgoutput itself.
//!
//! Reference: <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use snafu::ensure;

use super::{PgOutputDecodeSnafu, Result};

/// Numeric relation id assigned by Postgres; also acts as the index into the
/// decoder's relation cache.
pub type RelationId = u32;

/// The pgoutput message-type tag (`data[0]`), without decoding the body. Lets a
/// multiplexed reader (the shared replication pump) branch on message kind
/// before deciding whether to fully decode or defer. See the message-format
/// reference in the module docs.
#[must_use]
pub fn message_type(data: &[u8]) -> Option<u8> {
    data.first().copied()
}

/// The relation id carried by an Insert/Update/Delete/Relation message, read
/// from `data[1..5]` (big-endian) without decoding the tuple — so the pump can
/// route a change to its dataset and buffer the raw bytes for deferred decode.
///
/// Returns `None` if `data` is too short. Only valid for those message types:
/// Truncate (`[nrel][flags][relids…]`), Begin, and Commit place other fields at
/// that offset, so callers MUST check [`message_type`] first.
#[must_use]
pub fn relation_id(data: &[u8]) -> Option<RelationId> {
    let bytes: [u8; 4] = data.get(1..5)?.try_into().ok()?;
    Some(u32::from_be_bytes(bytes))
}

/// A decoded pgoutput message, still in its "per-transaction" form.
#[derive(Debug, Clone)]
pub enum DecodedMessage {
    Begin {
        final_lsn: u64,
        commit_ts: i64,
        xid: u32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
        commit_ts: i64,
    },
    Relation(Relation),
    Insert {
        relation_id: RelationId,
        tuple: TupleData,
    },
    Update {
        relation_id: RelationId,
        old: Option<TupleData>,
        new: TupleData,
    },
    Delete {
        relation_id: RelationId,
        old: TupleData,
    },
    Truncate {
        relation_ids: Vec<RelationId>,
    },
    /// Ignored types (Type, Origin, Message, `StreamStart`, etc.) still get decoded
    /// to a length so we can skip them safely.
    Other,
}

/// Description of a relation sent by Postgres once per (slot, relation) and
/// re-sent if the schema changes.
#[derive(Debug, Clone)]
pub struct Relation {
    pub relation_id: RelationId,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub is_key: bool,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

/// A single row's column values. `None` means NULL, `Some(Value::Unchanged)`
/// means the column was unchanged TOAST.
#[derive(Debug, Clone)]
pub struct TupleData {
    pub columns: Vec<Option<Value>>,
}

/// A single column value, carried as a zero-copy [`Bytes`] slice of the
/// originating `XLogData` frame.
///
/// The decoder does not copy or validate value payloads: text values keep
/// their raw (unvalidated) bytes and binary values keep their `send`-format
/// bytes. Interpretation (UTF-8 validation, integer/temporal/numeric parsing,
/// binary `FromSql` decoding) happens once, downstream, when a value is
/// appended into its typed Arrow builder. Because `Bytes` is refcounted, a
/// whole transaction's worth of buffered tuples holds only slices of the
/// underlying frame buffers alive — no per-column heap allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Text-format payload (pgoutput tuple tag `t`). Raw, not yet UTF-8
    /// validated.
    Text(Bytes),
    /// Binary-format payload (pgoutput tuple tag `b`) — the type's `send`
    /// wire form. Also used for `bytea` under the text protocol.
    Binary(Bytes),
    /// TOAST column that was not changed in the UPDATE (tuple tag `u`).
    Unchanged,
}

/// Stateful decoder that caches `Relation` messages across calls.
///
/// Cached relations are refcounted so a consumer can hold the exact generation
/// its raw tuple bytes were decoded against without a deep copy, and compare
/// generations by pointer. See [`Self::relation`].
#[derive(Default)]
pub struct Decoder {
    relations: HashMap<RelationId, Arc<Relation>>,
}

impl Decoder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a previously-seen relation by id.
    ///
    /// Returns the refcounted generation, so a caller buffering raw tuple bytes
    /// pays a refcount bump rather than deep-copying the namespace, table name,
    /// and per-column names. Two buffers built from the same generation share
    /// one pointer, which is what lets them be merged (`Arc::ptr_eq`) without
    /// re-comparing the column layout.
    #[must_use]
    pub fn relation(&self, id: RelationId) -> Option<&Arc<Relation>> {
        self.relations.get(&id)
    }

    /// Iterate over cached relations (insertion order not preserved).
    pub fn relation_iter(&self) -> impl Iterator<Item = &Relation> {
        self.relations.values().map(AsRef::as_ref)
    }

    /// Rewrite the cached relation's key flags to the dataset-declared primary
    /// keys after the original relation message has been validated against the
    /// source replica identity. This keeps the per-change hot path borrowed and
    /// avoids cloning the whole relation for every row.
    pub fn apply_declared_primary_keys(&mut self, id: RelationId, declared_pks: &[String]) {
        if declared_pks.is_empty() {
            return;
        }

        if let Some(rel) = self.relations.get_mut(&id) {
            // `make_mut` copies only while an already-published buffer still
            // holds this generation, which is exactly when it must: that buffer's
            // rows were decoded against the pre-rewrite key flags and must keep
            // them, while the cache moves on to the rewritten generation.
            for col in &mut Arc::make_mut(rel).columns {
                col.is_key = declared_pks.iter().any(|pk| pk == &col.name);
            }
        }
    }

    /// Decode a single pgoutput message. If it's a `Relation`, the decoder
    /// caches it internally so later Insert/Update/Delete messages can refer
    /// to it.
    ///
    /// Takes the `XLogData` payload as an owned [`Bytes`]; tuple values are
    /// peeled out as zero-copy sub-slices of it (see [`Value`]).
    pub fn decode(&mut self, mut buf: Bytes) -> Result<DecodedMessage> {
        ensure!(
            buf.remaining() >= 1,
            PgOutputDecodeSnafu {
                message: "empty message".to_string()
            }
        );
        let msg_type = buf.get_u8();
        match msg_type {
            b'B' => decode_begin(&mut buf),
            b'C' => decode_commit(&mut buf),
            b'R' => {
                let rel = decode_relation(&mut buf)?;
                self.relations
                    .insert(rel.relation_id, Arc::new(rel.clone()));
                Ok(DecodedMessage::Relation(rel))
            }
            b'I' => decode_insert(&mut buf),
            b'U' => decode_update(&mut buf),
            b'D' => decode_delete(&mut buf),
            b'T' => decode_truncate(&mut buf),
            // Type / Origin / Message / Stream* — safe to skip for our use case.
            b'Y' | b'O' | b'M' | b'S' | b'E' | b'r' | b'l' | b'w' | b'c' | b'a' | b'p' => {
                Ok(DecodedMessage::Other)
            }
            other => PgOutputDecodeSnafu {
                message: format!("unknown pgoutput message type: {}", other as char),
            }
            .fail(),
        }
    }
}

fn decode_begin(buf: &mut Bytes) -> Result<DecodedMessage> {
    ensure!(
        buf.remaining() >= 8 + 8 + 4,
        PgOutputDecodeSnafu {
            message: "short Begin".to_string()
        }
    );
    let final_lsn = buf.get_u64();
    let commit_ts = buf.get_i64();
    let xid = buf.get_u32();
    Ok(DecodedMessage::Begin {
        final_lsn,
        commit_ts,
        xid,
    })
}

fn decode_commit(buf: &mut Bytes) -> Result<DecodedMessage> {
    ensure!(
        buf.remaining() >= 1 + 8 + 8 + 8,
        PgOutputDecodeSnafu {
            message: "short Commit".to_string()
        }
    );
    let _flags = buf.get_u8();
    let commit_lsn = buf.get_u64();
    let end_lsn = buf.get_u64();
    let commit_ts = buf.get_i64();
    Ok(DecodedMessage::Commit {
        commit_lsn,
        end_lsn,
        commit_ts,
    })
}

fn decode_relation(buf: &mut Bytes) -> Result<Relation> {
    ensure!(
        buf.remaining() >= 4,
        PgOutputDecodeSnafu {
            message: "short Relation header".to_string()
        }
    );
    let relation_id = buf.get_u32();
    let namespace = read_cstring(buf)?;
    let name = read_cstring(buf)?;
    ensure!(
        buf.remaining() > 2,
        PgOutputDecodeSnafu {
            message: "short Relation body".to_string()
        }
    );
    let replica_identity = buf.get_u8();
    let ncols = buf.get_u16();
    let mut columns = Vec::with_capacity(ncols as usize);
    for _ in 0..ncols {
        ensure!(
            buf.remaining() >= 1,
            PgOutputDecodeSnafu {
                message: "short Column flags".to_string()
            }
        );
        let flags = buf.get_u8();
        let col_name = read_cstring(buf)?;
        ensure!(
            buf.remaining() >= 4 + 4,
            PgOutputDecodeSnafu {
                message: "short Column type".to_string()
            }
        );
        let type_oid = buf.get_u32();
        let type_modifier = buf.get_i32();
        columns.push(Column {
            is_key: (flags & 0x01) != 0,
            name: col_name,
            type_oid,
            type_modifier,
        });
    }
    Ok(Relation {
        relation_id,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn decode_insert(buf: &mut Bytes) -> Result<DecodedMessage> {
    ensure!(
        buf.remaining() > 4,
        PgOutputDecodeSnafu {
            message: "short Insert".to_string()
        }
    );
    let relation_id = buf.get_u32();
    let tag = buf.get_u8();
    ensure!(
        tag == b'N',
        PgOutputDecodeSnafu {
            message: format!("Insert expected tag 'N', got {}", tag as char)
        }
    );
    let tuple = read_tuple(buf)?;
    Ok(DecodedMessage::Insert { relation_id, tuple })
}

fn decode_update(buf: &mut Bytes) -> Result<DecodedMessage> {
    ensure!(
        buf.remaining() > 4,
        PgOutputDecodeSnafu {
            message: "short Update".to_string()
        }
    );
    let relation_id = buf.get_u32();
    let mut old: Option<TupleData> = None;
    loop {
        ensure!(
            buf.remaining() >= 1,
            PgOutputDecodeSnafu {
                message: "short Update tag".to_string()
            }
        );
        let tag = buf.get_u8();
        match tag {
            b'K' | b'O' => {
                old = Some(read_tuple(buf)?);
            }
            b'N' => {
                let new = read_tuple(buf)?;
                return Ok(DecodedMessage::Update {
                    relation_id,
                    old,
                    new,
                });
            }
            other => {
                return PgOutputDecodeSnafu {
                    message: format!("Update: unknown tag '{}'", other as char),
                }
                .fail();
            }
        }
    }
}

fn decode_delete(buf: &mut Bytes) -> Result<DecodedMessage> {
    ensure!(
        buf.remaining() > 4,
        PgOutputDecodeSnafu {
            message: "short Delete".to_string()
        }
    );
    let relation_id = buf.get_u32();
    let tag = buf.get_u8();
    ensure!(
        tag == b'K' || tag == b'O',
        PgOutputDecodeSnafu {
            message: format!("Delete: expected 'K' or 'O', got '{}'", tag as char)
        }
    );
    let old = read_tuple(buf)?;
    Ok(DecodedMessage::Delete { relation_id, old })
}

fn decode_truncate(buf: &mut Bytes) -> Result<DecodedMessage> {
    ensure!(
        buf.remaining() > 4,
        PgOutputDecodeSnafu {
            message: "short Truncate".to_string()
        }
    );
    let nrel = buf.get_u32();
    let _flags = buf.get_u8();
    // Cap the pre-allocation to what the remaining buffer could actually hold
    // (4 bytes per relation id), so a malicious or corrupt `nrel` cannot trigger
    // a multi-gigabyte allocation before the per-element bounds check runs.
    let mut relation_ids = Vec::with_capacity((nrel as usize).min(buf.remaining() / 4));
    for _ in 0..nrel {
        ensure!(
            buf.remaining() >= 4,
            PgOutputDecodeSnafu {
                message: "short Truncate relation list".to_string()
            }
        );
        relation_ids.push(buf.get_u32());
    }
    Ok(DecodedMessage::Truncate { relation_ids })
}

fn read_tuple(buf: &mut Bytes) -> Result<TupleData> {
    ensure!(
        buf.remaining() >= 2,
        PgOutputDecodeSnafu {
            message: "short Tuple header".to_string()
        }
    );
    let n = buf.get_u16();
    let mut columns = Vec::with_capacity(n as usize);
    for _ in 0..n {
        ensure!(
            buf.remaining() >= 1,
            PgOutputDecodeSnafu {
                message: "short Tuple column tag".to_string()
            }
        );
        let tag = buf.get_u8();
        match tag {
            b'n' => columns.push(None),
            b'u' => columns.push(Some(Value::Unchanged)),
            // `t` (text) and `b` (binary) differ only in how the downstream
            // Arrow builder interprets the payload. Both peel a length-prefixed
            // slice off `buf` with zero copy: `<Bytes as Buf>::copy_to_bytes`
            // is a refcount bump + range advance, no allocation or UTF-8 check.
            b't' | b'b' => {
                ensure!(
                    buf.remaining() >= 4,
                    PgOutputDecodeSnafu {
                        message: "short Tuple value length".to_string()
                    }
                );
                let len = buf.get_u32() as usize;
                ensure!(
                    buf.remaining() >= len,
                    PgOutputDecodeSnafu {
                        message: "short Tuple value body".to_string()
                    }
                );
                let bytes = buf.copy_to_bytes(len);
                columns.push(Some(if tag == b't' {
                    Value::Text(bytes)
                } else {
                    Value::Binary(bytes)
                }));
            }
            other => {
                return PgOutputDecodeSnafu {
                    message: format!("Tuple: unknown tag '{}'", other as char),
                }
                .fail();
            }
        }
    }
    Ok(TupleData { columns })
}

fn read_cstring(buf: &mut Bytes) -> Result<String> {
    // `Bytes` is contiguous, so `chunk()` exposes the whole remaining slice.
    let nul =
        buf.chunk()
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| super::Error::PgOutputDecode {
                message: "unterminated cstring".to_string(),
            })?;
    let s = std::str::from_utf8(&buf.chunk()[..nul])
        .map_err(|e| super::Error::PgOutputDecode {
            message: format!("invalid utf8 in cstring: {e}"),
        })?
        .to_string();
    buf.advance(nul + 1);
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_relation_fixture() -> Vec<u8> {
        let mut out = vec![b'R'];
        out.extend_from_slice(&42u32.to_be_bytes()); // relation_id
        out.extend_from_slice(b"public\0");
        out.extend_from_slice(b"users\0");
        out.push(b'd'); // replica identity DEFAULT
        out.extend_from_slice(&2u16.to_be_bytes()); // ncols
        // Column id: is_key=1, oid=23 (int4), typmod=-1
        out.push(0x01);
        out.extend_from_slice(b"id\0");
        out.extend_from_slice(&23u32.to_be_bytes());
        out.extend_from_slice(&(-1i32).to_be_bytes());
        // Column name: not key, oid=25 (text)
        out.push(0x00);
        out.extend_from_slice(b"name\0");
        out.extend_from_slice(&25u32.to_be_bytes());
        out.extend_from_slice(&(-1i32).to_be_bytes());
        out
    }

    fn build_insert_fixture() -> Vec<u8> {
        let mut out = vec![b'I'];
        out.extend_from_slice(&42u32.to_be_bytes()); // relation_id
        out.push(b'N'); // new tuple tag
        out.extend_from_slice(&2u16.to_be_bytes()); // ncols
        // col 0: text "1"
        out.push(b't');
        out.extend_from_slice(&1u32.to_be_bytes());
        out.push(b'1');
        // col 1: text "Alice"
        out.push(b't');
        out.extend_from_slice(&5u32.to_be_bytes());
        out.extend_from_slice(b"Alice");
        out
    }

    #[test]
    fn decode_begin_commit_roundtrip() {
        let mut decoder = Decoder::new();
        // Begin: final_lsn=0x1234, commit_ts=7, xid=11
        let mut begin = vec![b'B'];
        begin.extend_from_slice(&0x1234u64.to_be_bytes());
        begin.extend_from_slice(&7i64.to_be_bytes());
        begin.extend_from_slice(&11u32.to_be_bytes());
        match decoder.decode(Bytes::from(begin)).expect("decode begin") {
            DecodedMessage::Begin {
                final_lsn,
                commit_ts,
                xid,
            } => {
                assert_eq!(final_lsn, 0x1234);
                assert_eq!(commit_ts, 7);
                assert_eq!(xid, 11);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_relation_inserts_into_cache() {
        let mut decoder = Decoder::new();
        let buf = build_relation_fixture();
        let rel = match decoder.decode(Bytes::from(buf)).expect("decode relation") {
            DecodedMessage::Relation(r) => r,
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(rel.relation_id, 42);
        assert_eq!(rel.namespace, "public");
        assert_eq!(rel.name, "users");
        assert_eq!(rel.columns.len(), 2);
        assert!(rel.columns[0].is_key);
        assert_eq!(rel.columns[0].name, "id");
        assert!(!rel.columns[1].is_key);
        assert_eq!(rel.columns[1].name, "name");
        assert!(decoder.relation(42).is_some());
    }

    #[test]
    fn decode_insert_basic() {
        let mut decoder = Decoder::new();
        let buf = build_insert_fixture();
        let msg = decoder.decode(Bytes::from(buf)).expect("decode insert");
        let DecodedMessage::Insert { relation_id, tuple } = msg else {
            panic!("expected Insert")
        };
        assert_eq!(relation_id, 42);
        assert_eq!(tuple.columns.len(), 2);
        assert!(matches!(tuple.columns[0], Some(Value::Text(ref s)) if s == "1"));
        assert!(matches!(tuple.columns[1], Some(Value::Text(ref s)) if s == "Alice"));
    }

    #[test]
    fn decode_delete_with_key_only() {
        let mut decoder = Decoder::new();
        let mut buf = vec![b'D'];
        buf.extend_from_slice(&42u32.to_be_bytes());
        buf.push(b'K');
        buf.extend_from_slice(&2u16.to_be_bytes());
        // col 0: text "7"
        buf.push(b't');
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(b'7');
        // col 1: null
        buf.push(b'n');
        let DecodedMessage::Delete { relation_id, old } =
            decoder.decode(Bytes::from(buf)).expect("decode")
        else {
            panic!("expected Delete")
        };
        assert_eq!(relation_id, 42);
        assert_eq!(old.columns.len(), 2);
        assert!(matches!(old.columns[0], Some(Value::Text(ref s)) if s == "7"));
        assert!(old.columns[1].is_none());
    }

    #[test]
    fn decode_update_new_only() {
        let mut decoder = Decoder::new();
        let mut buf = vec![b'U'];
        buf.extend_from_slice(&42u32.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.push(b't');
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(b'X');
        let DecodedMessage::Update {
            relation_id,
            old,
            new,
        } = decoder.decode(Bytes::from(buf)).expect("decode")
        else {
            panic!("expected Update")
        };
        assert_eq!(relation_id, 42);
        assert!(old.is_none());
        assert_eq!(new.columns.len(), 1);
        assert!(matches!(new.columns[0], Some(Value::Text(ref s)) if s == "X"));
    }

    #[test]
    fn decode_update_with_key_and_new() {
        let mut decoder = Decoder::new();
        let mut buf = vec![b'U'];
        buf.extend_from_slice(&42u32.to_be_bytes());
        buf.push(b'K');
        // old tuple has 1 col, text "7"
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.push(b't');
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(b'7');
        // new tuple
        buf.push(b'N');
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.push(b't');
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(b'9');
        let DecodedMessage::Update { old, new, .. } =
            decoder.decode(Bytes::from(buf)).expect("decode")
        else {
            panic!("expected Update")
        };
        let old = old.expect("old tuple should be present");
        assert!(matches!(old.columns[0], Some(Value::Text(ref s)) if s == "7"));
        assert!(matches!(new.columns[0], Some(Value::Text(ref s)) if s == "9"));
    }

    #[test]
    fn decode_truncate() {
        let mut decoder = Decoder::new();
        let mut buf = vec![b'T'];
        buf.extend_from_slice(&2u32.to_be_bytes()); // nrel
        buf.push(0x00);
        buf.extend_from_slice(&42u32.to_be_bytes());
        buf.extend_from_slice(&43u32.to_be_bytes());
        let DecodedMessage::Truncate { relation_ids } =
            decoder.decode(Bytes::from(buf)).expect("decode")
        else {
            panic!("expected Truncate")
        };
        assert_eq!(relation_ids, vec![42, 43]);
    }

    #[test]
    fn decode_truncate_does_not_overallocate_on_huge_nrel() {
        // Regression: a malicious or corrupt `nrel` must not drive a multi-GB
        // `Vec::with_capacity` before the per-element bounds check runs. With the
        // cap in place this returns an error quickly instead of attempting a
        // ~16 GiB allocation (which would abort/OOM the process).
        let mut decoder = Decoder::new();
        let mut buf = vec![b'T'];
        buf.extend_from_slice(&u32::MAX.to_be_bytes()); // nrel ≈ 4 billion
        buf.push(0x00); // flags
        buf.extend_from_slice(&42u32.to_be_bytes()); // only one relation id actually present
        decoder
            .decode(Bytes::from(buf))
            .expect_err("oversized truncate nrel should error, not over-allocate");
    }

    #[test]
    fn decode_unknown_message_type_errors() {
        let mut decoder = Decoder::new();
        let buf = [b'Z', 0, 0, 0];
        decoder
            .decode(Bytes::copy_from_slice(&buf))
            .expect_err("unknown message type");
    }
}
