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

//! The record format of the object-native Cayenne metastore prototype.
//!
//! A record is the list of typed actions one metastore transaction committed.
//! Large values (schemas, statistics, PK-index blobs, bloom filters) are
//! immutable objects that an action names by BLAKE3 digest and length; small
//! inline payloads (the inline tier's Arrow IPC rows) travel in the record.
//!
//! Protocol rules from the design: no map fields (sorted repeated entries), no
//! floating point, and a field whose zero differs from "absent" is `optional`.

/// A reference to an immutable object: its BLAKE3 digest and its length.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Message)]
pub struct ObjRef {
    #[prost(bytes = "vec", tag = "1")]
    pub digest: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub len: u64,
}

impl ObjRef {
    /// The reference for `bytes`.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            digest: blake3::hash(bytes).as_bytes().to_vec(),
            len: bytes.len() as u64,
        }
    }
}

/// One data file of a snapshot (`cayenne_snapshot_file`).
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct FileEntry {
    #[prost(string, tag = "1")]
    pub path: String,
    #[prost(int64, tag = "2")]
    pub rows: i64,
    #[prost(int64, tag = "3")]
    pub size: i64,
    #[prost(int64, tag = "4")]
    pub min_seq: i64,
    #[prost(int64, tag = "5")]
    pub max_seq: i64,
    #[prost(string, optional, tag = "6")]
    pub digest: Option<String>,
}

/// One deletion vector (`cayenne_delete_file`).
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct DvEntry {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub path: String,
    #[prost(bool, tag = "3")]
    pub relative: bool,
    #[prost(string, tag = "4")]
    pub format: String,
    #[prost(int64, tag = "5")]
    pub count: i64,
    #[prost(int64, tag = "6")]
    pub size: i64,
    #[prost(string, optional, tag = "7")]
    pub source: Option<String>,
    #[prost(int64, tag = "8")]
    pub seq: i64,
    #[prost(int64, optional, tag = "9")]
    pub reinsert: Option<i64>,
}

/// One cold-tier file (`cayenne_cold_tier_file`).
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct ColdEntry {
    #[prost(string, tag = "1")]
    pub url: String,
    #[prost(int64, tag = "2")]
    pub rows: i64,
    #[prost(int64, tag = "3")]
    pub size: i64,
    #[prost(int64, tag = "4")]
    pub min_seq: i64,
    #[prost(int64, tag = "5")]
    pub max_seq: i64,
    #[prost(message, optional, tag = "6")]
    pub stats: Option<ObjRef>,
    #[prost(message, optional, tag = "7")]
    pub bloom: Option<ObjRef>,
}

/// Per-file footer statistics (`cayenne_snapshot_file_statistics`).
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct FileStat {
    #[prost(string, tag = "1")]
    pub snapshot_id: String,
    #[prost(string, tag = "2")]
    pub path: String,
    #[prost(int64, tag = "3")]
    pub size: i64,
    #[prost(int64, tag = "4")]
    pub rows: i64,
    #[prost(message, optional, tag = "5")]
    pub blob: Option<ObjRef>,
}

/// A primary key and a sequence number.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct PkSeq {
    #[prost(bytes = "vec", tag = "1")]
    pub pk: Vec<u8>,
    #[prost(int64, tag = "2")]
    pub seq: i64,
}

/// Which rows a removal selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum Select {
    /// The listed ids.
    Listed = 0,
    /// Every row of the table.
    All = 1,
    /// Every row except the listed ids.
    AllExcept = 2,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct CreateTable {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub path: String,
    #[prost(bool, tag = "4")]
    pub path_is_relative: bool,
    #[prost(message, optional, tag = "5")]
    pub schema: Option<ObjRef>,
    #[prost(string, optional, tag = "6")]
    pub primary_key_json: Option<String>,
    #[prost(string, optional, tag = "7")]
    pub on_conflict_json: Option<String>,
    #[prost(string, optional, tag = "8")]
    pub partition_column: Option<String>,
    #[prost(string, optional, tag = "9")]
    pub vortex_config_json: Option<String>,
    #[prost(string, tag = "10")]
    pub current_snapshot_id: String,
    #[prost(int64, tag = "11")]
    pub seq: i64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct TableRef {
    #[prost(string, tag = "1")]
    pub table_id: String,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SetSchema {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(message, optional, tag = "2")]
    pub schema: Option<ObjRef>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SetConfig {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, optional, tag = "2")]
    pub vortex_config_json: Option<String>,
    #[prost(string, optional, tag = "3")]
    pub on_conflict_json: Option<String>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SetCurrentSnapshot {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub snapshot_id: String,
    /// Guard: the pointer must still be here.
    #[prost(string, optional, tag = "3")]
    pub expected_previous: Option<String>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct AdvanceSeq {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(int64, tag = "2")]
    pub to: i64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct AddFiles {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub snapshot_id: String,
    #[prost(message, repeated, tag = "3")]
    pub files: Vec<FileEntry>,
    /// Replace an existing entry instead of requiring the name to be free.
    #[prost(bool, tag = "4")]
    pub upsert: bool,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct RemoveBySnapshot {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(enumeration = "Select", tag = "2")]
    pub select: i32,
    #[prost(string, repeated, tag = "3")]
    pub snapshot_ids: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SetFileStats {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(message, optional, tag = "2")]
    pub stat: Option<FileStat>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SetGroup {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub snapshot_id: String,
    #[prost(int64, tag = "3")]
    pub seq: i64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct RemoveGroups {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(enumeration = "Select", tag = "2")]
    pub select: i32,
    #[prost(string, repeated, tag = "3")]
    pub snapshot_ids: Vec<String>,
    /// Guard: every listed group must exist.
    #[prost(bool, tag = "4")]
    pub require_present: bool,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct AddDeleteFiles {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(message, repeated, tag = "2")]
    pub files: Vec<DvEntry>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct RemoveDeleteFiles {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(enumeration = "Select", tag = "2")]
    pub select: i32,
    #[prost(string, repeated, tag = "3")]
    pub ids: Vec<String>,
    /// With `All`: only deletion vectors at or below this `seq`.
    #[prost(int64, optional, tag = "4")]
    pub seq_le: Option<i64>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SetTableStats {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(message, optional, tag = "2")]
    pub blob: Option<ObjRef>,
    #[prost(int64, tag = "3")]
    pub num_rows: i64,
    #[prost(message, optional, tag = "4")]
    pub ndv: Option<ObjRef>,
    #[prost(bool, tag = "5")]
    pub exact: bool,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct AddColdFiles {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(message, repeated, tag = "2")]
    pub files: Vec<ColdEntry>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SetPkIndex {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub snapshot_id: String,
    #[prost(message, optional, tag = "3")]
    pub index: Option<ObjRef>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct IndexRun {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub index_key: String,
    #[prost(string, tag = "3")]
    pub run_name: String,
    #[prost(uint64, tag = "4")]
    pub rows: u64,
    #[prost(uint64, tag = "5")]
    pub size: u64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct InlinedData {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub id: String,
    #[prost(string, optional, tag = "3")]
    pub partition_key: Option<String>,
    /// The rows themselves, inline in the record.
    #[prost(bytes = "vec", tag = "4")]
    pub payload: Vec<u8>,
    #[prost(int64, tag = "5")]
    pub count: i64,
    #[prost(int64, tag = "6")]
    pub seq: i64,
    #[prost(string, tag = "7")]
    pub created_at: String,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct UpdateInlinedData {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub id: String,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
    #[prost(int64, tag = "4")]
    pub count: i64,
    /// Guard: the SQL transaction required this inline row to exist.
    #[prost(bool, tag = "5")]
    pub require_present: bool,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct RemoveInlined {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(enumeration = "Select", tag = "2")]
    pub select: i32,
    #[prost(string, repeated, tag = "3")]
    pub ids: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct InlinedDelete {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub id: String,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
    #[prost(int64, tag = "4")]
    pub count: i64,
    #[prost(int64, tag = "5")]
    pub seq: i64,
    #[prost(bool, tag = "6")]
    pub published: bool,
    /// An identical existing entry is a no-op; a different one is an error.
    #[prost(bool, tag = "7")]
    pub idempotent: bool,
    #[prost(string, tag = "8")]
    pub created_at: String,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SetInlinedDeletesPublished {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(enumeration = "Select", tag = "2")]
    pub select: i32,
    #[prost(string, repeated, tag = "3")]
    pub ids: Vec<String>,
    #[prost(bool, tag = "4")]
    pub published: bool,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct AddKeys {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(message, repeated, tag = "2")]
    pub keys: Vec<PkSeq>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct ClearInsertRecords {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(int64, optional, tag = "2")]
    pub seq_le: Option<i64>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct AddPartition {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub partition_id: String,
    #[prost(string, repeated, tag = "3")]
    pub columns: Vec<String>,
    #[prost(string, repeated, tag = "4")]
    pub values: Vec<String>,
    #[prost(string, tag = "5")]
    pub path: String,
    #[prost(bool, tag = "6")]
    pub path_is_relative: bool,
    #[prost(int64, tag = "7")]
    pub record_count: i64,
    #[prost(int64, tag = "8")]
    pub size: i64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct AdvanceSourcePosition {
    #[prost(string, tag = "1")]
    pub source: String,
    #[prost(string, tag = "2")]
    pub partition: String,
    #[prost(bytes = "vec", tag = "3")]
    pub position: Vec<u8>,
}

/// A mutation the tap saw but no converted method described. The state of
/// `table_id` is unknown after it, so the differential skips that table until
/// it is resynchronized.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Unconverted {
    #[prost(string, tag = "1")]
    pub table_id: String,
    #[prost(string, tag = "2")]
    pub method: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Action {
    #[prost(
        oneof = "action::Kind",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36"
    )]
    pub kind: Option<action::Kind>,
}

pub mod action {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        CreateTable(super::CreateTable),
        #[prost(message, tag = "2")]
        DropTable(super::TableRef),
        #[prost(message, tag = "3")]
        SetSchema(super::SetSchema),
        #[prost(message, tag = "4")]
        SetConfig(super::SetConfig),
        #[prost(message, tag = "5")]
        SetCurrentSnapshot(super::SetCurrentSnapshot),
        #[prost(message, tag = "6")]
        AdvanceSeq(super::AdvanceSeq),
        #[prost(message, tag = "7")]
        AddFiles(super::AddFiles),
        #[prost(message, tag = "8")]
        RemoveFiles(super::RemoveBySnapshot),
        #[prost(message, tag = "9")]
        SetFileStats(super::SetFileStats),
        #[prost(message, tag = "10")]
        RemoveFileStats(super::RemoveBySnapshot),
        #[prost(message, tag = "11")]
        SetGroup(super::SetGroup),
        #[prost(message, tag = "12")]
        RemoveGroups(super::RemoveGroups),
        #[prost(message, tag = "13")]
        AddDeleteFiles(super::AddDeleteFiles),
        #[prost(message, tag = "14")]
        RemoveDeleteFiles(super::RemoveDeleteFiles),
        #[prost(message, tag = "15")]
        SetTableStats(super::SetTableStats),
        #[prost(message, tag = "16")]
        ClearTableStats(super::TableRef),
        #[prost(message, tag = "17")]
        AddColdFiles(super::AddColdFiles),
        #[prost(message, tag = "18")]
        ClearColdFiles(super::TableRef),
        #[prost(message, tag = "19")]
        ClearColdStats(super::TableRef),
        #[prost(message, tag = "20")]
        SetPkIndex(super::SetPkIndex),
        #[prost(message, tag = "21")]
        ClearPkIndex(super::TableRef),
        #[prost(message, tag = "22")]
        AddIndexRun(super::IndexRun),
        #[prost(message, tag = "23")]
        RemoveIndexRun(super::IndexRun),
        #[prost(message, tag = "24")]
        AddInlinedData(super::InlinedData),
        #[prost(message, tag = "25")]
        UpdateInlinedData(super::UpdateInlinedData),
        #[prost(message, tag = "26")]
        RemoveInlinedData(super::RemoveInlined),
        #[prost(message, tag = "27")]
        AddInlinedDelete(super::InlinedDelete),
        #[prost(message, tag = "28")]
        SetInlinedDeletesPublished(super::SetInlinedDeletesPublished),
        #[prost(message, tag = "29")]
        RemoveInlinedDeletes(super::RemoveInlined),
        #[prost(message, tag = "30")]
        AddInsertRecords(super::AddKeys),
        #[prost(message, tag = "31")]
        ClearInsertRecords(super::ClearInsertRecords),
        #[prost(message, tag = "32")]
        MarkDirty(super::AddKeys),
        #[prost(message, tag = "33")]
        ClearDelivered(super::AddKeys),
        #[prost(message, tag = "34")]
        AddPartition(super::AddPartition),
        #[prost(message, tag = "35")]
        AdvanceSourcePosition(super::AdvanceSourcePosition),
        #[prost(message, tag = "36")]
        Unconverted(super::Unconverted),
    }
}

/// The actions of one committed metastore transaction.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Record {
    #[prost(message, repeated, tag = "1")]
    pub actions: Vec<Action>,
}

impl Action {
    #[must_use]
    pub fn new(kind: action::Kind) -> Self {
        Self { kind: Some(kind) }
    }

    /// The table the action changes, if it changes one.
    #[must_use]
    pub fn table_id(&self) -> Option<&str> {
        use action::Kind;
        Some(match self.kind.as_ref()? {
            Kind::CreateTable(a) => &a.table_id,
            Kind::DropTable(a)
            | Kind::ClearTableStats(a)
            | Kind::ClearColdFiles(a)
            | Kind::ClearColdStats(a)
            | Kind::ClearPkIndex(a) => &a.table_id,
            Kind::SetSchema(a) => &a.table_id,
            Kind::SetConfig(a) => &a.table_id,
            Kind::SetCurrentSnapshot(a) => &a.table_id,
            Kind::AdvanceSeq(a) => &a.table_id,
            Kind::AddFiles(a) => &a.table_id,
            Kind::RemoveFiles(a) | Kind::RemoveFileStats(a) => &a.table_id,
            Kind::SetFileStats(a) => &a.table_id,
            Kind::SetGroup(a) => &a.table_id,
            Kind::RemoveGroups(a) => &a.table_id,
            Kind::AddDeleteFiles(a) => &a.table_id,
            Kind::RemoveDeleteFiles(a) => &a.table_id,
            Kind::SetTableStats(a) => &a.table_id,
            Kind::AddColdFiles(a) => &a.table_id,
            Kind::SetPkIndex(a) => &a.table_id,
            Kind::AddIndexRun(a) | Kind::RemoveIndexRun(a) => &a.table_id,
            Kind::AddInlinedData(a) => &a.table_id,
            Kind::UpdateInlinedData(a) => &a.table_id,
            Kind::RemoveInlinedData(a) | Kind::RemoveInlinedDeletes(a) => &a.table_id,
            Kind::AddInlinedDelete(a) => &a.table_id,
            Kind::SetInlinedDeletesPublished(a) => &a.table_id,
            Kind::AddInsertRecords(a) | Kind::MarkDirty(a) | Kind::ClearDelivered(a) => &a.table_id,
            Kind::ClearInsertRecords(a) => &a.table_id,
            Kind::AddPartition(a) => &a.table_id,
            Kind::Unconverted(a) => &a.table_id,
            Kind::AdvanceSourcePosition(_) => return None,
        })
    }

    /// A short name for reports.
    #[must_use]
    pub fn name(&self) -> &'static str {
        use action::Kind;
        match &self.kind {
            None => "Empty",
            Some(kind) => match kind {
                Kind::CreateTable(_) => "CreateTable",
                Kind::DropTable(_) => "DropTable",
                Kind::SetSchema(_) => "SetSchema",
                Kind::SetConfig(_) => "SetConfig",
                Kind::SetCurrentSnapshot(_) => "SetCurrentSnapshot",
                Kind::AdvanceSeq(_) => "AdvanceSeq",
                Kind::AddFiles(_) => "AddFiles",
                Kind::RemoveFiles(_) => "RemoveFiles",
                Kind::SetFileStats(_) => "SetFileStats",
                Kind::RemoveFileStats(_) => "RemoveFileStats",
                Kind::SetGroup(_) => "SetGroup",
                Kind::RemoveGroups(_) => "RemoveGroups",
                Kind::AddDeleteFiles(_) => "AddDeleteFiles",
                Kind::RemoveDeleteFiles(_) => "RemoveDeleteFiles",
                Kind::SetTableStats(_) => "SetTableStats",
                Kind::ClearTableStats(_) => "ClearTableStats",
                Kind::AddColdFiles(_) => "AddColdFiles",
                Kind::ClearColdFiles(_) => "ClearColdFiles",
                Kind::ClearColdStats(_) => "ClearColdStats",
                Kind::SetPkIndex(_) => "SetPkIndex",
                Kind::ClearPkIndex(_) => "ClearPkIndex",
                Kind::AddIndexRun(_) => "AddIndexRun",
                Kind::RemoveIndexRun(_) => "RemoveIndexRun",
                Kind::AddInlinedData(_) => "AddInlinedData",
                Kind::UpdateInlinedData(_) => "UpdateInlinedData",
                Kind::RemoveInlinedData(_) => "RemoveInlinedData",
                Kind::AddInlinedDelete(_) => "AddInlinedDelete",
                Kind::SetInlinedDeletesPublished(_) => "SetInlinedDeletesPublished",
                Kind::RemoveInlinedDeletes(_) => "RemoveInlinedDeletes",
                Kind::AddInsertRecords(_) => "AddInsertRecords",
                Kind::ClearInsertRecords(_) => "ClearInsertRecords",
                Kind::MarkDirty(_) => "MarkDirty",
                Kind::ClearDelivered(_) => "ClearDelivered",
                Kind::AddPartition(_) => "AddPartition",
                Kind::AdvanceSourcePosition(_) => "AdvanceSourcePosition",
                Kind::Unconverted(_) => "Unconverted",
            },
        }
    }
}
