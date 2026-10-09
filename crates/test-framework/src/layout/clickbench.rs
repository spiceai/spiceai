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

//! `ClickBench` layout keys. `hits` has no declared key; the primary key is the
//! one `ClickBench`'s own `PostgreSQL` schema declares, which the full
//! 99,997,497-row dataset loads under, so it is unique. Its string columns are
//! binary in `hits.parquet`, so the indexes are on its numeric columns.
//! `EventTime` is seconds since the epoch, `EventDate` days since it.

use spicepod::component::dataset::TimeFormat;

use super::TableKeys;

pub(super) static TABLES: &[TableKeys] = &[TableKeys {
    table: "hits",
    primary_key: &["CounterID", "EventDate", "UserID", "EventTime", "WatchID"],
    indexes: &[&["UserID"], &["CounterID"], &["RegionID"]],
    sort: &["CounterID", "EventDate"],
    cluster: &["CounterID", "EventDate"],
    time_column: Some(("EventTime", TimeFormat::UnixSeconds)),
    partition_by: Some("bucket(5, \"EventDate\")"),
}];
