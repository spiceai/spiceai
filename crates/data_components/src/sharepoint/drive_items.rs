use chrono::{DateTime, Utc};
/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampSecondArray, UInt32Array};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct User {
    id: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreatedBy {
    user: User,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LastModifiedBy {
    user: User,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Folder {
    child_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DriveItemResponse {
    pub value: Vec<DriveItem>,

    // @odata.nextLink e.g. `"https://graph.microsoft.com/v1.0/users?$top=5&$skiptoken=RFNwdAIAAQAAAD8...AAAAAAAA"`
    #[serde(rename = "@odata.nextLink")]
    pub next_link: Option<String>,
}

/// Represents a Sharepoint [`DriveItem`]. JSON representation from:
///  - get: `<https://learn.microsoft.com/en-us/graph/api/driveitem-get?view=graph-rest-1.0&tabs=http#response-1>`
///  - list: `<https://learn.microsoft.com/en-us/graph/api/driveitem-list-children?view=graph-rest-1.0&tabs=http#response>`
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DriveItem {
    created_by: CreatedBy,
    created_date_time: String,
    c_tag: String,
    e_tag: String,
    folder: Option<Folder>,
    id: String,
    last_modified_by: LastModifiedBy,
    last_modified_date_time: String,
    name: String,
    // root: Option<Root>, struct Root {}
    size: i64,
    web_url: String,
}

/// Flattened Arrow schema for [`DriveItem`].
pub fn drive_item_table_schema() -> arrow::datatypes::Schema {
    arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("created_by_id", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("created_by_name", arrow::datatypes::DataType::Utf8, true),
        // "2016-03-21T20:01:37Z",
        arrow::datatypes::Field::new(
            "created_date_time",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            false,
        ),
        arrow::datatypes::Field::new("c_tag", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("e_tag", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new(
            "folder_child_count",
            arrow::datatypes::DataType::UInt32,
            true,
        ),
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new(
            "last_modified_by_id",
            arrow::datatypes::DataType::Utf8,
            false,
        ),
        arrow::datatypes::Field::new(
            "last_modified_by_name",
            arrow::datatypes::DataType::Utf8,
            true,
        ),
        arrow::datatypes::Field::new(
            "last_modified_date_time",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            false,
        ),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("size", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("web_url", arrow::datatypes::DataType::Utf8, false),
    ])
}

/// Microsoft graph returns timestamps in ISO 8601 format
fn parse_timestamp(ts: &str) -> ArrowResult<i64> {
    Ok(DateTime::parse_from_rfc3339(ts)
        .map_err(|e| ArrowError::CastError(e.to_string()))?
        .with_timezone(&Utc)
        .timestamp())
}

pub(crate) fn drive_items_to_record_batch(drive_items: &[DriveItem]) -> ArrowResult<RecordBatch> {
    let schema = Arc::new(drive_item_table_schema());

    // Aggregate column wise
    let created_by_id: Vec<&str> = drive_items
        .iter()
        .map(|item| item.created_by.user.id.as_str())
        .collect();
    let created_by_name: Vec<Option<&str>> = drive_items
        .iter()
        .map(|item| Some(item.created_by.user.display_name.as_str()))
        .collect();
    let created_date_time: Vec<i64> = drive_items
        .iter()
        .map(|item| parse_timestamp(&item.created_date_time))
        .collect::<ArrowResult<Vec<i64>>>()?;
    let c_tag: Vec<&str> = drive_items.iter().map(|item| item.c_tag.as_str()).collect();
    let e_tag: Vec<&str> = drive_items.iter().map(|item| item.e_tag.as_str()).collect();
    let folder_child_count: Vec<Option<u32>> = drive_items
        .iter()
        .map(|item| item.folder.clone().map(|f| f.child_count))
        .collect();
    let id: Vec<&str> = drive_items.iter().map(|item| item.id.as_str()).collect();
    let last_modified_by_id: Vec<&str> = drive_items
        .iter()
        .map(|item| item.last_modified_by.user.id.as_str())
        .collect();
    let last_modified_by_name: Vec<Option<&str>> = drive_items
        .iter()
        .map(|item| Some(item.last_modified_by.user.display_name.as_str()))
        .collect();
    let last_modified_date_time: Vec<i64> = drive_items
        .iter()
        .map(|item| parse_timestamp(&item.last_modified_date_time))
        .collect::<ArrowResult<Vec<i64>>>()?;
    let name: Vec<&str> = drive_items.iter().map(|item| item.name.as_str()).collect();
    let size: Vec<i64> = drive_items.iter().map(|item| item.size).collect();
    let web_url: Vec<&str> = drive_items
        .iter()
        .map(|item| item.web_url.as_str())
        .collect();

    // Create the Arrow arrays
    let created_by_id_array = Arc::new(StringArray::from(created_by_id)) as ArrayRef;
    let created_by_name_array = Arc::new(StringArray::from(created_by_name)) as ArrayRef;
    let created_date_time_array =
        Arc::new(TimestampSecondArray::from(created_date_time)) as ArrayRef;
    let c_tag_array = Arc::new(StringArray::from(c_tag)) as ArrayRef;
    let e_tag_array = Arc::new(StringArray::from(e_tag)) as ArrayRef;
    let folder_child_count_array = Arc::new(UInt32Array::from(folder_child_count)) as ArrayRef;
    let id_array = Arc::new(StringArray::from(id)) as ArrayRef;
    let last_modified_by_id_array = Arc::new(StringArray::from(last_modified_by_id)) as ArrayRef;
    let last_modified_by_name_array =
        Arc::new(StringArray::from(last_modified_by_name)) as ArrayRef;
    let last_modified_date_time_array =
        Arc::new(TimestampSecondArray::from(last_modified_date_time)) as ArrayRef;
    let name_array = Arc::new(StringArray::from(name)) as ArrayRef;
    let size_array = Arc::new(Int64Array::from(size)) as ArrayRef;
    let web_url_array = Arc::new(StringArray::from(web_url)) as ArrayRef;

    RecordBatch::try_new(
        schema,
        vec![
            created_by_id_array,
            created_by_name_array,
            created_date_time_array,
            c_tag_array,
            e_tag_array,
            folder_child_count_array,
            id_array,
            last_modified_by_id_array,
            last_modified_by_name_array,
            last_modified_date_time_array,
            name_array,
            size_array,
            web_url_array,
        ],
    )
}
