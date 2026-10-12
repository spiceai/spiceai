/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use chrono::{DateTime, Utc};
use graph_rs_sdk::{GraphFailure, error::ErrorMessage};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::error::Error;

/// A Microsoft Graph [identity](https://learn.microsoft.com/en-us/graph/api/resources/identity?view=graph-rest-1.0).
/// Neither field is guaranteed for a drive item.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct User {
    id: Option<String>,
    display_name: Option<String>,
}

/// A Microsoft Graph [identitySet](https://learn.microsoft.com/en-us/graph/api/resources/identityset?view=graph-rest-1.0).
/// Every member is optional: an item that an application created can have no `user`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreatedBy {
    user: Option<User>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LastModifiedBy {
    user: Option<User>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Folder {
    child_count: u32,
}

/// One page of a Microsoft Graph `list children` response. Items stay as JSON until
/// [`parse_drive_item_page`] reads them one at a time, so an error can name the item.
#[derive(Debug, Deserialize)]
struct DriveItemPage {
    value: Vec<serde_json::Value>,
}

/// Represents a Sharepoint [`DriveItem`]. JSON representation from:
///  - get: `<https://learn.microsoft.com/en-us/graph/api/driveitem-get?view=graph-rest-1.0&tabs=http#response-1>`
///  - list: `<https://learn.microsoft.com/en-us/graph/api/driveitem-list-children?view=graph-rest-1.0&tabs=http#response>`
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DriveItem {
    created_by: CreatedBy,

    #[serde(rename = "createdDateTime")]
    created_at: String,
    /// Not returned for folders.
    c_tag: Option<String>,
    e_tag: String,
    folder: Option<Folder>,
    pub(crate) id: String,
    last_modified_by: LastModifiedBy,

    #[serde(rename = "lastModifiedDateTime")]
    last_modified_at: String,
    name: String,
    size: i64,
    web_url: String,
}

impl DriveItem {
    pub(crate) fn is_folder(&self) -> bool {
        self.folder.is_some()
    }
}

pub(crate) static DRIVE_ITEM_FILE_CONTENT_COLUMN: &str = "content";

/// Flattened Arrow schema for [`DriveItem`].
pub fn drive_item_table_schema(include_file_content: bool) -> arrow::datatypes::Schema {
    let mut fields = vec![
        arrow::datatypes::Field::new("created_by_id", arrow::datatypes::DataType::Utf8, true),
        arrow::datatypes::Field::new("created_by_name", arrow::datatypes::DataType::Utf8, true),
        arrow::datatypes::Field::new(
            "created_at",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            false,
        ),
        arrow::datatypes::Field::new("c_tag", arrow::datatypes::DataType::Utf8, true),
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
            true,
        ),
        arrow::datatypes::Field::new(
            "last_modified_by_name",
            arrow::datatypes::DataType::Utf8,
            true,
        ),
        arrow::datatypes::Field::new(
            "last_modified_at",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            false,
        ),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("size", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("web_url", arrow::datatypes::DataType::Utf8, false),
    ];
    if include_file_content {
        fields.push(arrow::datatypes::Field::new(
            DRIVE_ITEM_FILE_CONTENT_COLUMN,
            arrow::datatypes::DataType::Utf8,
            true,
        ));
    }
    arrow::datatypes::Schema::new(fields)
}

/// Parses one page of a Microsoft Graph `list children` response.
///
/// # Errors
///
/// Returns [`Error::MicrosoftGraphFailure`] when Graph answers with an error status, and
/// [`Error::InvalidDriveItemPage`] or [`Error::InvalidDriveItem`] when the page or one of its
/// items does not match the driveItem shape.
pub(crate) fn parse_drive_item_page(
    status: http::StatusCode,
    body: serde_json::Value,
) -> Result<Vec<DriveItem>, Error> {
    if !status.is_success() {
        let message: ErrorMessage = serde_json::from_value(body).unwrap_or_default();
        return Err(Error::MicrosoftGraphFailure {
            source: Box::new(GraphFailure::ErrorMessage(message)),
        });
    }

    let page: DriveItemPage =
        serde_json::from_value(body).map_err(|source| Error::InvalidDriveItemPage { source })?;
    page.value
        .iter()
        .map(|item| {
            serde_path_to_error::deserialize(item).map_err(|source| {
                let field = |key: &str| {
                    item.get(key)
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string()
                };
                Error::InvalidDriveItem {
                    name: field("name"),
                    id: field("id"),
                    source,
                }
            })
        })
        .collect()
}

/// Microsoft graph returns timestamps in ISO 8601 format
fn parse_timestamp(ts: &str) -> ArrowResult<i64> {
    Ok(DateTime::parse_from_rfc3339(ts)
        .map_err(|e| ArrowError::CastError(e.to_string()))?
        .with_timezone(&Utc)
        .timestamp())
}

pub(crate) fn drive_items_to_record_batch(
    drive_items: &[DriveItem],
    item_content: Option<Vec<Option<String>>>,
) -> ArrowResult<RecordBatch> {
    let schema = Arc::new(drive_item_table_schema(item_content.is_some()));

    // Aggregate column wise
    let created_by_id: Vec<Option<&str>> = drive_items
        .iter()
        .map(|item| item.created_by.user.as_ref().and_then(|u| u.id.as_deref()))
        .collect();
    let created_by_name: Vec<Option<&str>> = drive_items
        .iter()
        .map(|item| {
            item.created_by
                .user
                .as_ref()
                .and_then(|u| u.display_name.as_deref())
        })
        .collect();
    let created_date_time: Vec<i64> = drive_items
        .iter()
        .map(|item| parse_timestamp(&item.created_at))
        .collect::<ArrowResult<Vec<i64>>>()?;
    let c_tag: Vec<Option<&str>> = drive_items
        .iter()
        .map(|item| item.c_tag.as_deref())
        .collect();
    let e_tag: Vec<&str> = drive_items.iter().map(|item| item.e_tag.as_str()).collect();
    let folder_child_count: Vec<Option<u32>> = drive_items
        .iter()
        .map(|item| item.folder.clone().map(|f| f.child_count))
        .collect();
    let id: Vec<&str> = drive_items.iter().map(|item| item.id.as_str()).collect();
    let last_modified_by_id: Vec<Option<&str>> = drive_items
        .iter()
        .map(|item| {
            item.last_modified_by
                .user
                .as_ref()
                .and_then(|u| u.id.as_deref())
        })
        .collect();
    let last_modified_by_name: Vec<Option<&str>> = drive_items
        .iter()
        .map(|item| {
            item.last_modified_by
                .user
                .as_ref()
                .and_then(|u| u.display_name.as_deref())
        })
        .collect();
    let last_modified_date_time: Vec<i64> = drive_items
        .iter()
        .map(|item| parse_timestamp(&item.last_modified_at))
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

    let mut columns = vec![
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
    ];

    if let Some(content) = item_content {
        if content.len() != drive_items.len() {
            return Err(ArrowError::InvalidArgumentError(
                "drive item content length does not match drive items in list".to_string(),
            ));
        }
        let content_array = Arc::new(StringArray::from(content)) as ArrayRef;
        columns.push(content_array);
    }

    RecordBatch::try_new(schema, columns)
}

#[cfg(test)]
pub(crate) mod tests {
    use arrow::util::{display::FormatOptions, pretty::pretty_format_batches_with_options};
    use http::StatusCode;
    use serde_json::json;

    use super::*;

    /// A Microsoft Graph `list children` page, recorded and anonymized: a folder (no `cTag`), a file,
    /// and a file with only `application` identities, as Graph returns for an item an application created.
    const LIST_CHILDREN_PAGE: &str = include_str!("testdata/list_children_page.json");

    pub(crate) fn recorded_drive_items() -> Vec<DriveItem> {
        let page: serde_json::Value =
            serde_json::from_str(LIST_CHILDREN_PAGE).expect("fixture should be valid JSON");
        parse_drive_item_page(StatusCode::OK, page).expect("recorded page should parse")
    }

    pub(crate) fn format_columns(batch: &RecordBatch, columns: &[&str]) -> String {
        let indices: Vec<usize> = columns
            .iter()
            .map(|c| batch.schema().index_of(c).expect("column should exist"))
            .collect();
        let projected = batch.project(&indices).expect("projection should succeed");
        pretty_format_batches_with_options(&[projected], &FormatOptions::new().with_null("NULL"))
            .expect("record batch should format")
            .to_string()
    }

    // Regression test for #2797.
    #[test]
    fn parses_folder_file_and_application_created_item() {
        let batch = drive_items_to_record_batch(&recorded_drive_items(), None)
            .expect("drive items should convert to a record batch");

        let table = format_columns(
            &batch,
            &[
                "name",
                "folder_child_count",
                "c_tag",
                "created_by_id",
                "created_by_name",
                "last_modified_by_id",
                "last_modified_by_name",
            ],
        );
        assert_eq!(
            table,
            r#"+----------------------+--------------------+----------------------------------------------+--------------------------------------+-----------------+--------------------------------------+-----------------------+
| name                 | folder_child_count | c_tag                                        | created_by_id                        | created_by_name | last_modified_by_id                  | last_modified_by_name |
+----------------------+--------------------+----------------------------------------------+--------------------------------------+-----------------+--------------------------------------+-----------------------+
| General              | 2                  | NULL                                         | 87d349ed-44d7-43e1-9a83-5f2406dee5bd | Adele Vance     | 87d349ed-44d7-43e1-9a83-5f2406dee5bd | Adele Vance           |
| Quarterly report.pdf | NULL               | "c:{15E70678-2538-429B-895F-404C4DFBFF59},2" | 87d349ed-44d7-43e1-9a83-5f2406dee5bd | Adele Vance     | 87d349ed-44d7-43e1-9a83-5f2406dee5bd | Adele Vance           |
| export.csv           | NULL               | "c:{4D0F5E7A-6C1B-4B8E-9E2A-3F7C1D2B8A90},2" | NULL                                 | NULL            | NULL                                 | NULL                  |
+----------------------+--------------------+----------------------------------------------+--------------------------------------+-----------------+--------------------------------------+-----------------------+"#
        );
    }

    #[test]
    fn names_the_item_and_field_that_fail_to_parse() {
        let page = json!({"value": [{
            "id": "01BYE5RZ4HWMMKFFJCLVFI26HRLREZZ5IT",
            "name": "General",
            "createdBy": {"user": {"id": 42, "displayName": "Adele Vance"}},
            "createdDateTime": "2020-08-21T12:25:29Z",
            "eTag": "\"{A218B307-2295-4A5D-8D78-F15C499CF513},1\"",
            "lastModifiedBy": {"user": {"displayName": "Adele Vance"}},
            "lastModifiedDateTime": "2020-08-21T12:25:29Z",
            "size": 0,
            "webUrl": "https://contoso.sharepoint.com/sites/Marketing/Shared%20Documents/General",
            "folder": {"childCount": 0}
        }]});

        let err = parse_drive_item_page(StatusCode::OK, page)
            .expect_err("an item with a numeric user id should not parse");
        assert_eq!(
            err.to_string(),
            "Failed to read SharePoint drive item 'General' (id '01BYE5RZ4HWMMKFFJCLVFI26HRLREZZ5IT'): the Microsoft Graph response has an unexpected value: createdBy.user.id: invalid type: integer `42`, expected a string. Report this at https://github.com/spiceai/spiceai/issues"
        );
    }

    #[test]
    fn reports_microsoft_graph_errors() {
        let body = json!({"error": {"code": "accessDenied", "message": "Access denied"}});

        let err = parse_drive_item_page(StatusCode::FORBIDDEN, body)
            .expect_err("a 403 response should be an error");
        assert_eq!(
            err.to_string(),
            "Error interacting with Microsoft Sharepoint: Generic Microsoft error. Code: accessDenied, Message: Access denied, Inner error: None"
        );
    }
}
