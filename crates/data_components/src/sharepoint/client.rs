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
use std::{any::Any, collections::HashMap, fmt, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef, error::ArrowError};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::project_schema,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
};
use futures::{Stream, StreamExt};

use graph_rs_sdk::{
    default_drive::DefaultDriveApiClient, drives::DrivesIdApiClient, error::ErrorMessage,
    GraphClient, GraphFailure, ODataQuery,
};

use http::Response;
use snafu::ResultExt;

use crate::sharepoint::drive_items::drive_items_to_record_batch;

use super::{
    drive_items::{
        drive_item_table_schema, DriveItem, DriveItemResponse, DRIVE_ITEM_FILE_CONTENT_COLUMN,
    },
    error::Error,
};

/// Represents all the ways a Sharepoint [Drive](https://learn.microsoft.com/en-us/graph/api/resources/drive?view=graph-rest-1.0) can be identified.
#[derive(Default, Debug, Clone, PartialEq)]
pub enum PublicDrivePtr {
    DriveId(String),
    DriveName(String),
    UserId(String),
    GroupId(String),
    GroupName(String),

    SiteId(String),
    SiteName(String),

    #[default]
    Me,
}

/// Represents all the ways a Sharepoint [DriveItem](https://learn.microsoft.com/en-us/graph/api/resources/driveitem?view=graph-rest-1.0) can be identified.
#[derive(Default, Debug, Clone, PartialEq)]
pub enum DriveItemPtr {
    ItemId(String),
    ItemPath(String),

    #[default]
    Root,
}

/// Parse a spicepod dataset's `from` string into its [`DrivePtr`] and [`DriveItemPtr`] components.
///
/// The input string is expected to follow the format:
/// `sharepoint:<drive_type>:<drive_id>/<item_type>:<item_value>`.
///
/// - `<drive_type>` can be "me", "drive", "driveId", "user", "group", "groupId", "site", or "siteId".
/// - `<drive_id>` (optional) is a string identifier corresponding to the drive type. Only empty if `drive_type` is "me".
/// - `<item_type>` can be "root", "item", or "path".
/// - `<item_value>` is a string identifier or path corresponding to the item type. Only empty if `drive_type` is "root".
///
/// # Returns
///
/// A `Result` containing a tuple `(DrivePtr, DriveItemPtr)` if the parsing is successful,
/// or an `Error` if the input format is invalid.
///
/// # Errors
///
/// This function will return an `Error::InvalidDriveFormat` if the input string does not match
/// the expected format or contains an unknown `drive_type` or `item_type`.
///
/// # Example Formats
/// - `"sharepoint:driveId:b!-RIj2DuyvEyV1T4NlOaMHk8XkS_I8MdFlUCq1BlcjgmhRfAj3-Z8RY2VpuvV_tpd/id:01KLLPFP5RRWHNEMUG75BKNGSRXGDRL5C4"`
/// - `"sharepoint:me/root"`
/// - `"sharepoint:drive:Documents/path:/Documents"`
/// - `"sharepoint:site:contoso.sharepoint.com/root"`
/// - `"sharepoint:user:48d31887-5fad-4d73-a9f5-3c356e68a038/path:/documents/reports"`
pub fn parse_from(from: &str) -> Result<(PublicDrivePtr, DriveItemPtr), Error> {
    let (drive, item) = from
        .trim_start_matches("sharepoint:")
        .split_once('/')
        .ok_or(Error::InvalidDriveFormat {
            input: from.to_string(),
        })?;

    let drive_ptr = match drive.split_once(':') {
        None => {
            if drive != "me" {
                return Err(Error::InvalidDriveFormat {
                    input: drive.to_string(),
                });
            };
            PublicDrivePtr::Me
        }
        Some(("siteId", id)) => PublicDrivePtr::SiteId(id.to_string()),
        Some(("site", name)) => PublicDrivePtr::SiteName(name.to_string()),
        Some(("driveId", id)) => PublicDrivePtr::DriveId(id.to_string()),
        Some(("drive", name)) => PublicDrivePtr::DriveName(name.to_string()),
        Some(("user", id)) => PublicDrivePtr::UserId(id.to_string()),
        Some(("groupId", id)) => PublicDrivePtr::GroupId(id.to_string()),
        Some(("group", name)) => PublicDrivePtr::GroupName(name.to_string()),
        _ => {
            return Err(Error::InvalidDriveFormat {
                input: drive.to_string(),
            })
        }
    };

    let item_ptr = match item.split_once(':') {
        None => {
            if item != "root" {
                return Err(Error::InvalidDriveFormat {
                    input: item.to_string(),
                });
            };
            DriveItemPtr::Root
        }
        Some(("id", id)) => DriveItemPtr::ItemId(id.to_string()),
        Some(("path", path)) => DriveItemPtr::ItemPath(path.to_string()),
        _ => {
            return Err(Error::InvalidDriveFormat {
                input: item.to_string(),
            })
        }
    };

    Ok((drive_ptr, item_ptr))
}

/// Unique identifier for a drive. This is a subset of the `DrivePtr` enum types that uniquely identify a drive.
#[derive(Default, Debug, Clone, PartialEq)]
enum DrivePtr {
    DriveId(String),
    UserId(String),
    GroupId(String),
    SiteId(String),

    #[default]
    Me,
}

/// Resolves a `DrivePtr` into a `DriveId`. This ensures that the `DriveId` is unique and can be used to fetch drive items.
async fn resolve_drive_ptr(
    client: Arc<GraphClient>,
    drive: &PublicDrivePtr,
) -> Result<DrivePtr, Error> {
    match drive {
        PublicDrivePtr::DriveId(id) => Ok(DrivePtr::DriveId(id.to_string())),
        PublicDrivePtr::UserId(id) => Ok(DrivePtr::UserId(id.to_string())),
        PublicDrivePtr::GroupId(id) => Ok(DrivePtr::GroupId(id.to_string())),
        PublicDrivePtr::SiteId(id) => Ok(DrivePtr::SiteId(id.to_string())),
        PublicDrivePtr::Me => Ok(DrivePtr::Me),
        PublicDrivePtr::DriveName(name) => {
            let drives = get_drive_items(Arc::clone(&client))
                .await
                .map_err(|e| Error::MicrosoftGraphFailure { source: e })?;
            let Some(drive_id) = drives.get(name) else {
                tracing::warn!(
                    "Drive with name '{}' is not found. Available drives: {}.",
                    name,
                    drives
                        .keys()
                        .map(|name| format!("'{name}'"))
                        .collect::<Vec<String>>()
                        .join(", ")
                );
                return Err(Error::DriveNotFound {
                    drive: name.to_string(),
                });
            };
            Ok(DrivePtr::DriveId(drive_id.to_string()))
        }
        PublicDrivePtr::GroupName(name) => {
            let groups = get_group_items(Arc::clone(&client))
                .await
                .map_err(|e| Error::MicrosoftGraphFailure { source: e })?;
            let Some(group_id) = groups.get(name) else {
                tracing::warn!(
                    "Group with name '{}' is not found. Available groups: {}.",
                    name,
                    groups
                        .keys()
                        .map(|name| format!("'{name}'"))
                        .collect::<Vec<String>>()
                        .join(", ")
                );
                return Err(Error::GroupNotFound {
                    group: name.to_string(),
                });
            };
            Ok(DrivePtr::GroupId(group_id.to_string()))
        }
        PublicDrivePtr::SiteName(name) => {
            let sites = get_site_items(Arc::clone(&client))
                .await
                .map_err(|e| Error::MicrosoftGraphFailure { source: e })?;
            let Some(site_id) = sites.get(name) else {
                tracing::warn!(
                    "Site with name '{}' is not found. Available sites: {}.",
                    name,
                    sites
                        .keys()
                        .map(|name| format!("'{name}'"))
                        .collect::<Vec<String>>()
                        .join(", ")
                );
                return Err(Error::SiteNotFound {
                    site: name.to_string(),
                });
            };
            Ok(DrivePtr::SiteId(site_id.to_string()))
        }
    }
}

/// Possible Microsoft Graph API endpoints to retrieve drive items, determined by the `DrivePtr` variant.
enum DriveApi {
    Id(DrivesIdApiClient),
    Default(DefaultDriveApiClient),
}

pub struct SharepointClient {
    client: Arc<GraphClient>,
    drive: DrivePtr,
    drive_item: DriveItemPtr,
    include_file_content: bool,
}

impl SharepointClient {
    pub async fn new(
        client: Arc<GraphClient>,
        from: &str,
        include_file_content: bool,
    ) -> Result<Self, Error> {
        let (drive, drive_item) = parse_from(from)?;

        // Resolve `PublicDrivePtr`s into internal `DrivePtr`.
        let drive = resolve_drive_ptr(Arc::clone(&client), &drive).await?;

        Ok(Self {
            client,
            drive,
            drive_item,
            include_file_content,
        })
    }
}

#[async_trait]
impl TableProvider for SharepointClient {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(drive_item_table_schema(self.include_file_content))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SharepointListExec::new(
            Arc::clone(&self.client),
            &self.drive,
            &self.drive_item,
            projection,
            &self.schema(),
            limit,
        )?))
    }
}

struct SharepointListExec {
    client: Arc<GraphClient>,
    drive: DrivePtr,
    drive_item: DriveItemPtr,
    schema: SchemaRef,
    properties: PlanProperties,
    projections: Option<Vec<usize>>,
    limit: Option<usize>,
}

impl SharepointListExec {
    pub fn new(
        client: Arc<GraphClient>,
        drive: &DrivePtr,
        drive_item: &DriveItemPtr,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(schema, projections)?;
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Ok(Self {
            client,
            drive: drive.clone(),
            drive_item: drive_item.clone(),
            schema: projected_schema,
            properties,
            limit,
            projections: projections.cloned(),
        })
    }

    /// Streams [`DriveItemResponse`] from the Microsoft Graph API for the [`SharepointListExec`]'s selected drive and drive item.
    fn stream_drive_items(
        &self,
    ) -> Result<
        impl Stream<Item = Result<Response<Result<DriveItemResponse, ErrorMessage>>, GraphFailure>>,
        GraphFailure,
    > {
        // Request docs: `<https://learn.microsoft.com/en-us/graph/api/driveitem-get?view=graph-rest-1.0&tabs=http#http-request>`
        let mut req = match Self::drive_client(&self.client, &self.drive) {
            DriveApi::Id(client) => match &self.drive_item {
                DriveItemPtr::ItemId(id) => client.item(id).list_children(),
                DriveItemPtr::ItemPath(path) => {
                    client.item_by_path(format!(":{path}:")).list_children()
                }
                // _"If this property [root] is non-null, it indicates that the driveItem is the top-most driveItem in the drive."_
                DriveItemPtr::Root => client.items().list_items().filter(&["root ne null"]),
            },

            DriveApi::Default(client) => match &self.drive_item {
                DriveItemPtr::ItemId(id) => client.item(id).list_children(),
                DriveItemPtr::ItemPath(path) => {
                    client.item_by_path(format!(":{path}:")).list_children()
                }
                DriveItemPtr::Root => client.item_by_path("").list_children(),
            },
        };

        // LIMIT `value`
        if let Some(value) = self.limit {
            req = req.top(value.to_string());
        };

        // TODO: Implement the following to improve efficiency.
        // req.filter() // `WHERE <expr>`
        // req.order_by() // `ORDER BY <expr>`
        // req.expand() // To include file content

        req.paging().stream::<DriveItemResponse>()
    }

    /// Returns the drive items, converted into a stream of [`RecordBatch`]s.
    /// If `include_file_content`, the file content for each drive item is downloaded and included
    /// under the `DRIVE_ITEM_FILE_CONTENT_COLUMN` column.
    fn create_record_stream(
        &self,
        include_file_content: bool,
    ) -> DataFusionResult<impl Stream<Item = DataFusionResult<RecordBatch>>> {
        let mut resp_stream = self
            .stream_drive_items()
            .boxed()
            .map_err(DataFusionError::External)?;

        let graph = Arc::clone(&self.client);
        let drive = self.drive.clone();
        let projection = self.projections.clone();

        Ok(stream! {

            while let Some(s) = resp_stream.next().await {
                let response = match s.boxed().map_err(DataFusionError::External) {
                    Ok(r) => r,
                    Err(e) => {
                        yield Err(e);
                        continue;
                    }
                };
                match response.body() {
                    Ok(drive_items) => {
                        let content = if include_file_content {
                            match get_file_content(Arc::clone(&graph), &drive, &drive_items.value).await {
                                Ok(c) => Some(c),
                                Err(e) => {
                                    yield Err(DataFusionError::External(Box::new(e)));
                                    continue;
                                }
                            }
                        } else {
                            None
                        };
                        match drive_items_to_record_batch(&drive_items.value, content) {
                            Ok(record_batch) => {
                                // Ensure that the record batch is projected to the required columns (since `select` on OData from Microsoft Graph isn't used).
                                if let Some(projection) = &projection {
                                    yield record_batch.project(projection).map_err(|e| DataFusionError::ArrowError(e, None))
                                } else {
                                    yield Ok(record_batch)
                                }
                            },
                            Err(e) => yield Err(DataFusionError::ArrowError(e, None)),
                        }
                    },
                    Err(e) => {
                        tracing::debug!("Error fetching drive items. {:#?}", e);
                        yield Err(DataFusionError::External(Box::new(e.clone())))
                    },
                }
            }
        })
    }

    /// Returns the underlying content of a drive item.
    async fn get_drive_item_content(
        graph: &Arc<GraphClient>,
        drive: &DrivePtr,
        item_id: &str,
    ) -> Result<String, GraphFailure> {
        let resp = match Self::drive_client(graph, drive) {
            DriveApi::Id(client) => client.item(item_id).get_items_content(),
            DriveApi::Default(client) => client.item(item_id).get_items_content(),
        }
        .send()
        .await?;

        resp.text().await.map_err(GraphFailure::ReqwestError)
    }

    /// Returns the appropriate [`DriveApi`] for the given [`InternalDrivePtr`].
    fn drive_client(graph: &Arc<GraphClient>, drive: &DrivePtr) -> DriveApi {
        match drive {
            DrivePtr::DriveId(drive_id) => DriveApi::Id(graph.drive(drive_id)),
            DrivePtr::UserId(user_id) => DriveApi::Default(graph.user(user_id).drive()),
            DrivePtr::SiteId(site_id) => DriveApi::Default(graph.site(site_id).drive()),
            DrivePtr::Me => DriveApi::Default(graph.me().drive()),
            DrivePtr::GroupId(group_id) => DriveApi::Default(graph.group(group_id).drive()),
        }
    }
}

impl ExecutionPlan for SharepointListExec {
    fn name(&self) -> &'static str {
        "SharepointListExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Only retrieve file content if it is in projected schema.
        let include_file_content: bool = self
            .schema()
            .index_of(DRIVE_ITEM_FILE_CONTENT_COLUMN)
            .is_ok();
        let stream_adapter = RecordBatchStreamAdapter::new(
            self.schema(),
            self.create_record_stream(include_file_content)?,
        );

        Ok(Box::pin(stream_adapter))
    }
}

impl std::fmt::Debug for SharepointListExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SharepointListExec drive={:?} drive_item={:?}",
            self.drive, self.drive_item
        )
    }
}

impl DisplayAs for SharepointListExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SharepointListExec drive={:?} drive_item={:?}",
            self.drive, self.drive_item
        )
    }
}

/// Downloads the file content for each drive item. Assumes that each fiel in `items` is in the `drive`.
async fn get_file_content(
    graph: Arc<GraphClient>,
    drive: &DrivePtr,
    items: &[DriveItem],
) -> Result<Vec<String>, ArrowError> {
    let mut content: Vec<String> = Vec::with_capacity(items.len());
    for item in items {
        let file = SharepointListExec::get_drive_item_content(&graph, drive, &item.id)
            .await
            .boxed()
            .map_err(ArrowError::ExternalError)?;

        content.push(file);
    }
    Ok(content)
}

/// Returns a mapping of drive ids to drive names.
async fn get_drive_items(graph: Arc<GraphClient>) -> Result<HashMap<String, String>, GraphFailure> {
    let resp = graph
        .drives()
        .list_drive()
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    process_list_objs(&resp, "name")
}

/// Returns a mapping of group ids to group names.
async fn get_group_items(graph: Arc<GraphClient>) -> Result<HashMap<String, String>, GraphFailure> {
    let resp = graph
        .groups()
        .list_group()
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    process_list_objs(&resp, "displayName")
}

/// Returns a mapping of drive ids to drive names.
async fn get_site_items(graph: Arc<GraphClient>) -> Result<HashMap<String, String>, GraphFailure> {
    let resp = graph
        .sites()
        .list_site()
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    process_list_objs(&resp, "name")
}

/// Processes a list of objects returned by Microsoft Graph into a mapping of names to ids.
/// Expected `resp` format (additional fields are ignored):
/// ```json
/// {
///    "value": [
///       { "name": "name1", "id": "id1" },
///       { "name": "name2", "id": "id2" },
///    ]
/// }
/// ```
///
/// Returns (success)
/// ```rust
/// Ok(HashMap<String, String> {
///    "name1": "id1",
///    "name2": "id2",
/// })
/// ```
fn process_list_objs(
    resp: &serde_json::Value,
    name_key: &str,
) -> Result<HashMap<String, String>, GraphFailure> {
    if let Some(serde_json::Value::Array(objs)) = resp.get("value") {
        let output = objs
            .iter()
            .filter_map(|v| {
                let name = v.get(name_key).and_then(|n| n.as_str());
                let id = v.get("id").and_then(|n| n.as_str());
                if let (Some(name), Some(id)) = (name, id) {
                    Some((name.to_string(), id.to_string()))
                } else {
                    tracing::debug!(
                        "Unknown entry in list operation in Microsoft Graph. Response: {:#?}",
                        v
                    );
                    None
                }
            })
            .collect::<HashMap<String, String>>();
        Ok(output)
    } else {
        tracing::debug!(
            "Unknown entry in list operation in Microsoft Graph. Response: {:#?}",
            resp
        );
        Err(GraphFailure::error_kind(
            std::io::ErrorKind::InvalidData,
            "Unexpected response from list operation in Microsoft Graph",
        ))
    }
}
