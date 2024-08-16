use std::fmt;
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
#[allow(unused_variables, dead_code)]
use std::{any::Any, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    common::project_schema,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
};
use futures::{Stream, StreamExt};

use graph_rs_sdk::{
    default_drive::DefaultDriveApiClient, drives::DrivesIdApiClient, error::ErrorMessage,
    GraphClient, GraphFailure,
};

use http::Response;
use snafu::ResultExt;

use crate::sharepoint::drive_items::drive_items_to_record_batch;

use super::{
    drive_items::{drive_item_table_schema, DriveItemResponse},
    error::Error,
};

/// Represents all the ways a Sharepoint [Drive](https://learn.microsoft.com/en-us/graph/api/resources/drive?view=graph-rest-1.0) can be identified.
#[derive(Default, Debug, Clone, PartialEq)]
pub enum DrivePtr {
    DriveId(String),
    UserId(String),
    GroupId(String),

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
/// - `<drive_type>` can be "me", "drive", "user", or "group".
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
/// This function will return an `Error::DriveFormatError` if the input string does not match
/// the expected format or contains an unknown `drive_type` or `item_type`.
///
/// # Example Formats
///
/// - `"sharepoint:drive:b!-RIj2DuyvEyV1T4NlOaMHk8XkS_I8MdFlUCq1BlcjgmhRfAj3-Z8RY2VpuvV_tpd/id:01KLLPFP5RRWHNEMUG75BKNGSRXGDRL5C4"`
/// - `"sharepoint:me/root"`
/// - `"sharepoint:user:48d31887-5fad-4d73-a9f5-3c356e68a038/path:/documents/reports"`
/// ```
pub fn parse_from(from: &str) -> Result<(DrivePtr, DriveItemPtr), Error> {
    let (drive, item) = from
        .trim_start_matches("sharepoint:")
        .split_once('/')
        .ok_or(Error::DriveFormatError {
            input: from.to_string(),
        })?;

    let drive_ptr = match drive.split_once(':') {
        None => {
            if drive != "me" {
                return Err(Error::DriveFormatError {
                    input: drive.to_string(),
                });
            };
            DrivePtr::Me
        }
        Some(("drive", id)) => DrivePtr::DriveId(id.to_string()),
        Some(("user", id)) => DrivePtr::UserId(id.to_string()),
        Some(("group", id)) => DrivePtr::GroupId(id.to_string()),
        _ => {
            return Err(Error::DriveFormatError {
                input: drive.to_string(),
            })
        }
    };

    let item_ptr = match item.split_once(':') {
        None => {
            if item != "root" {
                return Err(Error::DriveFormatError {
                    input: item.to_string(),
                });
            };
            DriveItemPtr::Root
        }
        Some(("id", id)) => DriveItemPtr::ItemId(id.to_string()),
        Some(("path", path)) => DriveItemPtr::ItemPath(path.to_string()),
        _ => {
            return Err(Error::DriveFormatError {
                input: item.to_string(),
            })
        }
    };

    Ok((drive_ptr, item_ptr))
}

enum DriveApi {
    Id(DrivesIdApiClient),
    Default(DefaultDriveApiClient),
}

pub struct SharepointClient {
    client: Arc<GraphClient>,
    drive: DrivePtr,
    drive_item: DriveItemPtr,
}

impl SharepointClient {
    pub fn new(client: Arc<GraphClient>, from: &str) -> Result<Self, Error> {
        let (drive, drive_item) = parse_from(from)?;
        Ok(Self {
            client,
            drive,
            drive_item,
        })
    }

    #[must_use]
    pub fn table_schema() -> arrow::datatypes::Schema {
        drive_item_table_schema()
    }
}

#[async_trait]
impl TableProvider for SharepointClient {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Self::table_schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SharepointListExec::new(
            Arc::clone(&self.client),
            &self.drive,
            &self.drive_item,
            projection,
        )?))
    }
}

struct SharepointListExec {
    client: Arc<GraphClient>,
    drive: DrivePtr,
    drive_item: DriveItemPtr,

    schema: SchemaRef,
    properties: PlanProperties,
}

impl SharepointListExec {
    pub fn new(
        client: Arc<GraphClient>,
        drive: &DrivePtr,
        drive_item: &DriveItemPtr,
        projections: Option<&Vec<usize>>,
    ) -> DataFusionResult<Self> {
        let schema = project_schema(&Arc::new(SharepointClient::table_schema()), projections)?;
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Ok(Self {
            client,
            drive: drive.clone(),
            drive_item: drive_item.clone(),
            schema,
            properties,
        })
    }

    fn drive_client(graph: &Arc<GraphClient>, drive: &DrivePtr) -> DriveApi {
        match drive {
            DrivePtr::DriveId(drive_id) => DriveApi::Id(graph.drive(drive_id)),
            DrivePtr::UserId(user_id) => DriveApi::Default(graph.user(user_id).drive()),
            DrivePtr::GroupId(_group_id) => unimplemented!("group id not supported"), // self.client.group(group_id).get_drive().url(),
            DrivePtr::Me => DriveApi::Default(graph.me().drive()),
        }
    }

    /// TODO: "You can use the $expand query string parameter to include the children of an item in the same call as retrieving the metadata of an item if the item has a children relationship."
    /// `<https://learn.microsoft.com/en-us/graph/api/driveitem-get?view=graph-rest-1.0&tabs=http#optional-query-parameters>`
    /// TODO: might need to explicitly
    fn list_from_path(
        graph: &Arc<GraphClient>,
        drive: &DrivePtr,
        drive_item: &DriveItemPtr,
    ) -> Result<
        impl Stream<Item = Result<Response<Result<DriveItemResponse, ErrorMessage>>, GraphFailure>>,
        GraphFailure,
    > {
        // `<https://learn.microsoft.com/en-us/graph/api/driveitem-get?view=graph-rest-1.0&tabs=http#http-request>`
        let req = match Self::drive_client(graph, drive) {
            DriveApi::Id(client) => match drive_item {
                DriveItemPtr::ItemId(id) => client.item(id).list_children(),
                DriveItemPtr::ItemPath(path) => {
                    client.item_by_path(format!(":{path}:")).list_children()
                }
                DriveItemPtr::Root => client.item_by_path("").list_children(),
            },
            DriveApi::Default(client) => match drive_item {
                DriveItemPtr::ItemId(id) => client.item(id).list_children(),
                DriveItemPtr::ItemPath(path) => {
                    client.item_by_path(format!(":{path}:")).list_children()
                }
                DriveItemPtr::Root => client.item_by_path("").list_children(),
            },
        };

        println!("req={}", req.url());
        req.paging().stream::<DriveItemResponse>()
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
        let stream_adapter = RecordBatchStreamAdapter::new(
            self.schema(),
            process_list_drive_items(
                Arc::clone(&self.client),
                self.drive.clone(),
                self.drive_item.clone(),
            ),
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

fn process_list_drive_items(
    graph: Arc<GraphClient>,
    drive: DrivePtr,
    drive_item: DriveItemPtr,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    stream! {
        match SharepointListExec::list_from_path(&graph, &drive, &drive_item).boxed().map_err(DataFusionError::External) {
            Ok(mut resp) => {

                while let Some(s) = resp.next().await {
                    match s.boxed() {
                        Ok(r) => {
                            match r.body() {
                                Ok(drive_item) => match drive_items_to_record_batch(&drive_item.value).boxed() {
                                    Ok(batch) => yield Ok(batch),
                                    Err(e) => yield Err(DataFusionError::External(e)),
                                },
                                Err(e) => yield Err(DataFusionError::External(Box::new(e.clone()))),
                            }
                        },
                        Err(e) => yield Err(DataFusionError::External(e)),
                    }
                }
            },
            Err(e) => yield Err(e),
        }
    }
}
