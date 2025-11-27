// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Generic [FlightTableFactory] that can connect to Arrow Flight services,
//! with a [sql::FlightSqlDriver] provided out-of-the-box.

use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;

use crate::flight::exec::FlightExec;
use arrow_flight::error::FlightError;
use arrow_flight::FlightInfo;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{CreateExternalTable, Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};
use tonic::transport::{Channel, ClientTlsConfig};

pub mod codec;
mod exec;
pub mod sql;

pub use exec::enforce_schema;

/// Generic Arrow Flight data source. Requires a [FlightDriver] that allows implementors
/// to integrate any custom Flight RPC service by producing a [FlightMetadata] for some DDL.
///
/// # Sample usage:
/// ```
/// # use arrow_flight::{FlightClient, FlightDescriptor};
/// # use datafusion::prelude::SessionContext;
/// # use datafusion_table_providers::flight::{FlightDriver, FlightMetadata, FlightTableFactory};
/// # use std::collections::HashMap;
/// # use std::sync::Arc;
/// # use tonic::transport::Channel;
///
/// #[derive(Debug, Clone, Default)]
/// struct CustomFlightDriver {}
///
/// #[async_trait::async_trait]
/// impl FlightDriver for CustomFlightDriver {
///     async fn metadata(
///         &self,
///         channel: Channel,
///         opts: &HashMap<String, String>,
///     ) -> arrow_flight::error::Result<FlightMetadata> {
///         let mut client = FlightClient::new(channel);
///         // for simplicity, we'll just assume the server expects a string command and no handshake
///         let descriptor = FlightDescriptor::new_cmd(opts["flight.command"].clone());
///         let flight_info = client.get_flight_info(descriptor).await?;
///         FlightMetadata::try_from(flight_info)
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let ctx = SessionContext::new();
///     ctx.state_ref().write().table_factories_mut().insert(
///         "CUSTOM_FLIGHT".into(),
///         Arc::new(FlightTableFactory::new(Arc::new(
///             CustomFlightDriver::default(),
///         ))),
///     );
///     let _ = ctx.sql(
///         r#"
///         CREATE EXTERNAL TABLE custom_flight_table STORED AS CUSTOM_FLIGHT
///         LOCATION 'https://custom.flight.rpc'
///         OPTIONS ('flight.command' 'AI, show me the data!')
///     "#,
///     ); // no .await here, so we don't actually try to connect to the bogus URL
/// }
/// ```
#[derive(Clone, Debug)]
pub struct FlightTableFactory {
    driver: Arc<dyn FlightDriver>,
}

impl FlightTableFactory {
    /// Create a data source using the provided driver
    pub fn new(driver: Arc<dyn FlightDriver>) -> Self {
        Self { driver }
    }

    /// Convenient way to create a [FlightTable] programatically, as an alternative to DDL.
    pub async fn open_table(
        &self,
        entry_point: impl Into<String>,
        options: HashMap<String, String>,
    ) -> datafusion::common::Result<FlightTable> {
        let origin = entry_point.into();
        let channel = flight_channel(&origin).await?;
        let metadata = self
            .driver
            .metadata(channel.clone(), &options)
            .await
            .map_err(to_df_err)?;
        let num_rows = precision(metadata.info.total_records);
        let total_byte_size = precision(metadata.info.total_bytes);
        let logical_schema = metadata.schema.clone();
        let stats = Statistics {
            num_rows,
            total_byte_size,
            column_statistics: vec![],
        };
        let metadata_supplier = if metadata.props.reusable_flight_info {
            MetadataSupplier::Reusable(Arc::new(metadata))
        } else {
            MetadataSupplier::Refresh {
                driver: self.driver.clone(),
                channel,
                options,
            }
        };
        Ok(FlightTable {
            metadata_supplier,
            origin,
            logical_schema,
            stats,
        })
    }
}

#[async_trait]
impl TableProviderFactory for FlightTableFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let table = self.open_table(&cmd.location, cmd.options.clone()).await?;
        Ok(Arc::new(table))
    }
}

/// Extension point for integrating any Flight RPC service as a [FlightTableFactory].
/// Handles the initial `GetFlightInfo` call and all its prerequisites (such as `Handshake`),
/// to produce a [FlightMetadata].
#[async_trait]
pub trait FlightDriver: Sync + Send + Debug {
    /// Returns a [FlightMetadata] from the specified channel,
    /// according to the provided table options.
    /// The driver must provide at least a [FlightInfo] in order to construct a flight metadata.
    async fn metadata(
        &self,
        channel: Channel,
        options: &HashMap<String, String>,
    ) -> arrow_flight::error::Result<FlightMetadata>;
}

/// The information that a [FlightDriver] must produce
/// in order to register flights as DataFusion tables.
#[derive(Clone, Debug)]
pub struct FlightMetadata {
    /// FlightInfo object produced by the driver
    info: FlightInfo,
    /// Various knobs that control execution
    props: FlightProperties,
    /// Arrow schema. Can be enforced by the driver or inferred from the FlightInfo
    schema: SchemaRef,
}

impl FlightMetadata {
    /// Customize everything that is in the driver's control
    pub fn new(info: FlightInfo, props: FlightProperties, schema: SchemaRef) -> Self {
        Self {
            info,
            props,
            schema,
        }
    }

    /// Customize flight properties and try to use the FlightInfo schema
    pub fn try_new(info: FlightInfo, props: FlightProperties) -> arrow_flight::error::Result<Self> {
        let schema = Arc::new(info.clone().try_decode_schema()?);
        Ok(Self::new(info, props, schema))
    }
}

impl TryFrom<FlightInfo> for FlightMetadata {
    type Error = FlightError;

    fn try_from(info: FlightInfo) -> Result<Self, Self::Error> {
        Self::try_new(info, FlightProperties::default())
    }
}

/// Meant to gradually encapsulate all sorts of knobs required
/// for controlling the protocol and query execution details.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FlightProperties {
    unbounded_streams: bool,
    grpc_headers: HashMap<String, String>,
    size_limits: SizeLimits,
    reusable_flight_info: bool,
}

impl FlightProperties {
    pub fn new() -> Self {
        Default::default()
    }

    /// Whether the service will produce infinite streams
    pub fn with_unbounded_streams(mut self, unbounded_streams: bool) -> Self {
        self.unbounded_streams = unbounded_streams;
        self
    }

    /// gRPC headers to use on subsequent calls.
    pub fn with_grpc_headers(mut self, grpc_headers: HashMap<String, String>) -> Self {
        self.grpc_headers = grpc_headers;
        self
    }

    /// Max sizes in bytes for encoded/decoded gRPC messages.
    pub fn with_size_limits(mut self, size_limits: SizeLimits) -> Self {
        self.size_limits = size_limits;
        self
    }

    /// Whether the FlightInfo objects produced by the service can be used multiple times
    /// or need to be refreshed before every table scan.
    pub fn with_reusable_flight_info(mut self, reusable_flight_info: bool) -> Self {
        self.reusable_flight_info = reusable_flight_info;
        self
    }
}

/// Message size limits to be passed to the underlying gRPC library.
#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SizeLimits {
    encoding: usize,
    decoding: usize,
}

impl SizeLimits {
    pub fn new(encoding: usize, decoding: usize) -> Self {
        Self { encoding, decoding }
    }
}

impl Default for SizeLimits {
    fn default() -> Self {
        Self {
            // no limits
            encoding: usize::MAX,
            decoding: usize::MAX,
        }
    }
}

#[derive(Clone, Debug)]
enum MetadataSupplier {
    Reusable(Arc<FlightMetadata>),
    Refresh {
        driver: Arc<dyn FlightDriver>,
        channel: Channel,
        options: HashMap<String, String>,
    },
}

impl MetadataSupplier {
    async fn flight_metadata(&self) -> datafusion::common::Result<Arc<FlightMetadata>> {
        match self {
            Self::Reusable(metadata) => Ok(metadata.clone()),
            Self::Refresh {
                driver,
                channel,
                options,
            } => Ok(Arc::new(
                driver
                    .metadata(channel.clone(), options)
                    .await
                    .map_err(to_df_err)?,
            )),
        }
    }
}

/// Table provider that wraps a specific flight from an Arrow Flight service
pub struct FlightTable {
    metadata_supplier: MetadataSupplier,
    origin: String,
    logical_schema: SchemaRef,
    stats: Statistics,
}

impl std::fmt::Debug for FlightTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightTable")
            .field("origin", &self.origin)
            .field("logical_schema", &self.logical_schema)
            .field("stats", &self.stats)
            .finish()
    }
}

#[async_trait]
impl TableProvider for FlightTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.logical_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let metadata = self.metadata_supplier.flight_metadata().await?;
        Ok(Arc::new(FlightExec::try_new(
            metadata.as_ref(),
            projection,
            &self.origin,
        )?))
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(self.stats.clone())
    }
}

fn to_df_err<E: Error + Send + Sync + 'static>(err: E) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

async fn flight_channel(source: impl Into<String>) -> datafusion::common::Result<Channel> {
    let tls_config = ClientTlsConfig::new().with_enabled_roots();
    Channel::from_shared(source.into())
        .map_err(to_df_err)?
        .tls_config(tls_config)
        .map_err(to_df_err)?
        .connect()
        .await
        .map_err(to_df_err)
}

fn precision(total: i64) -> Precision<usize> {
    if total < 0 {
        Precision::Absent
    } else {
        Precision::Exact(total as usize)
    }
}
