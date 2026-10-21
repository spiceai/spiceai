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

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    flight_service_server::FlightService,
    sql::{self},
};
use arrow_tools::schema::to_source_native_type_name;
use datafusion::datasource::TableType;
use datafusion::prelude::SessionContext;
use tonic::{Request, Response, Status};

use crate::{FlightSqlService, record_batches_to_flight_stream, to_tonic_err};

pub(crate) fn get_flight_info(
    _query: &sql::CommandGetTables,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    let fd = request.into_inner();
    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: fd.cmd.clone(),
    });
    Response::new(
        FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(fd),
    )
}

pub(crate) async fn do_get(
    ctx: Arc<SessionContext>,
    query: sql::CommandGetTables,
) -> Result<Response<<FlightSqlService as FlightService>::DoGetStream>, Status> {
    tracing::trace!("do_get get_tables: {query:?}");

    let catalogs = match &query.catalog {
        Some(c) => vec![c.clone()],
        None => ctx.catalog_names(),
    };
    let mut builder = query.into_builder();

    for catalog_name in catalogs {
        let catalog = ctx
            .catalog(&catalog_name)
            .ok_or_else(|| Status::internal(format!("catalog not found: {catalog_name}")))?;
        for schema_name in catalog.schema_names() {
            let Some(schema_provider) = catalog.schema(&schema_name) else {
                continue;
            };
            for table_name in schema_provider.table_names() {
                let Some(table_provider) = schema_provider
                    .table(&table_name)
                    .await
                    .map_err(to_tonic_err)?
                else {
                    continue;
                };
                let table_type = table_type_name(table_provider.table_type());
                let schema = with_native_types_metadata(table_provider.schema().as_ref());
                builder.append(
                    &catalog_name,
                    &schema_name,
                    &table_name,
                    table_type,
                    &schema,
                )?;
            }
        }
    }

    let record_batch = builder.build().map_err(to_tonic_err)?;
    Ok(Response::new(Box::pin(record_batches_to_flight_stream(
        vec![record_batch],
    ))))
}

pub(crate) fn table_type_name(table_type: TableType) -> &'static str {
    match table_type {
        TableType::Base => "BASE TABLE",
        TableType::View => "VIEW",
        TableType::Temporary => "LOCAL TEMPORARY",
    }
}

/// Annotate each field with `ARROW:FLIGHT:SQL:TYPE_NAME` metadata required by
/// some ODBC/JDBC drivers.
fn with_native_types_metadata(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let field = f.as_ref().clone();
            let mut meta = field.metadata().clone();
            meta.insert(
                "ARROW:FLIGHT:SQL:TYPE_NAME".to_string(),
                to_source_native_type_name(field.data_type()).to_string(),
            );
            field.with_metadata(meta)
        })
        .collect::<Vec<_>>();
    Schema::new(fields)
}
