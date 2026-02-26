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

use crate::{
    datafusion::request_context_extension::get_current_datafusion,
    flight::{
        Service, metrics, record_batches_to_flight_stream, to_tonic_err,
        util::set_flightsql_protocol,
    },
    timing::TimedStream,
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket, flight_service_server::FlightService, sql,
};
use arrow_schema::Schema;
use arrow_tools::schema::to_source_native_type_name;
use datafusion::datasource::TableType;
use runtime_request_context::{AsyncMarker, RequestContext};
use tonic::{Request, Response, Status};

pub(crate) async fn get_flight_info(
    query: &sql::CommandGetTables,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    let _start = metrics::track_flight_request("get_flight_info", Some("get_tables")).await;
    set_flightsql_protocol().await;

    let fd = request.into_inner();
    tracing::trace!("get_flight_info: {query:?}");

    Response::new(FlightInfo {
        flight_descriptor: Some(fd.clone()),
        endpoint: vec![FlightEndpoint {
            ticket: Some(Ticket { ticket: fd.cmd }),
            ..Default::default()
        }],
        ..Default::default()
    })
}

pub(crate) async fn do_get(
    query: sql::CommandGetTables,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("get_tables")).await;
    set_flightsql_protocol().await;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    let catalog = &query.catalog;
    tracing::trace!("do_get_tables: {query:?}");
    let filtered_catalogs = match catalog {
        Some(catalog) => vec![catalog.clone()],
        None => datafusion.ctx.catalog_names(),
    };
    let mut builder = query.into_builder();

    for catalog_name in filtered_catalogs {
        let catalog_provider = datafusion.ctx.catalog(&catalog_name).ok_or_else(|| {
            Status::internal(format!("unable to get catalog provider for {catalog_name}"))
        })?;

        for schema_name in catalog_provider.schema_names() {
            let Some(schema_provider) = catalog_provider.schema(&schema_name) else {
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

    Ok(Response::new(Box::pin(TimedStream::new(
        record_batches_to_flight_stream(vec![record_batch]),
        move || start,
    ))
        as <Service as FlightService>::DoGetStream))
}

pub(crate) fn table_type_name(table_type: TableType) -> &'static str {
    match table_type {
        // from https://github.com/apache/arrow-datafusion/blob/26b8377b0690916deacf401097d688699026b8fb/datafusion/core/src/catalog/information_schema.rs#L284-L288
        TableType::Base => "BASE TABLE",
        TableType::View => "VIEW",
        TableType::Temporary => "LOCAL TEMPORARY",
    }
}

/// Duplicate Arrow types as data source-specific names for the data type, which is required by some of the clients using Arrow Flight SQL.
/// See `<https://github.com/apache/arrow-rs/blob/639b5bb93e5a152a437b93a25ead8095a3866a9b/arrow-flight/src/sql/arrow.flight.protocol.sql.rs#L172>`
fn with_native_types_metadata(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let mut field = f.as_ref().clone();
            // There is no in-place mutation for field metadata, so we need to clone and then modify it.
            let mut metadata = field.metadata().clone();
            metadata.insert(
                "ARROW:FLIGHT:SQL:TYPE_NAME".to_string(),
                to_source_native_type_name(field.data_type()).to_string(),
            );
            field = field.with_metadata(metadata);
            field
        })
        .collect::<Vec<_>>();
    Schema::new(fields)
}
