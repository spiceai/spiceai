use arrow_flight::{
    flight_service_server::FlightService, sql, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use datafusion::datasource::TableType;
use tonic::{Request, Response, Status};

use crate::flight::{record_batches_to_flight_stream, to_tonic_err, Service};

pub(crate) fn get_flight_info(
    query: &sql::CommandGetTables,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
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
    flight_svc: &Service,
    query: sql::CommandGetTables,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let catalog = &query.catalog;
    tracing::trace!("do_get_tables: {query:?}");
    let filtered_catalogs = match catalog {
        Some(catalog) => vec![catalog.to_string()],
        None => flight_svc.datafusion.read().await.ctx.catalog_names(),
    };
    let mut builder = query.into_builder();

    for catalog_name in filtered_catalogs {
        let catalog_provider = flight_svc
            .datafusion
            .read()
            .await
            .ctx
            .catalog(&catalog_name)
            .ok_or_else(|| {
                Status::internal(format!("unable to get catalog provider for {catalog_name}"))
            })?;

        for schema_name in catalog_provider.schema_names() {
            let Some(schema_provider) = catalog_provider.schema(&schema_name) else {
                continue;
            };

            for table_name in schema_provider.table_names() {
                let Some(table_provider) = schema_provider.table(&table_name).await else {
                    continue;
                };

                let table_type = table_type_name(table_provider.table_type());

                builder.append(
                    &catalog_name,
                    &schema_name,
                    &table_name,
                    table_type,
                    table_provider.schema().as_ref(),
                )?;
            }
        }
    }

    let record_batch = builder.build().map_err(to_tonic_err)?;

    Ok(Response::new(
        Box::pin(record_batches_to_flight_stream(vec![record_batch]))
            as <Service as FlightService>::DoGetStream,
    ))
}

fn table_type_name(table_type: TableType) -> &'static str {
    match table_type {
        // from https://github.com/apache/arrow-datafusion/blob/26b8377b0690916deacf401097d688699026b8fb/datafusion/core/src/catalog/information_schema.rs#L284-L288
        TableType::Base => "BASE TABLE",
        TableType::View => "VIEW",
        TableType::Temporary => "LOCAL TEMPORARY",
    }
}
