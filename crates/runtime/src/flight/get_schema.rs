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

use arrow_flight::{
    FlightDescriptor, IpcMessage, SchemaAsIpc, SchemaResult, flight_descriptor::DescriptorType,
};
use arrow_ipc::writer::IpcWriteOptions;
use datafusion::sql::TableReference;
use tonic::{Request, Response, Status};

use crate::{datafusion::request_context_extension::get_current_datafusion, flight::metrics};
use runtime_request_context::{AsyncMarker, RequestContext};

use super::{Service, to_tonic_err};

pub(crate) async fn handle(
    request: Request<FlightDescriptor>,
) -> Result<Response<SchemaResult>, Status> {
    let _start = metrics::track_flight_request("get_schema", None).await;
    tracing::trace!("get_schema: {request:?}");

    let fd = request.into_inner();

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    match fd.r#type {
        x if x == DescriptorType::Cmd as i32 => {
            let sql: &str = std::str::from_utf8(&fd.cmd).map_err(to_tonic_err)?;
            let (arrow_schema, _) = Service::get_arrow_schema(datafusion, sql)
                .await
                .map_err(to_tonic_err)?;
            let options = IpcWriteOptions::default();
            let IpcMessage(schema) = SchemaAsIpc::new(&arrow_schema, &options)
                .try_into()
                .map_err(to_tonic_err)?;

            let schema_result = SchemaResult { schema };

            Ok(Response::new(schema_result))
        }
        x if x == DescriptorType::Path as i32 => {
            let path = fd.path.join(".");
            let table_reference = TableReference::from(path);
            tracing::debug!("get_schema: table_reference: {:?}", table_reference);
            let Some(table) = datafusion.get_table(&table_reference).await else {
                return Err(Status::not_found("Table not found"));
            };
            let schema = table.schema();

            let options = IpcWriteOptions::default();
            let IpcMessage(schema) = SchemaAsIpc::new(&schema, &options)
                .try_into()
                .map_err(to_tonic_err)?;

            let schema_result = SchemaResult { schema };

            Ok(Response::new(schema_result))
        }
        _ => Err(Status::unimplemented("Unsupported descriptor type")),
    }
}
