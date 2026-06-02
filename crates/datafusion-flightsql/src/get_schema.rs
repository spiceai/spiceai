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

use arrow_flight::{
    FlightDescriptor, IpcMessage, SchemaAsIpc, SchemaResult, flight_descriptor::DescriptorType,
};
use arrow_ipc::writer::IpcWriteOptions;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use tonic::{Request, Response, Status};

use crate::{FlightSqlService, handle_datafusion_error, to_tonic_err};

pub(crate) async fn handle(
    ctx: Arc<SessionContext>,
    request: Request<FlightDescriptor>,
) -> Result<Response<SchemaResult>, Status> {
    let fd = request.into_inner();

    match fd.r#type {
        x if x == DescriptorType::Cmd as i32 => {
            let sql = std::str::from_utf8(&fd.cmd)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let schema = FlightSqlService::get_arrow_schema(&ctx, sql).await?;

            let options = IpcWriteOptions::default();
            let IpcMessage(schema_bytes) = SchemaAsIpc::new(&schema, &options)
                .try_into()
                .map_err(to_tonic_err)?;
            Ok(Response::new(SchemaResult {
                schema: schema_bytes,
            }))
        }
        x if x == DescriptorType::Path as i32 => {
            let table_ref = TableReference::from(fd.path.join("."));
            let table = ctx
                .table_provider(table_ref)
                .await
                .map_err(handle_datafusion_error)?;
            let schema = table.schema();
            let options = IpcWriteOptions::default();
            let IpcMessage(schema_bytes) = SchemaAsIpc::new(schema.as_ref(), &options)
                .try_into()
                .map_err(to_tonic_err)?;
            Ok(Response::new(SchemaResult {
                schema: schema_bytes,
            }))
        }
        _ => Err(Status::unimplemented("Unsupported descriptor type")),
    }
}
