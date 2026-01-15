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
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    encode::FlightDataEncoderBuilder,
    flight_service_server::FlightService,
    sql::{
        self, Nullable, ProstMessageExt, Searchable, XdbcDataType,
        metadata::{XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder},
    },
};
use futures::{StreamExt, TryStreamExt, stream};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    flight::{Service, metrics, to_tonic_err, util::set_flightsql_protocol},
    timing::TimedStream,
};

/// Get a `FlightInfo` for retrieving XDBC type information.
pub(crate) fn get_flight_info(
    query: sql::CommandGetXdbcTypeInfo,
    request: Request<FlightDescriptor>,
) -> FlightInfo {
    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: query.as_any().encode_to_vec().into(),
    });

    FlightInfo::new()
        .with_endpoint(endpoint)
        .with_descriptor(fd)
}

/// Execute `CommandGetXdbcTypeInfo` and return XDBC type information.
pub(crate) async fn do_get(
    query: sql::CommandGetXdbcTypeInfo,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("get_xdbc_type_info")).await;
    set_flightsql_protocol().await;

    tracing::trace!("do_get_xdbc_type_info: {query:?}");

    let builder = query.into_builder(get_xdbc_type_info_data());
    let batch = builder.build().map_err(to_tonic_err)?;
    let schema = batch.schema();
    let batches = vec![batch];

    let batch_stream = stream::iter(batches).map(Ok);
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batch_stream)
        .map_err(|e| Status::internal(format!("{e:?}")));

    let timed_stream = TimedStream::new(stream, move || start);

    Ok(Response::new(
        Box::pin(timed_stream) as <Service as FlightService>::DoGetStream
    ))
}

/// Get static XDBC type information data.
///
/// This returns metadata about supported data types in a format
/// compatible with ODBC/JDBC drivers.
#[expect(clippy::too_many_lines)]
pub(crate) fn get_xdbc_type_info_data() -> &'static XdbcTypeInfoData {
    static INSTANCE: std::sync::LazyLock<XdbcTypeInfoData> = std::sync::LazyLock::new(|| {
        let mut builder = XdbcTypeInfoDataBuilder::new();

        // String types
        builder.append(XdbcTypeInfo {
            type_name: "VARCHAR".to_string(),
            data_type: XdbcDataType::XdbcVarchar,
            column_size: Some(i32::MAX),
            literal_prefix: Some("'".to_string()),
            literal_suffix: Some("'".to_string()),
            create_params: Some(vec!["length".to_string()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: true,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("VARCHAR".to_string()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcVarchar,
            datetime_subcode: None,
            num_prec_radix: None,
            interval_precision: None,
        });

        // Integer types
        builder.append(XdbcTypeInfo {
            type_name: "INTEGER".to_string(),
            data_type: XdbcDataType::XdbcInteger,
            column_size: Some(10),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            local_type_name: Some("INTEGER".to_string()),
            minimum_scale: Some(0),
            maximum_scale: Some(0),
            sql_data_type: XdbcDataType::XdbcInteger,
            datetime_subcode: None,
            num_prec_radix: Some(10),
            interval_precision: None,
        });

        builder.append(XdbcTypeInfo {
            type_name: "BIGINT".to_string(),
            data_type: XdbcDataType::XdbcBigint,
            column_size: Some(19),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            local_type_name: Some("BIGINT".to_string()),
            minimum_scale: Some(0),
            maximum_scale: Some(0),
            sql_data_type: XdbcDataType::XdbcBigint,
            datetime_subcode: None,
            num_prec_radix: Some(10),
            interval_precision: None,
        });

        builder.append(XdbcTypeInfo {
            type_name: "SMALLINT".to_string(),
            data_type: XdbcDataType::XdbcSmallint,
            column_size: Some(5),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            local_type_name: Some("SMALLINT".to_string()),
            minimum_scale: Some(0),
            maximum_scale: Some(0),
            sql_data_type: XdbcDataType::XdbcSmallint,
            datetime_subcode: None,
            num_prec_radix: Some(10),
            interval_precision: None,
        });

        // Floating point types
        builder.append(XdbcTypeInfo {
            type_name: "DOUBLE".to_string(),
            data_type: XdbcDataType::XdbcDouble,
            column_size: Some(15),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            local_type_name: Some("DOUBLE".to_string()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcDouble,
            datetime_subcode: None,
            num_prec_radix: Some(2),
            interval_precision: None,
        });

        builder.append(XdbcTypeInfo {
            type_name: "FLOAT".to_string(),
            data_type: XdbcDataType::XdbcFloat,
            column_size: Some(7),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            local_type_name: Some("FLOAT".to_string()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcFloat,
            datetime_subcode: None,
            num_prec_radix: Some(2),
            interval_precision: None,
        });

        // Boolean type
        builder.append(XdbcTypeInfo {
            type_name: "BOOLEAN".to_string(),
            data_type: XdbcDataType::XdbcBit,
            column_size: Some(1),
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("BOOLEAN".to_string()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcBit,
            datetime_subcode: None,
            num_prec_radix: None,
            interval_precision: None,
        });

        // Date/Time types
        builder.append(XdbcTypeInfo {
            type_name: "DATE".to_string(),
            data_type: XdbcDataType::XdbcDate,
            column_size: Some(10),
            literal_prefix: Some("'".to_string()),
            literal_suffix: Some("'".to_string()),
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("DATE".to_string()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcDate,
            datetime_subcode: None,
            num_prec_radix: None,
            interval_precision: None,
        });

        builder.append(XdbcTypeInfo {
            type_name: "TIMESTAMP".to_string(),
            data_type: XdbcDataType::XdbcTimestamp,
            column_size: Some(29),
            literal_prefix: Some("'".to_string()),
            literal_suffix: Some("'".to_string()),
            create_params: None,
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("TIMESTAMP".to_string()),
            minimum_scale: Some(0),
            maximum_scale: Some(9),
            sql_data_type: XdbcDataType::XdbcTimestamp,
            datetime_subcode: None,
            num_prec_radix: None,
            interval_precision: None,
        });

        // Binary types
        builder.append(XdbcTypeInfo {
            type_name: "BINARY".to_string(),
            data_type: XdbcDataType::XdbcBinary,
            column_size: Some(i32::MAX),
            literal_prefix: Some("X'".to_string()),
            literal_suffix: Some("'".to_string()),
            create_params: Some(vec!["length".to_string()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("BINARY".to_string()),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::XdbcBinary,
            datetime_subcode: None,
            num_prec_radix: None,
            interval_precision: None,
        });

        match builder.build() {
            Ok(data) => data,
            Err(e) => {
                // This should never happen as we're providing valid static data.
                // If it does, it's a programming error that should be caught during development.
                panic!("Failed to build XDBC type info data - this is a programming error: {e:?}");
            }
        }
    });

    &INSTANCE
}
