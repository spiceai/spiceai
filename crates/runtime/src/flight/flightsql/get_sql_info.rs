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

use std::collections::HashMap;

use arrow::{
    compute::can_cast_types,
    datatypes::{DataType, IntervalUnit, TimeUnit},
};
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_service_server::FlightService,
    sql::{
        self,
        metadata::{SqlInfoData, SqlInfoDataBuilder},
        ProstMessageExt, SqlInfo, SqlNullOrdering, SqlSupportedCaseSensitivity,
        SqlSupportedTransactions, SqlSupportsConvert, SupportedSqlGrammar,
    },
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use futures::{stream, StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    flight::{metrics, to_tonic_err, util::set_flightsql_protocol, Service},
    timing::TimedStream,
};

/// Get a `FlightInfo` for retrieving `SqlInfo`.
pub(crate) async fn get_flight_info(
    query: &sql::CommandGetSqlInfo,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let _start = metrics::track_flight_request("get_flight_info", Some("get_sql_info")).await;
    set_flightsql_protocol().await;

    tracing::trace!("get_flight_info_get_sql_info: query={query:?}");
    let builder = query.clone().into_builder(get_sql_info_data());
    let record_batch = builder.build().map_err(to_tonic_err)?;

    let fd = request.into_inner();

    let ticket = Ticket {
        ticket: query.as_any().encode_to_vec().into(),
    };

    let endpoint = FlightEndpoint::new().with_ticket(ticket);

    Ok(Response::new(
        FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(fd)
            .try_with_schema(&record_batch.schema())
            .map_err(to_tonic_err)?,
    ))
}

/// Get a `FlightDataStream` containing the list of `SqlInfo` results.
pub(crate) async fn do_get(
    query: sql::CommandGetSqlInfo,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    tracing::trace!("do_get_sql_info: {query:?}");
    let start = metrics::track_flight_request("do_get", Some("get_sql_info")).await;
    set_flightsql_protocol().await;

    let builder = query.into_builder(get_sql_info_data());
    let record_batch = builder.build().map_err(to_tonic_err)?;

    let batches_stream = stream::iter(vec![Ok(record_batch)]);

    let flight_data_stream = FlightDataEncoderBuilder::new().build(batches_stream);

    Ok(Response::new(
        TimedStream::new(flight_data_stream.map_err(to_tonic_err), move || start).boxed(),
    ))
}

const SQL_INFO_SQL_KEYWORDS: &[&str] = &[
    // SQL-92 Reserved Words
    "absolute",
    "action",
    "add",
    "all",
    "allocate",
    "alter",
    "and",
    "any",
    "are",
    "as",
    "asc",
    "assertion",
    "at",
    "authorization",
    "avg",
    "begin",
    "between",
    "bit",
    "bit_length",
    "both",
    "by",
    "cascade",
    "cascaded",
    "case",
    "cast",
    "catalog",
    "char",
    "char_length",
    "character",
    "character_length",
    "check",
    "close",
    "coalesce",
    "collate",
    "collation",
    "column",
    "commit",
    "connect",
    "connection",
    "constraint",
    "constraints",
    "continue",
    "convert",
    "corresponding",
    "count",
    "create",
    "cross",
    "current",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "cursor",
    "date",
    "day",
    "deallocate",
    "dec",
    "decimal",
    "declare",
    "default",
    "deferrable",
    "deferred",
    "delete",
    "desc",
    "describe",
    "descriptor",
    "diagnostics",
    "disconnect",
    "distinct",
    "domain",
    "double",
    "drop",
    "else",
    "end",
    "end-exec",
    "escape",
    "except",
    "exception",
    "exec",
    "execute",
    "exists",
    "external",
    "extract",
    "false",
    "fetch",
    "first",
    "float",
    "for",
    "foreign",
    "found",
    "from",
    "full",
    "get",
    "global",
    "go",
    "goto",
    "grant",
    "group",
    "having",
    "hour",
    "identity",
    "immediate",
    "in",
    "indicator",
    "initially",
    "inner",
    "input",
    "insensitive",
    "insert",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "is",
    "isolation",
    "join",
    "key",
    "language",
    "last",
    "leading",
    "left",
    "level",
    "like",
    "local",
    "lower",
    "match",
    "max",
    "min",
    "minute",
    "module",
    "month",
    "names",
    "national",
    "natural",
    "nchar",
    "next",
    "no",
    "not",
    "null",
    "nullif",
    "numeric",
    "octet_length",
    "of",
    "on",
    "only",
    "open",
    "option",
    "or",
    "order",
    "outer",
    "output",
    "overlaps",
    "pad",
    "partial",
    "position",
    "precision",
    "prepare",
    "preserve",
    "primary",
    "prior",
    "privileges",
    "procedure",
    "public",
    "read",
    "real",
    "references",
    "relative",
    "restrict",
    "revoke",
    "right",
    "rollback",
    "rows",
    "schema",
    "scroll",
    "second",
    "section",
    "select",
    "session",
    "session_user",
    "set",
    "size",
    "smallint",
    "some",
    "space",
    "sql",
    "sqlcode",
    "sqlerror",
    "sqlstate",
    "substring",
    "sum",
    "system_user",
    "table",
    "temporary",
    "then",
    "time",
    "timestamp",
    "timezone_hour",
    "timezone_minute",
    "to",
    "trailing",
    "transaction",
    "translate",
    "translation",
    "trim",
    "true",
    "union",
    "unique",
    "unknown",
    "update",
    "upper",
    "usage",
    "user",
    "using",
    "value",
    "values",
    "varchar",
    "varying",
    "view",
    "when",
    "whenever",
    "where",
    "with",
    "work",
    "write",
    "year",
    "zone",
];

const SQL_INFO_NUMERIC_FUNCTIONS: &[&str] = &[
    "abs", "acos", "asin", "atan", "atan2", "ceil", "cos", "exp", "floor", "ln", "log", "log10",
    "log2", "pow", "power", "round", "signum", "sin", "sqrt", "tan", "trunc",
];

const SQL_INFO_STRING_FUNCTIONS: &[&str] = &[
    "arrow_typeof",
    "ascii",
    "bit_length",
    "btrim",
    "char_length",
    "character_length",
    "chr",
    "concat",
    "concat_ws",
    "digest",
    "from_unixtime",
    "initcap",
    "left",
    "length",
    "lower",
    "lpad",
    "ltrim",
    "md5",
    "octet_length",
    "random",
    "regexp_match",
    "regexp_replace",
    "repeat",
    "replace",
    "reverse",
    "right",
    "rpad",
    "rtrim",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "split_part",
    "starts_with",
    "strpos",
    "substr",
    "to_hex",
    "translate",
    "trim",
    "upper",
    "uuid",
];

const SQL_INFO_DATE_TIME_FUNCTIONS: &[&str] = &[
    "current_date",
    "current_time",
    "date_bin",
    "date_part",
    "date_trunc",
    "datepart",
    "datetrunc",
    "from_unixtime",
    "now",
    "to_timestamp",
    "to_timestamp_micros",
    "to_timestamp_millis",
    "to_timestamp_seconds",
];

const SQL_INFO_SYSTEM_FUNCTIONS: &[&str] = &["array", "arrow_typeof", "struct"];

static SQL_DATA_TYPE_TO_ARROW_DATA_TYPE: Lazy<HashMap<SqlSupportsConvert, DataType>> =
    Lazy::new(|| {
        [
            // Referenced from DataFusion data types
            // https://arrow.apache.org/datafusion/user-guide/sql/data_types.html
            // Some SQL types are not supported by DataFusion
            // https://arrow.apache.org/datafusion/user-guide/sql/data_types.html#unsupported-sql-types
            (SqlSupportsConvert::SqlConvertBigint, DataType::Int64),
            // SqlSupportsConvert::SqlConvertBinary is not supported
            (SqlSupportsConvert::SqlConvertBit, DataType::Boolean),
            (SqlSupportsConvert::SqlConvertChar, DataType::Utf8),
            (SqlSupportsConvert::SqlConvertDate, DataType::Date32),
            (
                SqlSupportsConvert::SqlConvertDecimal,
                // Use the max precision 38
                // https://docs.rs/arrow-schema/47.0.0/arrow_schema/constant.DECIMAL128_MAX_PRECISION.html
                DataType::Decimal128(38, 2),
            ),
            (SqlSupportsConvert::SqlConvertFloat, DataType::Float32),
            (SqlSupportsConvert::SqlConvertInteger, DataType::Int32),
            (
                SqlSupportsConvert::SqlConvertIntervalDayTime,
                DataType::Interval(IntervalUnit::DayTime),
            ),
            (
                SqlSupportsConvert::SqlConvertIntervalYearMonth,
                DataType::Interval(IntervalUnit::YearMonth),
            ),
            // SqlSupportsConvert::SqlConvertLongvarbinary is not supported
            // LONG VARCHAR is identical to VARCHAR
            // https://docs.oracle.com/javadb/10.6.2.1/ref/rrefsqlj15147.html
            (SqlSupportsConvert::SqlConvertLongvarchar, DataType::Utf8),
            // NUMERIC is a synonym for DECIMAL and behaves the same way
            // https://docs.oracle.com/javadb/10.6.2.1/ref/rrefsqlj12362.html
            (
                SqlSupportsConvert::SqlConvertNumeric,
                // Use the max precision 38
                // https://docs.rs/arrow-schema/47.0.0/arrow_schema/constant.DECIMAL128_MAX_PRECISION.html
                DataType::Decimal128(38, 2),
            ),
            (SqlSupportsConvert::SqlConvertReal, DataType::Float32),
            (SqlSupportsConvert::SqlConvertSmallint, DataType::Int16),
            (
                SqlSupportsConvert::SqlConvertTime,
                DataType::Time64(TimeUnit::Nanosecond),
            ),
            (
                SqlSupportsConvert::SqlConvertTimestamp,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (SqlSupportsConvert::SqlConvertTinyint, DataType::Int8),
            // SqlSupportsConvert::SqlConvertVarbinary is not supported
            (SqlSupportsConvert::SqlConvertVarchar, DataType::Utf8),
        ]
        .iter()
        .cloned()
        .collect()
    });

pub(crate) static SQL_INFO_SUPPORTS_CONVERT: Lazy<HashMap<i32, Vec<i32>>> = Lazy::new(|| {
    let mut convert: HashMap<i32, Vec<i32>> = HashMap::new();
    for (from_type_sql, from_type_arrow) in SQL_DATA_TYPE_TO_ARROW_DATA_TYPE.clone() {
        let mut can_convert_to: Vec<i32> = vec![];
        for (to_type_sql, to_type_arrow) in SQL_DATA_TYPE_TO_ARROW_DATA_TYPE.clone() {
            if can_cast_types(&from_type_arrow, &to_type_arrow) {
                can_convert_to.push(to_type_sql as i32);
            }
        }
        if !can_convert_to.is_empty() {
            convert.insert(from_type_sql as i32, can_convert_to);
        }
    }
    convert
});

#[allow(non_snake_case)]
static INSTANCE: Lazy<SqlInfoData> = Lazy::new(|| {
    // The following are not defined in the [`SqlInfo`], but are
    // documented at
    // https://arrow.apache.org/docs/format/FlightSql.html#protocol-buffer-definitions.

    let SqlInfoFlightSqlServerSql = 4;
    let SqlInfoFlightSqlServerSubstrait = 5;
    let SqlInfoFlightSqlServerTransaction = 8;
    let SqlInfoFlightSqlServerCancel = 9;
    let SqlInfoFlightSqlServerStatementTimeout = 100;
    let SqlInfoFlightSqlServerTransactionTimeout = 101;

    // Copied from https://github.com/influxdata/idpe/blob/85aa7a52b40f173cc4d79ac02b3a4a13e82333c4/queryrouter/internal/server/flightsql_handler.go#L208-L275

    let mut builder = SqlInfoDataBuilder::new();

    // Server information
    builder.append(SqlInfo::FlightSqlServerName, "Spice.ai");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    // 1.3 comes from https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/Schema.fbs#L24
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.append(SqlInfo::FlightSqlServerReadOnly, true);
    builder.append(SqlInfoFlightSqlServerSql, true);
    builder.append(SqlInfoFlightSqlServerSubstrait, false);
    builder.append(
        SqlInfoFlightSqlServerTransaction,
        SqlSupportedTransactions::SqlTransactionUnspecified as i32,
    );
    // don't yet support `CancelQuery` action
    builder.append(SqlInfoFlightSqlServerCancel, false);
    builder.append(SqlInfoFlightSqlServerStatementTimeout, 0i32);
    builder.append(SqlInfoFlightSqlServerTransactionTimeout, 0i32);
    // SQL syntax information
    builder.append(SqlInfo::SqlDdlCatalog, false);
    builder.append(SqlInfo::SqlDdlSchema, false);
    builder.append(SqlInfo::SqlDdlTable, false);
    builder.append(
        SqlInfo::SqlIdentifierCase,
        SqlSupportedCaseSensitivity::SqlCaseSensitivityLowercase as i32,
    );
    builder.append(SqlInfo::SqlIdentifierQuoteChar, r#"""#);
    builder.append(
        SqlInfo::SqlQuotedIdentifierCase,
        SqlSupportedCaseSensitivity::SqlCaseSensitivityCaseInsensitive as i32,
    );
    builder.append(SqlInfo::SqlAllTablesAreSelectable, true);
    builder.append(
        SqlInfo::SqlNullOrdering,
        SqlNullOrdering::SqlNullsSortedHigh as i32,
    );
    builder.append(SqlInfo::SqlKeywords, SQL_INFO_SQL_KEYWORDS);
    builder.append(SqlInfo::SqlNumericFunctions, SQL_INFO_NUMERIC_FUNCTIONS);
    builder.append(SqlInfo::SqlStringFunctions, SQL_INFO_STRING_FUNCTIONS);
    builder.append(SqlInfo::SqlSystemFunctions, SQL_INFO_SYSTEM_FUNCTIONS);
    builder.append(SqlInfo::SqlDatetimeFunctions, SQL_INFO_DATE_TIME_FUNCTIONS);
    builder.append(SqlInfo::SqlSearchStringEscape, "\\");
    builder.append(SqlInfo::SqlExtraNameCharacters, "");
    builder.append(SqlInfo::SqlSupportsColumnAliasing, true);
    builder.append(SqlInfo::SqlNullPlusNullIsNull, true);
    builder.append(
        SqlInfo::SqlSupportsConvert,
        SQL_INFO_SUPPORTS_CONVERT.clone(),
    );
    builder.append(SqlInfo::SqlSupportsTableCorrelationNames, false);
    builder.append(SqlInfo::SqlSupportsDifferentTableCorrelationNames, false);
    builder.append(SqlInfo::SqlSupportsExpressionsInOrderBy, true);
    builder.append(SqlInfo::SqlSupportsOrderByUnrelated, true);
    builder.append(SqlInfo::SqlSupportedGroupBy, 3i32);
    builder.append(SqlInfo::SqlSupportsLikeEscapeClause, true);
    builder.append(SqlInfo::SqlSupportsNonNullableColumns, true);
    builder.append(
        SqlInfo::SqlSupportedGrammar,
        SupportedSqlGrammar::SqlCoreGrammar as i32,
    );
    // report IOx supports all ansi 92
    builder.append(SqlInfo::SqlAnsi92SupportedLevel, 0b111_i32);
    builder.append(SqlInfo::SqlSupportsIntegrityEnhancementFacility, false);
    builder.append(SqlInfo::SqlOuterJoinsSupportLevel, 2i32);
    builder.append(SqlInfo::SqlSchemaTerm, "schema");
    builder.append(SqlInfo::SqlProcedureTerm, "procedure");
    builder.append(SqlInfo::SqlCatalogAtStart, false);
    builder.append(SqlInfo::SqlSchemasSupportedActions, 0i32);
    builder.append(SqlInfo::SqlCatalogsSupportedActions, 0i32);
    builder.append(SqlInfo::SqlSupportedPositionedCommands, 0i32);
    builder.append(SqlInfo::SqlSelectForUpdateSupported, false);
    builder.append(SqlInfo::SqlStoredProceduresSupported, false);
    builder.append(SqlInfo::SqlSupportedSubqueries, 15i32);
    builder.append(SqlInfo::SqlCorrelatedSubqueriesSupported, true);
    builder.append(SqlInfo::SqlSupportedUnions, 3i32);
    builder.append(SqlInfo::SqlMaxBinaryLiteralLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxCharLiteralLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnsInGroupBy, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnsInIndex, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnsInOrderBy, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnsInSelect, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnsInTable, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxConnections, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxCursorNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxIndexLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlDbSchemaNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxProcedureNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxCatalogNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxRowSize, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxRowSizeIncludesBlobs, true);
    builder.append(SqlInfo::SqlMaxStatementLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxStatements, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxTableNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxTablesInSelect, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxUsernameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlDefaultTransactionIsolation, 0i64);
    builder.append(SqlInfo::SqlTransactionsSupported, false);
    builder.append(SqlInfo::SqlSupportedTransactionsIsolationLevels, 0i32);
    builder.append(SqlInfo::SqlDataDefinitionCausesTransactionCommit, false);
    builder.append(SqlInfo::SqlDataDefinitionsInTransactionsIgnored, true);
    builder.append(SqlInfo::SqlSupportedResultSetTypes, 0i32);
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetUnspecified,
        0i32,
    );
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetForwardOnly,
        0i32,
    );
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetScrollSensitive,
        0i32,
    );
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetScrollInsensitive,
        0i32,
    );
    builder.append(SqlInfo::SqlBatchUpdatesSupported, false);
    builder.append(SqlInfo::SqlSavepointsSupported, false);
    builder.append(SqlInfo::SqlNamedParametersSupported, false);
    builder.append(SqlInfo::SqlLocatorsUpdateCopy, false);
    builder.append(SqlInfo::SqlStoredFunctionsUsingCallSyntaxSupported, false);

    match builder.build() {
        Ok(data) => data,
        Err(e) => panic!("Error building SqlInfoData: {e}"),
    }
});

/// Return a [`SqlInfoData`] that describes Spice's capablities
pub(crate) fn get_sql_info_data() -> &'static SqlInfoData {
    &INSTANCE
}
