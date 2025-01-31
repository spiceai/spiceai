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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum QuerySet {
    #[serde(rename = "tpch")]
    Tpch,
    #[serde(rename = "tpcds")]
    Tpcds,
    #[serde(rename = "clickbench")]
    Clickbench,
}

impl QuerySet {
    #[must_use]
    pub fn get_queries(
        &self,
        overrides: Option<QueryOverrides>,
    ) -> Vec<(&'static str, &'static str)> {
        match self {
            QuerySet::Tpch => get_tpch_test_queries(overrides),
            QuerySet::Tpcds => get_tpcds_test_queries(overrides),
            QuerySet::Clickbench => get_clickbench_test_queries(overrides),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum QueryOverrides {
    SQLite,
    PostgreSQL,
    MySQL,
    Dremio,
    Spark,
    ODBCAthena,
    DuckDB,
    Snowflake,
    IcebergSF1,
    SpicecloudCatalog,
}

impl QueryOverrides {
    #[must_use]
    pub fn from_engine(engine: &str) -> Option<Self> {
        match engine {
            "sqlite" => Some(Self::SQLite),
            "postgres" => Some(Self::PostgreSQL),
            "mysql" => Some(Self::MySQL),
            "dremio" => Some(Self::Dremio),
            "spark" => Some(Self::Spark),
            "odbc_athena" => Some(Self::ODBCAthena),
            "duckdb" => Some(Self::DuckDB),
            _ => None,
        }
    }
}

#[macro_export]
macro_rules! generate_tpch_queries {
    ( $( $i:tt ),* ) => {
        vec![
            $(
                (
                    concat!("tpch_", stringify!($i)),
                    include_str!(concat!("./tpch/", stringify!($i), ".sql"))
                )
            ),*
        ]
    }
}

#[macro_export]
macro_rules! generate_tpch_queries_override {
    ( $override:expr, $( $i:tt ),* ) => {
        vec![
            $(
                (
                    concat!("tpch_", stringify!($i)),
                    include_str!(concat!("./tpch/", $override, "/", stringify!($i), ".sql"))
                )
            ),*
        ]
    }
}

#[macro_export]
macro_rules! remove_tpch_query {
    ( $queries:expr, $( $i:literal ),* ) => {
        {
            let query_names: Vec<&str> = vec![ $( concat!("tpch_q", stringify!($i)), )* ];
            $queries.into_iter()
                .filter(|(name, _)| !query_names.contains(name))
                .collect()
        }
    };
}

#[allow(clippy::too_many_lines)]
#[must_use]
pub fn get_tpch_test_queries(
    overrides: Option<QueryOverrides>,
) -> Vec<(&'static str, &'static str)> {
    let queries = generate_tpch_queries!(
        q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q13, q14, q16, q17, q18, q19, q20, q21,
        q22, simple_q1, simple_q2, simple_q3, simple_q4, simple_q5
    );

    match overrides {
        Some(QueryOverrides::ODBCAthena) => remove_tpch_query!(
            queries, 4,  // https://github.com/spiceai/spiceai/issues/2077
            20  // https://github.com/spiceai/spiceai/issues/2078
        ),
        Some(QueryOverrides::Spark) => remove_tpch_query!(
            queries,
            2, // Analysis error: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY] Unsupported subquery expression: Correlated scalar subqueries can only be used in filters, aggregations, projections, and UPDATE/MERGE/DELETE commands
            17 // Analysis error: [UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY] Unsupported subquery expression: Correlated scalar subqueries can only be used in filters, aggregations, projections, and UPDATE/MERGE/DELETE commands
        ),
        Some(QueryOverrides::Snowflake) => generate_tpch_queries_override!(
            "snowflake",
            q1,
            q2,
            q3,
            q4,
            q5,
            q6,
            q7,
            q8,
            q9,
            q10,
            q11,
            q12,
            q13,
            q14,
            q16,
            q17,
            q18,
            q19,
            q20,
            q21,
            q22,
            simple_q1,
            simple_q2,
            simple_q3,
            simple_q4,
            simple_q5,
            simple_q6,
            simple_q7
        ),
        Some(QueryOverrides::IcebergSF1) => generate_tpch_queries_override!(
            "iceberg_sf1",
            q1,
            q2,
            q3,
            q4,
            q5,
            q6,
            q7,
            q8,
            q9,
            q10,
            q11,
            q12,
            q13,
            q14,
            q16,
            q17,
            q18,
            q19,
            q20,
            q21,
            q22,
            simple_q1,
            simple_q2,
            simple_q3,
            simple_q4,
            simple_q5,
            simple_q6,
            simple_q7
        ),
        Some(QueryOverrides::SpicecloudCatalog) => generate_tpch_queries_override!(
            "spicecloud_catalog",
            q1,
            q2,
            q3,
            q4,
            q5,
            q6,
            q7,
            q8,
            q9,
            q10,
            q11,
            q12,
            q13,
            q14,
            q16,
            q17,
            q18,
            q19,
            q20,
            q21,
            q22,
            simple_q1,
            simple_q2,
            simple_q3,
            simple_q4,
            simple_q5,
            simple_q6,
            simple_q7
        ),
        _ => queries,
    }
}

#[macro_export]
macro_rules! generate_tpcds_queries {
    ( $( $i:literal ),* ) => {
        vec![
            $(
                (
                    concat!("tpcds_q", stringify!($i)),
                    include_str!(concat!("./tpcds/q", stringify!($i), ".sql"))
                )
            ),*
        ]
    }
}

#[macro_export]
macro_rules! remove_tpcds_query {
    ( $queries:expr, $( $i:literal ),* ) => {
        {
            let query_names: Vec<&str> = vec![ $( concat!("tpcds_q", stringify!($i)), )* ];
            $queries.into_iter()
                .filter(|(name, _)| !query_names.contains(name))
                .collect()
        }
    };
}

#[macro_export]
macro_rules! add_tpcds_query_overrides {
    ( $queries:expr, $override:expr, $( $i:literal ),* ) => {
        {
            let mut queries = $queries;
            $(
                queries.push((
                    concat!("tpcds_q", stringify!($i)),
                    include_str!(concat!("./tpcds/", $override, "/q", stringify!($i), ".sql"))
                ));
            )*
            queries
        }
    }
}

#[must_use]
pub fn get_tpcds_test_queries(
    overrides: Option<QueryOverrides>,
) -> Vec<(&'static str, &'static str)> {
    let queries = generate_tpcds_queries!(
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 25, 26, 27, 28,
        29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
        53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75,
        76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98,
        99
    );
    // q14, q23, q24 and q39 removed by default as they contain multiple queries, which aren't supported

    match overrides {
        Some(QueryOverrides::DuckDB) => remove_tpcds_query!(
            queries, 8,  // EXCEPT and INTERSECT aren't supported
            38, // EXCEPT and INTERSECT aren't supported
            87  // EXCEPT and INTERSECT aren't supported
        ),
        Some(QueryOverrides::MySQL) => remove_tpcds_query!(
            queries, 8,  // EXCEPT and INTERSECT aren't supported
            38, // EXCEPT and INTERSECT aren't supported
            51, // MySQL does not support FULL JOIN
            87, // EXCEPT and INTERSECT aren't supported
            97  // MySQL does not support FULL JOIN
        ),
        Some(QueryOverrides::PostgreSQL) => {
            // Query 1, 30, 64, 81 commented out due to rewritten query's expensive plan in Postgres
            // Issue: https://github.com/spiceai/spiceai/issues/2939
            let queries: Vec<(&'static str, &'static str)> = remove_tpcds_query!(
                queries, 1, 8,  // EXCEPT and INTERSECT aren't supported
                4, // slow postgresql performance: https://www.postgresql.org/message-id/9A28C8860F777E439AA12E8AEA7694F801133F57%40BPXM15GP.gisp.nec.co.jp
                30, // https://github.com/spiceai/spiceai/issues/2939
                36, // overridden below
                38, // EXCEPT and INTERSECT aren't supported
                64, // https://github.com/spiceai/spiceai/issues/2939
                70, // overridden below
                81, // https://github.com/spiceai/spiceai/issues/2939
                86, // overridden below
                87  // EXCEPT and INTERSECT aren't supported
            );
            add_tpcds_query_overrides!(queries, "postgres", 36, 70, 86)
        }
        Some(_) | None => queries,
    }
}

macro_rules! generate_clickbench_queries {
  ( $( $i:literal ),* ) => {
      vec![
          $(
              (
                  concat!("clickbench_q", stringify!($i)),
                  include_str!(concat!("./clickbench/q", stringify!($i), ".sql"))
              )
          ),*
      ]
  }
}

macro_rules! generate_clickbench_query_overrides {
  ( $engine:expr, $( $i:literal ),* ) => {
      vec![
          $(
              (
                  concat!("clickbench_q", stringify!($i)),
                  include_str!(concat!("./clickbench/", $engine, "/q", stringify!($i), ".sql"))
              )
          ),*
      ]
  }
}

#[must_use]
pub fn get_clickbench_test_queries(
    overrides: Option<QueryOverrides>,
) -> Vec<(&'static str, &'static str)> {
    let mut queries = generate_clickbench_queries!(
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43
    );

    let overrides = match overrides {
        Some(QueryOverrides::SQLite) => {
            queries.remove(28); // q29 includes regexp_replace which is not supported by sqlite
            Some(generate_clickbench_query_overrides!(
                "sqlite", 7, 19, 24, 25, 27, 37, 38, 39, 40, 41, 42, 43
            ))
        }
        Some(QueryOverrides::PostgreSQL) => {
            // Column aliases cannot appear with expressions in ORDER BY in Postgres: https://www.postgresql.org/docs/current/queries-order.html
            // expressions can appear with other expressions, so re-write the query to fit
            Some(generate_clickbench_query_overrides!("postgres", 43))
        }
        Some(QueryOverrides::Dremio) => {
            // Column aliases cannot appear with expressions in ORDER BY in Postgres: https://www.postgresql.org/docs/current/queries-order.html
            // expressions can appear with other expressions, so re-write the query to fit
            Some(generate_clickbench_query_overrides!(
                "dremio", 21, 22, 23, 24
            ))
        }
        Some(QueryOverrides::DuckDB) => {
            // specific to the DuckDB accelerator when used with on_zero_results: use_source
            // the unparser does not support binary scalar literals, so cast the binary columns to text
            Some(generate_clickbench_query_overrides!(
                "duckdb", 11, 12, 13, 14, 15, 22, 23, 25, 26, 27, 28, 29, 31, 32, 37, 38
            ))
        }
        _ => None,
    };

    // replace queries with overrides based on their filename matches
    if let Some(overrides) = overrides {
        for (key, value) in overrides {
            if let Some(query) = queries.iter_mut().find(|(k, _)| *k == key) {
                *query = (key, value);
            }
        }
    }

    queries
}
