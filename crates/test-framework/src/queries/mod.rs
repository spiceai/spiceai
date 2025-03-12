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

use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Deserialize, Serialize, Default)]
pub enum QuerySet {
    #[default]
    #[serde(rename = "tpch")]
    Tpch,
    #[serde(rename = "tpcds")]
    Tpcds,
    #[serde(rename = "clickbench")]
    Clickbench,
}

#[derive(Debug, Clone, Copy)]
pub struct TableWithTimeColumn {
    pub name: &'static str,
    pub column: &'static str,
}

impl From<&(&'static str, &'static str)> for TableWithTimeColumn {
    fn from((name, column): &(&'static str, &'static str)) -> Self {
        Self { name, column }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TableWithRowCount {
    pub name: &'static str,
    pub count: u32,
}

impl From<&(&'static str, u32)> for TableWithRowCount {
    fn from((name, count): &(&'static str, u32)) -> Self {
        Self {
            name,
            count: *count,
        }
    }
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

    /// At scale factor 1, how many rows should be present in each table for the query set
    #[must_use]
    pub fn row_counts(&self) -> Vec<TableWithRowCount> {
        match self {
            QuerySet::Tpch => [
                ("customer", 150_000),
                ("lineitem", 6_001_215),
                ("nation", 25),
                ("orders", 1_500_000),
                ("part", 200_000),
                ("partsupp", 800_000),
                ("region", 5),
                ("supplier", 10_000),
            ]
            .iter()
            .map(TableWithRowCount::from)
            .collect(),
            QuerySet::Tpcds => [
                ("call_center", 6),
                ("catalog_page", 1_000),
                ("catalog_sales", 144_000),
                ("catalog_returns", 72_000),
                ("income_band", 20),
                ("inventory", 11_000),
                ("store_sales", 144_000),
                ("store_returns", 72_000),
                ("web_sales", 144_000),
                ("web_returns", 72_000),
                ("customer", 500_000),
                ("customer_address", 150_000),
                ("customer_demographics", 192_080),
                ("date_dim", 73_000),
                ("household_demographics", 7200),
                ("item", 18_000),
                ("promotion", 300),
                ("reason", 35),
                ("ship_mode", 20),
                ("store", 1_000),
                ("time_dim", 86_400),
                ("warehouse", 5),
                ("web_page", 1_000),
                ("web_site", 1_000),
            ]
            .iter()
            .map(TableWithRowCount::from)
            .collect(),
            QuerySet::Clickbench => [("hits_delayed", 40_000_000)]
                .iter()
                .map(TableWithRowCount::from)
                .collect(),
        }
    }

    #[must_use]
    pub fn append_time_columns(&self) -> Vec<TableWithTimeColumn> {
        match self {
            QuerySet::Tpch => [
                ("customer", "c_created_at"),
                ("lineitem", "l_created_at"),
                ("nation", "n_created_at"),
                ("orders", "o_created_at"),
                ("part", "p_created_at"),
                ("partsupp", "ps_created_at"),
                ("region", "r_created_at"),
                ("supplier", "s_created_at"),
            ]
            .iter()
            .map(TableWithTimeColumn::from)
            .collect(),
            QuerySet::Tpcds => [
                ("call_center", "cc_created_at"),
                ("catalog_page", "cp_created_at"),
                ("catalog_sales", "cs_created_at"),
                ("catalog_returns", "cr_created_at"),
                ("income_band", "ib_created_at"),
                ("inventory", "i_created_at"),
                ("store_sales", "ss_created_at"),
                ("store_returns", "sr_created_at"),
                ("web_sales", "ws_created_at"),
                ("web_returns", "wr_created_at"),
                ("customer", "c_created_at"),
                ("customer_address", "ca_created_at"),
                ("customer_demographics", "cd_created_at"),
                ("date_dim", "d_created_at"),
                ("household_demographics", "hd_created_at"),
                ("item", "i_created_at"),
                ("promotion", "p_created_at"),
                ("reason", "r_created_at"),
                ("ship_mode", "sm_created_at"),
                ("store", "s_created_at"),
                ("time_dim", "t_created_at"),
                ("warehouse", "w_created_at"),
                ("web_page", "wp_created_at"),
                ("web_site", "ws_created_at"),
            ]
            .iter()
            .map(TableWithTimeColumn::from)
            .collect(),
            QuerySet::Clickbench => [("hits_delayed", "created_at")]
                .iter()
                .map(TableWithTimeColumn::from)
                .collect(),
        }
    }
}

impl Display for QuerySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuerySet::Tpch => write!(f, "tpch"),
            QuerySet::Tpcds => write!(f, "tpcds"),
            QuerySet::Clickbench => write!(f, "clickbench"),
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
        q22, simple_q1, simple_q2, simple_q3, simple_q4, simple_q5, simple_q6, simple_q7
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
