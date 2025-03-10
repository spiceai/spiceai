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

use std::time::{Duration, Instant};

use anyhow::Result;
use duckdb::Connection;
use tokio::task::JoinHandle;

use crate::queries::QuerySet;

pub(crate) struct AppendWorker {
    id: usize,
    _end_duration: Duration,
    query_set: QuerySet,
}

impl AppendWorker {
    pub fn new(id: usize, end_duration: Duration, query_set: QuerySet) -> Self {
        Self {
            id,
            _end_duration: end_duration,
            query_set,
        }
    }

    pub fn start(self) -> Result<JoinHandle<Result<()>>> {
        // Outside of the join handle, run some initial setup
        // This ensures the appendable dataset is ready before the workers start
        println!("Running append data setup");
        let dest_db_file = "./.data/tpch_append.db";
        if std::fs::exists("./.data/tpch_append.db")? {
            std::fs::remove_file("./.data/tpch_append.db")?;
        }

        let tables = match self.query_set {
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
            .to_vec(),
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
            .to_vec(),
            QuerySet::Clickbench => vec![("hits_delayed", "created_at")],
        };

        for (table, _) in &tables {
            if std::fs::exists(format!("./.data/{table}.parquet"))? {
                std::fs::remove_file(format!("./.data/{table}.parquet"))?;
            }
        }

        let load_index = 0;
        let dest_conn = Connection::open(dest_db_file)?;
        println!(
            "Loading initial data for {} benchmark suite",
            self.query_set
        );
        match self.query_set {
            QuerySet::Tpch => {
                let mut sql = format!(
                    "
                INSTALL tpch;
                LOAD tpch;
                BEGIN;
                CALL dbgen(sf=1, children=10, step={load_index}, suffix={suffix});
                ",
                    suffix = if load_index == 0 { "''" } else { "'_new'" },
                );

                for (table, column) in &tables {
                    if load_index == 0 {
                        sql += &format!(
                            "
                        ALTER TABLE {table} ADD COLUMN {column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                        COPY {table} TO '.data/{table}.parquet' (FORMAT 'parquet');
                        "
                        );
                    } else {
                        sql += &format!("
                        ALTER TABLE {table}_new ADD COLUMN {column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                        INSERT INTO {table} SELECT * FROM {table}_new;
                        DROP TABLE {table}_new;
                        COPY {table} TO '.data/{table}.parquet' (FORMAT 'parquet');
                        ");
                    }
                }

                sql += "COMMIT;";

                dest_conn.execute_batch(&sql)?;
            }
            _ => {
                todo!("Implement TPCDS and ClickBench");
            }
        }

        // for now, just wait a bit and exit
        Ok(tokio::spawn(async move {
            // pretend to fail after 10 seconds
            let end_time = Instant::now() + Duration::from_secs(10);
            while Instant::now() < end_time {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(anyhow::anyhow!("Worker {} failed", self.id))
        }))
    }
}
