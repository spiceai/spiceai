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

use std::collections::HashSet;

use datafusion::sql::sqlparser::{
    ast::{Expr, Query, SelectItem, SetExpr, Statement, TableFactor},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use reqwest::Client;
use serde_json::{Value, json};

pub async fn tables_and_projection(
    http_client: Client,
    http_base_url: impl Into<String>,
    sql: &str,
) -> Result<(Vec<String>, Vec<String>), anyhow::Error> {
    let url = format!("{}/v1/sql", http_base_url.into());

    let response = http_client
        .post(&url)
        .header("Content-Type", "application/vnd.spiceai.nsql.v1+json")
        .body(
            serde_json::to_string(&json!({
                "sql": format!("EXPLAIN FORMAT PGJSON {sql}"),
                "parameters": [],
            }))
            .map_err(|e| anyhow::anyhow!("Failed to serialize request body: {}", e))?,
        )
        .send()
        .await?;

    let json: Vec<Value> = response.json().await?;
    let plan_str = json
        .first()
        .and_then(|v| v.get("plan"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("Failed to extract plan from response"))?;

    let plan: Vec<Value> = serde_json::from_str(plan_str)?;
    let root_plan = plan
        .first()
        .and_then(|v| v.get("Plan"))
        .ok_or_else(|| anyhow::anyhow!("Failed to extract Plan from response"))?;

    let mut tables = HashSet::new();
    let mut projections = HashSet::new();

    extract_table_scans(root_plan, &mut tables, &mut projections);

    Ok((
        tables.into_iter().collect(),
        projections.into_iter().collect(),
    ))
}

fn extract_table_scans(
    plan: &Value,
    tables: &mut HashSet<String>,
    projections: &mut HashSet<String>,
) {
    if let Some(node_type) = plan.get("Node Type").and_then(Value::as_str) {
        if node_type == "TableScan" {
            if let Some(relation_name) = plan.get("Relation Name").and_then(Value::as_str) {
                tables.insert(relation_name.to_string());
            }
            if let Some(output) = plan.get("Output").and_then(Value::as_array) {
                for col in output {
                    if let Some(col_str) = col.as_str() {
                        projections.insert(col_str.to_string());
                    }
                }
            }
        }
    }

    if let Some(plans) = plan.get("Plans").and_then(Value::as_array) {
        for sub_plan in plans {
            extract_table_scans(sub_plan, tables, projections);
        }
    }
}
