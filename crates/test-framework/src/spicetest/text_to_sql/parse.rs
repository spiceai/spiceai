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

use arrow::datatypes::{Field, Schema};
use datafusion::{common::Column, sql::TableReference};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashSet;

// Fetches the logical plan by running `EXPLAIN FORMAT PGJSON <sql>` against `v1/sql`.
//
// Returns the logical plan in PGJSON format.
pub async fn logical_plan(
    http_client: Client,
    http_base_url: &str,
    sql: &str,
) -> Result<Value, anyhow::Error> {
    let url = format!("{http_base_url}/v1/sql");

    let response = http_client
        .post(&url)
        .body(format!("EXPLAIN FORMAT PGJSON {sql}"))
        .header("Content-Type", "text/plain")
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

    Ok(root_plan.clone())
}

pub fn extract_tables_and_projection(
    logical_plan: &Value,
) -> (HashSet<TableReference>, HashSet<Column>) {
    let mut tables = HashSet::new();
    let mut projections = HashSet::new();

    extract_table_scans(logical_plan, &mut tables, &mut projections);

    (tables, projections)
}

fn extract_table_scans(
    plan: &Value,
    tables: &mut HashSet<TableReference>,
    projections: &mut HashSet<Column>,
) {
    if let Some(node_type) = plan.get("Node Type").and_then(Value::as_str)
        && node_type == "TableScan"
    {
        let mut relation_opt: Option<TableReference> = None;
        if let Some(relation_name) = plan.get("Relation Name").and_then(Value::as_str) {
            let tbl = TableReference::parse_str(relation_name);
            tables.insert(tbl.clone());
            relation_opt = Some(tbl);
        }
        if let Some(output) = plan.get("Output").and_then(Value::as_array) {
            for col in output {
                if let Some(col_str) = col.as_str() {
                    projections.insert(Column::new(relation_opt.clone(), col_str));
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

/// Finds the SQL query's schema by running the SQL against `v1/sql` endpoint.
///
/// Attempts to use more efficient SQL with equivalent outputs.
pub async fn sql_schema(
    http_client: Client,
    http_base_url: &str,
    sql: &str,
) -> Result<Schema, anyhow::Error> {
    let url = format!("{http_base_url}/v1/sql");

    let response = http_client
        .post(&url)
        .body(format!("SELECT * FROM ({sql}) LIMIT 1"))
        .header("Content-Type", "text/plain")
        .header("Accept", "application/vnd.spiceai.nsql.v1+json")
        .send()
        .await?;

    let mut json: Value = response.json().await?;
    let Some(schema) = json.get_mut("schema") else {
        return Err(anyhow::anyhow!("Failed to extract schema from response"));
    };
    let Some(f) = schema.get_mut("fields") else {
        return Err(anyhow::anyhow!("Failed to extract fields from schema"));
    };
    let fields: Vec<Field> = serde_json::from_value(f.take())
        .map_err(|e| anyhow::anyhow!("Failed to deserialize fields from schema: {e}"))?;
    Ok(Schema::new(fields))
}
