/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! CLI-backed streaming sources with isolated table or topic namespaces.

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use serde_json::{Value, json};

use super::{
    Namespace, Source, TextResult,
    generic::Generic,
    write::{RowChange, row_changes, table_parts},
};
use crate::{
    docker::Container,
    generate::{ColumnKind, Dialect, TableSpec, WriteMode},
    script::SourceSpec,
};

pub struct Document {
    inner: Generic,
    kind: String,
    tables: tokio::sync::Mutex<std::collections::HashSet<String>>,
}

impl Document {
    pub async fn start(
        name: &str,
        run_id: &str,
        kind: &str,
        version: Option<&str>,
    ) -> Result<Self> {
        let spec = match kind {
            "kafka" => json!({
                "kind": "docker", "image": format!("docker.redpanda.com/redpandadata/redpanda:{}", version.unwrap_or("v26.1.6")), "ports": [19092],
                "command": ["redpanda", "start", "--smp", "1", "--overprovisioned", "--node-id", "0", "--mode", "dev-container", "--kafka-addr", "internal://0.0.0.0:9092,external://0.0.0.0:19092", "--advertise-kafka-addr", "internal://127.0.0.1:9092,external://127.0.0.1:${port.19092}"],
                "ready": "rpk cluster health | grep -E 'Healthy:.+true'"
            }),
            "mongodb" => json!({
                "kind": "docker", "image": format!("docker.io/library/mongo:{}", version.unwrap_or("8.0")), "ports": [27017],
                "command": ["mongod", "--replSet", "rs0", "--bind_ip_all"],
                "ready": "mongosh --quiet --eval \"try { rs.status(); db.hello().isWritablePrimary ? 1 : 0 } catch (e) { rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]}); 0 }\" | grep -qx 1"
            }),
            "dynamodb" => json!({
                "kind": "docker", "image": format!("docker.io/localstack/localstack:{}", version.unwrap_or("4.9")), "ports": [4566],
                "env": {"SERVICES": "dynamodb", "AWS_DEFAULT_REGION": "us-east-1"}, "ready": "awslocal dynamodb list-tables"
            }),
            _ => bail!("unknown document source '{kind}'"),
        };
        let spec: SourceSpec = serde_json::from_value(spec)?;
        Ok(Self {
            inner: Generic::start(name, run_id, &spec).await?,
            kind: kind.into(),
            tables: tokio::sync::Mutex::new(std::collections::HashSet::new()),
        })
    }

    fn container_ref(&self) -> Result<&Container> {
        self.inner
            .container()
            .context("document source has no container")
    }

    fn table_name(&self, namespace: &str, table: &str) -> Result<String> {
        let parts = table_parts(table)?;
        ensure!(
            parts.len() == 1,
            "{} writes require an unqualified table name",
            self.kind
        );
        let table = parts.first().context("empty table name")?;
        Ok(if self.kind == "mongodb" {
            table.clone()
        } else {
            format!("{namespace}_{table}")
        })
    }

    async fn input(&self, cmd: &[&str], input: Vec<u8>) -> Result<()> {
        self.container_ref()?
            .exec_with_input(cmd, &mut std::iter::once(Ok(input)))
            .await?;
        Ok(())
    }

    async fn kafka(&self, table: &str, key: &str, changes: &[RowChange]) -> Result<()> {
        let mut tables = self.tables.lock().await;
        if !tables.contains(table) {
            let output = self
                .container_ref()?
                .exec(&["rpk", "topic", "create", table, "-p", "3"])
                .await?;
            ensure!(
                !output.contains("ERROR"),
                "Kafka topic creation failed: {output}"
            );
            tables.insert(table.into());
        }
        drop(tables);
        // Length framing keeps keys containing tabs, newlines or quotes intact.
        let mut input = Vec::new();
        for change in changes {
            let (key, value) = match change {
                RowChange::Upsert(doc) => (
                    doc.get(key).context("missing Kafka key")?,
                    serde_json::to_vec(doc)?,
                ),
                RowChange::Delete(key) => (key, Vec::new()),
            };
            let key = match key {
                Value::String(v) => v.clone(),
                other => other.to_string(),
            };
            input.extend_from_slice(&u32::try_from(key.len())?.to_be_bytes());
            input.extend_from_slice(&u32::try_from(value.len())?.to_be_bytes());
            input.extend_from_slice(key.as_bytes());
            input.extend(value);
        }
        if !input.is_empty() {
            self.input(
                &[
                    "rpk",
                    "topic",
                    "produce",
                    table,
                    "--tombstone",
                    "-f",
                    "%K{big32}%V{big32}%k%v",
                ],
                input,
            )
            .await?;
        }
        Ok(())
    }
}

/// Extended JSON preserves integer precision when mongosh reads the batch.
fn mongo_value(value: &Value) -> Value {
    match value {
        Value::Number(number) => {
            if number.is_i64() {
                json!({"$numberLong": number.to_string()})
            } else {
                json!({"$numberDecimal": number.to_string()})
            }
        }
        Value::Array(values) => Value::Array(values.iter().map(mongo_value).collect()),
        Value::Object(doc) => Value::Object(
            doc.iter()
                .map(|(k, v)| (k.clone(), mongo_value(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

#[async_trait]
impl Source for Document {
    fn container(&self) -> Option<&Container> {
        self.inner.container()
    }

    async fn create_namespace(&self, namespace: &str) -> Result<Namespace> {
        let mut ns = self.inner.create_namespace(namespace).await?;
        let port = |p: u16| self.container_ref()?.host_port(p);
        ns.params = match self.kind.as_str() {
            "kafka" => {
                json!({"kafka_bootstrap_servers": format!("127.0.0.1:{}", port(19092)?), "kafka_security_protocol": "PLAINTEXT"})
            }
            "mongodb" => {
                json!({"mongodb_connection_string": format!("mongodb://127.0.0.1:{}/{namespace}?directConnection=true&replicaSet=rs0&tls=false", port(27017)?)})
            }
            _ => {
                json!({"dynamodb_aws_region": "us-east-1", "dynamodb_aws_auth": "key", "dynamodb_aws_access_key_id": "test", "dynamodb_aws_secret_access_key": "test", "endpoint_url": format!("http://127.0.0.1:{}", port(4566)?)})
            }
        };
        Ok(ns)
    }

    async fn exec(&self, namespace: &str, command: &str) -> Result<()> {
        self.inner.exec(namespace, command).await
    }
    async fn query(&self, namespace: &str, command: &str) -> Result<TextResult> {
        self.inner.query(namespace, command).await
    }
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }
    async fn load(
        &self,
        _namespace: &str,
        _table: &str,
        _columns: &[String],
        _rows: &mut (dyn Iterator<Item = Vec<Option<String>>> + Send),
    ) -> Result<u64> {
        bail!("{} load needs typed rows; use generate or write", self.kind)
    }

    async fn apply(
        &self,
        namespace: &str,
        table: &str,
        key: &str,
        changes: &[RowChange],
    ) -> Result<()> {
        let table = self.table_name(namespace, table)?;
        if self.kind == "kafka" {
            return self.kafka(&table, key, changes).await;
        }
        if changes.is_empty() {
            return Ok(());
        }
        let operations = changes
            .iter()
            .map(|change| match change {
                RowChange::Upsert(doc) => json!({"doc": doc}),
                RowChange::Delete(value) => json!({"delete": value}),
            })
            .collect::<Vec<_>>();
        let payload = json!({"table": table, "key": key, "operations": operations});
        if self.kind == "mongodb" {
            let payload = serde_json::to_string(&mongo_value(&payload))?;
            let program = format!(
                r"const p = EJSON.parse({}, {{relaxed: false}});
const operations = p.operations.map(o => {{
    if (Object.hasOwn(o, 'delete')) return {{deleteOne: {{filter: {{_id: o.delete}}}}}};
    const doc = o.doc;
    if (Object.hasOwn(doc, '_id') && EJSON.stringify(doc._id) !== EJSON.stringify(doc[p.key])) throw new Error('_id must equal the write key');
    doc._id = doc[p.key];
    return {{replaceOne: {{filter: {{_id: doc._id}}, replacement: doc, upsert: true}}}};
}});
db.getCollection(p.table).bulkWrite(operations, {{ordered: true}});
",
                serde_json::to_string(&payload)?
            );
            self.input(
                &["mongosh", "--quiet", namespace, "--file", "/dev/stdin"],
                program.into_bytes(),
            )
            .await
        } else {
            let program = r"import sys, json, boto3
from decimal import Decimal
p = json.load(sys.stdin, parse_float=Decimal)
d = boto3.resource('dynamodb', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test')
first = p['operations'][0]
k = first.get('delete', first.get('doc', {}).get(p['key']))
if isinstance(k, bool) or not isinstance(k, (str, int, Decimal)):
    raise ValueError('DynamoDB key must be numeric or string')
t = d.Table(p['table'])
try:
    t.load()
except d.meta.client.exceptions.ResourceNotFoundException:
    t = d.create_table(TableName=p['table'], KeySchema=[{'AttributeName': p['key'], 'KeyType': 'HASH'}], AttributeDefinitions=[{'AttributeName': p['key'], 'AttributeType': 'S' if isinstance(k, str) else 'N'}], BillingMode='PAY_PER_REQUEST', StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'})
    t.wait_until_exists()
# Preserve every operation, including repeated writes to the same key.
for o in p['operations']:
    if 'delete' in o:
        t.delete_item(Key={p['key']: o['delete']})
    else:
        t.put_item(Item=o['doc'])
";
            self.input(&["python3", "-c", program], serde_json::to_vec(&payload)?)
                .await
        }
    }

    async fn generate(&self, namespace: &str, spec: &TableSpec) -> Result<u64> {
        ensure!(
            spec.rows > 0,
            "{} generate needs at least one row; its schema is inferred from documents",
            self.kind
        );
        let key = spec.primary_key.as_deref().unwrap_or("id");
        ensure!(
            spec.columns.iter().any(|c| c.name == key),
            "{} generate needs key column '{key}'",
            self.kind
        );
        let table = self.table_name(namespace, &spec.table)?;
        if spec.mode == WriteMode::Replace {
            match self.kind.as_str() {
                "mongodb" => {
                    self.container_ref()?
                        .exec(&[
                            "mongosh",
                            "--quiet",
                            namespace,
                            "--eval",
                            &format!(
                                "db.getCollection({}).drop()",
                                serde_json::to_string(&table)?
                            ),
                        ])
                        .await?;
                }
                "kafka" => {
                    let mut tables = self.tables.lock().await;
                    if tables.remove(&table) {
                        self.container_ref()?
                            .exec(&["rpk", "topic", "delete", &table])
                            .await?;
                    }
                }
                _ => {
                    let program = r"import boto3, sys
d = boto3.resource('dynamodb', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test')
t = d.Table(sys.argv[1])
try:
    t.delete()
    t.wait_until_not_exists()
except d.meta.client.exceptions.ResourceNotFoundException:
    pass
";
                    self.container_ref()?
                        .exec(&["python3", "-c", program, &table])
                        .await?;
                }
            }
        }
        let types = spec
            .columns
            .iter()
            .map(|column| {
                (
                    column.name.clone(),
                    match column.spec.kind {
                        ColumnKind::Sequence(_) | ColumnKind::Bigint(_) => "int8",
                        ColumnKind::Int(_) => "int4",
                        ColumnKind::Float(_) => "float8",
                        ColumnKind::Decimal(_) => "numeric",
                        ColumnKind::Bool(_) => "bool",
                        _ => "text",
                    }
                    .into(),
                )
            })
            .collect::<Vec<_>>();
        let mut rows = crate::generate::rows(spec)?;
        let mut count = 0;
        loop {
            let batch = rows.by_ref().take(2000).collect::<Vec<_>>();
            if batch.is_empty() {
                break;
            }
            count += batch.len() as u64;
            let images = TextResult {
                columns: types.iter().map(|(name, _)| name.clone()).collect(),
                rows: batch,
            };
            let changes = row_changes(&images, &types, key, false)?;
            self.apply(namespace, &spec.table, key, &changes).await?;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::mongo_value;
    use serde_json::json;

    #[test]
    fn mongo_unsigned_integers_use_decimal_instead_of_wrapping() {
        assert_eq!(
            mongo_value(&json!([
                9_223_372_036_854_775_807_i64,
                9_223_372_036_854_775_808_u64,
                u64::MAX
            ])),
            json!([{"$numberLong": "9223372036854775807"}, {"$numberDecimal": "9223372036854775808"}, {"$numberDecimal": "18446744073709551615"}])
        );
    }

    #[test]
    fn mongo_batches_keep_numeric_types_inside_operation_arrays() {
        assert_eq!(
            mongo_value(
                &json!({"operations": [{"doc": {"id": 9_223_372_036_854_775_807_i64, "n": 1.25, "b": true, "s": "42", "v": null}}, {"delete": 7}]})
            ),
            json!({"operations": [{"doc": {"id": {"$numberLong": "9223372036854775807"}, "n": {"$numberDecimal": "1.25"}, "b": true, "s": "42", "v": null}}, {"delete": {"$numberLong": "7"}}]})
        );
    }
}
