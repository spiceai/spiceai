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

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::json;

use super::{Source, TextResult};
use crate::docker::{Container, ContainerSpec, healthcheck};

const PASSWORD: &str = "spicescript";

pub struct Mysql {
    container: Container,
}

impl Mysql {
    /// Starts MySQL. Row-based binary logging is on by default from 8.0, so
    /// the same source serves full, append and CDC refresh; GTIDs are on so
    /// the connector can checkpoint by GTID.
    pub async fn start(name: &str, run_id: &str, version: Option<&str>) -> Result<Self> {
        let container = Container::start(ContainerSpec {
            name: format!("spicescript-{run_id}-{name}"),
            image: super::image("mysql", version)?,
            env: vec![("MYSQL_ROOT_PASSWORD".into(), PASSWORD.into())],
            command: Some(
                [
                    "--gtid-mode=ON",
                    "--enforce-gtid-consistency=ON",
                    "--max-connections=500",
                ]
                .map(String::from)
                .to_vec(),
            ),
            ports: vec![3306],
            healthcheck: Some(healthcheck(&format!(
                "mysqladmin ping -h127.0.0.1 -uroot --password={PASSWORD} --silent"
            ))),
            network: None,
        })
        .await?;
        Ok(Self { container })
    }

    /// Runs the `mysql` client inside the container as root, against
    /// `database` when given, with `args` after the connection options.
    async fn mysql(&self, database: Option<&str>, args: &[&str]) -> Result<String> {
        let password = format!("--password={PASSWORD}");
        let mut cmd = vec![
            "mysql",
            "-uroot",
            password.as_str(),
            "--protocol=TCP",
            "-h127.0.0.1",
            "--default-character-set=utf8mb4",
        ];
        cmd.extend_from_slice(args);
        cmd.extend(database);
        self.container.exec(&cmd).await
    }
}

#[async_trait]
impl Source for Mysql {
    fn container(&self) -> Option<&Container> {
        Some(&self.container)
    }

    async fn create_namespace(&self, namespace: &str) -> Result<super::Namespace> {
        self.mysql(None, &["-e", &format!("CREATE DATABASE `{namespace}`")])
            .await
            .with_context(|| format!("Failed to create database '{namespace}'"))?;
        Ok(super::Namespace::params(json!({
            "mysql_host": "127.0.0.1",
            "mysql_tcp_port": self.container.host_port(3306)?.to_string(),
            "mysql_user": "root",
            "mysql_pass": PASSWORD,
            "mysql_db": namespace,
            "mysql_sslmode": "disabled",
        })))
    }

    /// `mysql -e` runs the statements in order in one session, so session
    /// settings (`SET SESSION …`) apply to the statements after them.
    async fn exec(&self, namespace: &str, sql: &str) -> Result<()> {
        self.mysql(Some(namespace), &["-e", sql])
            .await
            .with_context(|| format!("MySQL rejected: {sql}"))
            .map(|_| ())
    }

    /// The answer as the client's XML, which marks NULL apart from the text
    /// `NULL`, with binary values (and `BIT`) as `0x…` hex.
    async fn query(&self, namespace: &str, sql: &str) -> Result<TextResult> {
        let xml = self
            .mysql(Some(namespace), &["--xml", "--binary-as-hex", "-e", sql])
            .await
            .with_context(|| format!("MySQL rejected oracle query: {sql}"))?;
        if xml.matches("<resultset").count() != 1 {
            bail!("an oracle query must be one SELECT: {sql}");
        }
        parse_xml(&xml)
    }

    fn dialect(&self) -> crate::generate::Dialect {
        crate::generate::Dialect::Mysql
    }

    /// Multi-row INSERTs of 2,000 rows each, streamed to one `mysql` session.
    async fn load(
        &self,
        namespace: &str,
        table: &str,
        columns: &[String],
        rows: &mut (dyn Iterator<Item = Vec<Option<String>>> + Send),
    ) -> Result<u64> {
        let head = format!("INSERT INTO {table} ({}) VALUES ", columns.join(", "));
        let count = std::sync::atomic::AtomicU64::new(0);
        let mut statements = std::iter::from_fn(|| {
            let mut statement = head.clone();
            let mut batch = 0;
            for row in &mut *rows {
                if batch > 0 {
                    statement.push(',');
                }
                statement.push('(');
                for (i, cell) in row.iter().enumerate() {
                    if i > 0 {
                        statement.push(',');
                    }
                    match cell {
                        None => statement.push_str("NULL"),
                        Some(v) => {
                            statement.push('\'');
                            statement.push_str(&v.replace('\\', "\\\\").replace('\'', "''"));
                            statement.push('\'');
                        }
                    }
                }
                statement.push(')');
                batch += 1;
                if batch == 2000 {
                    break;
                }
            }
            count.fetch_add(batch, std::sync::atomic::Ordering::Relaxed);
            (batch > 0).then(|| Ok((statement + ";\n").into_bytes()))
        });
        let password = format!("--password={PASSWORD}");
        self.container
            .exec_with_input(
                &[
                    "mysql",
                    "-uroot",
                    &password,
                    "--protocol=TCP",
                    "-h127.0.0.1",
                    "--default-character-set=utf8mb4",
                    namespace,
                ],
                &mut statements,
            )
            .await
            .with_context(|| format!("Failed to load {table}"))?;
        Ok(count.into_inner())
    }
}

/// Reads the `mysql` client's `--xml` answer: a `<row>` per row and a
/// `<field name="…">` per cell, with `xsi:nil="true"` for NULL. An empty
/// answer names no columns.
fn parse_xml(xml: &str) -> Result<TextResult> {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for row in xml.split("<row>").skip(1) {
        let row = row.split("</row>").next().unwrap_or_default();
        let mut cells = Vec::new();
        let mut names = Vec::new();
        for field in row.split("<field name=\"").skip(1) {
            let (name, rest) = field
                .split_once('"')
                .with_context(|| format!("malformed field in MySQL XML: {field}"))?;
            names.push(unescape_xml(name));
            if rest.trim_start().starts_with("xsi:nil=\"true\"") {
                cells.push(None);
            } else {
                let value = rest
                    .strip_prefix('>')
                    .and_then(|v| v.rsplit_once("</field>"))
                    .map(|(v, _)| v)
                    .with_context(|| format!("malformed field in MySQL XML: {field}"))?;
                cells.push(Some(unescape_xml(value)));
            }
        }
        if columns.is_empty() {
            columns = names;
        }
        rows.push(cells);
    }
    Ok(TextResult { columns, rows })
}

fn unescape_xml(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        let entity_end = rest[at..].find(';').map(|e| at + e);
        let Some(end) = entity_end else {
            out.push_str(&rest[at..]);
            return out;
        };
        let entity = &rest[at + 1..end];
        let decoded = match entity {
            "lt" => Some('<'),
            "gt" => Some('>'),
            "amp" => Some('&'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => entity
                .strip_prefix("#x")
                .map(|h| u32::from_str_radix(h, 16))
                .or_else(|| entity.strip_prefix('#').map(str::parse::<u32>))
                .and_then(Result::ok)
                .and_then(char::from_u32),
        };
        match decoded {
            Some(c) => out.push(c),
            None => out.push_str(&rest[at..=end]),
        }
        rest = &rest[end + 1..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::parse_xml;

    #[test]
    fn xml_answer_keeps_null_apart_from_text_and_unescapes() {
        let xml = r#"<?xml version="1.0"?>

<resultset statement="SELECT * FROM t
" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <row>
	<field name="b">0x01</field>
	<field name="s">NULL</field>
	<field name="n" xsi:nil="true" />
	<field name="j">{&quot;a&quot;: &quot;&lt;x &amp; y&gt;&quot; &#65;}</field>
  </row>

  <row>
	<field name="b" xsi:nil="true" />
	<field name="s">line
break	tab</field>
	<field name="n">3</field>
	<field name="j"></field>
  </row>
</resultset>
"#;
        let parsed = parse_xml(xml).expect("valid XML");
        let s = |v: &str| Some(v.to_string());
        assert_eq!(parsed.columns, vec!["b", "s", "n", "j"]);
        assert_eq!(
            parsed.rows,
            vec![
                vec![s("0x01"), s("NULL"), None, s(r#"{"a": "<x & y>" A}"#)],
                vec![None, s("line\nbreak\ttab"), s("3"), s("")],
            ]
        );
        let empty = parse_xml(r#"<?xml version="1.0"?><resultset statement="x"></resultset>"#)
            .expect("valid XML");
        assert!(empty.columns.is_empty() && empty.rows.is_empty());
    }
}
