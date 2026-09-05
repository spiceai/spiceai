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

//! Loader for an IBM `test-suites/tpch` checkout.
//!
//! The IBM Rust SDK's YAML loader (v0.1.1) does not populate `inputTables`
//! from `metadata.yaml`. This loader does, so Mode A can register the CSVs
//! each plan actually reads.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use snafu::ResultExt;

use crate::compare::{TableData, parse_typed_csv};
use crate::error::{self, Result};

#[derive(Debug, Deserialize)]
pub struct SuiteDefinition {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(rename = "testCases", default)]
    pub test_cases: Vec<TestCaseDefinition>,
}

#[derive(Debug, Deserialize)]
pub struct TestCaseDefinition {
    pub id: String,
    #[serde(default)]
    pub description: String,
    #[serde(rename = "planBinary")]
    pub plan_binary: String,
    #[serde(rename = "expectedOutput")]
    pub expected_output: Option<String>,
    #[serde(rename = "inputTables", default)]
    pub input_tables: Vec<InputTableDefinition>,
}

#[derive(Debug, Deserialize)]
pub struct InputTableDefinition {
    pub name: String,
    pub file: String,
}

#[derive(Clone, Debug)]
pub struct LoadedSuite {
    pub name: String,
    pub version: String,
    pub description: String,
    pub root: PathBuf,
    pub cases: Vec<LoadedCase>,
}

#[derive(Clone, Debug)]
pub struct LoadedCase {
    pub id: String,
    pub description: String,
    pub plan_path: PathBuf,
    pub plan_bytes: Vec<u8>,
    pub input_tables: Vec<InputTable>,
    pub expected: Option<TableData>,
}

#[derive(Clone, Debug)]
pub struct InputTable {
    pub name: String,
    pub csv_path: PathBuf,
}

pub fn load_tpch_suite(root: &Path) -> Result<LoadedSuite> {
    let metadata_path = root.join("metadata.yaml");
    ensure_exists(&metadata_path, "metadata.yaml")?;
    let text = std::fs::read_to_string(&metadata_path).context(error::ReadFileSnafu {
        path: metadata_path.clone(),
    })?;
    let def: SuiteDefinition = yaml::from_str(&text).context(error::YamlParseSnafu {
        path: metadata_path,
    })?;

    let mut cases = Vec::with_capacity(def.test_cases.len());
    for tc in def.test_cases {
        let plan_path = root.join(&tc.plan_binary);
        let plan_bytes = std::fs::read(&plan_path).context(error::ReadFileSnafu {
            path: plan_path.clone(),
        })?;

        let expected = {
            let csv_path = match &tc.expected_output {
                Some(rel) => root.join(rel),
                None => root.join("expected").join(format!("{}.csv", tc.id)),
            };
            if csv_path.exists() {
                let csv = std::fs::read_to_string(&csv_path).context(error::ReadFileSnafu {
                    path: csv_path.clone(),
                })?;
                Some(parse_typed_csv(&csv).context(error::InvalidGoldenSnafu { path: csv_path })?)
            } else {
                None
            }
        };

        let input_tables = tc
            .input_tables
            .into_iter()
            .map(|t| {
                let csv_path = root.join(&t.file);
                InputTable {
                    name: t.name,
                    csv_path,
                }
            })
            .collect();

        cases.push(LoadedCase {
            id: tc.id,
            description: tc.description,
            plan_path,
            plan_bytes,
            input_tables,
            expected,
        });
    }

    Ok(LoadedSuite {
        name: def.name,
        version: def.version,
        description: def.description,
        root: root.to_path_buf(),
        cases,
    })
}

fn ensure_exists(path: &Path, name: &str) -> Result<()> {
    if path.exists() {
        Ok(())
    } else {
        error::SuitePathMissingSnafu {
            path: path.to_path_buf(),
            name: name.to_string(),
        }
        .fail()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_yaml_deserializes_input_tables() {
        let yaml = r#"
name: "tpch"
version: "1.0.0"
description: "fixture"
testCases:
  - id: "q01"
    description: "Pricing Summary"
    planBinary: "plans/q01.bin"
    expectedOutput: "expected/q01.csv"
    inputTables:
      - name: "lineitem"
        file: "data/lineitem.csv"
"#;
        let def: SuiteDefinition = yaml::from_str(yaml).expect("parse fixture metadata");
        assert_eq!(def.name, "tpch");
        assert_eq!(def.test_cases.len(), 1);
        assert_eq!(def.test_cases[0].id, "q01");
        assert_eq!(def.test_cases[0].input_tables[0].name, "lineitem");
    }

    fn write_mini_suite(label: &str, expected_csv: &[u8]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "spice-substrait-golden-{}-{label}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("plans")).expect("mini suite plans dir");
        std::fs::create_dir_all(dir.join("expected")).expect("mini suite expected dir");
        std::fs::write(
            dir.join("metadata.yaml"),
            r#"
name: "tpch"
version: "1.0.0"
testCases:
  - id: "q99"
    planBinary: "plans/q99.bin"
    expectedOutput: "expected/q99.csv"
"#,
        )
        .expect("write mini suite metadata");
        std::fs::write(dir.join("plans/q99.bin"), []).expect("write mini suite plan");
        std::fs::write(dir.join("expected/q99.csv"), expected_csv).expect("write mini suite golden");
        dir
    }

    #[test]
    fn zero_byte_golden_is_a_load_error() {
        let dir = write_mini_suite("zero-byte", &[]);
        let err = load_tpch_suite(&dir).expect_err("zero-byte golden must fail load");
        let msg = err.to_string();
        assert!(
            msg.contains("typed header"),
            "load error should name the missing typed header: {msg}"
        );
        assert!(
            msg.contains("q99.csv"),
            "load error should name the golden file: {msg}"
        );
        std::fs::remove_dir_all(&dir).expect("cleanup mini suite");
    }

    #[test]
    fn header_only_golden_loads_as_empty_typed_table() {
        let dir = write_mini_suite("header-only", b"flag:string|n:integer\n");
        let suite = load_tpch_suite(&dir).expect("header-only golden must load");
        let expected = suite.cases[0]
            .expected
            .as_ref()
            .expect("header-only golden is present");
        assert_eq!(expected.columns.len(), 2);
        assert_eq!(expected.columns[0].name, "flag");
        assert_eq!(expected.columns[1].type_token, "integer");
        assert!(expected.rows.is_empty());
        std::fs::remove_dir_all(&dir).expect("cleanup mini suite");
    }
}
