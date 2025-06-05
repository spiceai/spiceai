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

use super::{ConfigurationLoadingFailedSnafu, DatabaseName};
use crate::dataconnector::glue::InputFormat;
use crate::dataconnector::parameters::aws::load_config;
use crate::{Runtime, dataconnector::parameters::ConnectorParams};
use aws_sdk_glue::Client;
use aws_sdk_glue::types::Table;
use globset::GlobSet;
use snafu::ResultExt as _;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

pub struct GlueCatalogState {
    include: Option<GlobSet>,
    orig_include: Vec<String>,
    client: Client,
    pub(super) databases: RwLock<HashMap<DatabaseName, Vec<Table>>>,
    pub(super) parameters: ConnectorParams,
    pub(super) runtime: Arc<Runtime>,
}

impl GlueCatalogState {
    pub async fn new(
        include: Option<GlobSet>,
        orig_include: Vec<String>,
        mut parameters: ConnectorParams,
        runtime: Arc<Runtime>,
    ) -> Result<Self, super::Error> {
        for validator in super::VALIDATORS.iter() {
            validator
                .validate(&mut parameters)
                .await
                .context(super::ParameterValidationSnafu)?;
        }

        let config = load_config(
            "GlueCatalogConnector",
            "region",
            "key",
            "secret",
            "session_token",
            &parameters.parameters,
        )
        .await
        .context(ConfigurationLoadingFailedSnafu)?;

        let client = Client::new(&config);

        Ok(Self {
            include,
            orig_include,
            client,
            databases: RwLock::new(HashMap::new()),
            parameters,
            runtime,
        })
    }

    pub async fn refresh(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut paginator = self.client.get_databases().into_paginator().send();

        let mut databases = HashMap::new();

        while let Some(maybe_get_databases_output) = paginator.next().await {
            let get_databases_output =
                maybe_get_databases_output.context(super::GetDatabasesSnafu)?;
            for db in get_databases_output.database_list {
                if !database_might_match(&db.name, &self.orig_include) {
                    tracing::debug!("skipping database {}", &db.name);
                    continue;
                }

                let mut paginator = self
                    .client
                    .get_tables()
                    .database_name(&db.name)
                    .into_paginator()
                    .send();

                let mut tables = Vec::new();

                while let Some(maybe_get_tables_output) = paginator.next().await {
                    let get_tables_output =
                        maybe_get_tables_output.map_err(|source| super::Error::GetTables {
                            database: db.name.to_string(),
                            source,
                        })?;
                    let some_tables = get_tables_output
                        .table_list
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|t| {
                            InputFormat::try_from(t).is_ok()
                                && is_included(self.include.as_ref(), &db.name, t.name())
                        })
                        .collect::<Vec<_>>();

                    tables.extend(some_tables);
                }

                databases.insert(db.name, tables);
            }
        }
        let mut dbs = match self.databases.write() {
            Ok(dbs) => dbs,
            Err(poisoned) => poisoned.into_inner(),
        };

        *dbs = databases;

        Ok(())
    }
}

impl fmt::Debug for GlueCatalogState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlueCatalogState")
            .field("databases", &self.databases)
            .finish_non_exhaustive()
    }
}

fn database_might_match(database: &str, patterns: &[String]) -> bool {
    patterns.iter().any(|pattern| {
        pattern == database
            || pattern.starts_with(&format!("{database}."))
            || pattern.starts_with("*.")
            || pattern == "*.*"
    })
}

fn is_included(include: Option<&globset::GlobSet>, database: &str, table: &str) -> bool {
    let database_with_table = format!("{database}.{table}");
    if let Some(include) = include {
        if !include.is_match(&database_with_table) {
            tracing::debug!("skipping table {database_with_table}");
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use globset::{Glob, GlobSetBuilder};

    #[test]
    fn database_might_match_exact_match() {
        let patterns = vec!["mydb".to_string()];
        assert!(database_might_match("mydb", &patterns));
    }

    #[test]
    fn database_might_match_prefix_match() {
        let patterns = vec!["mydb.table1".to_string()];
        assert!(database_might_match("mydb", &patterns));
    }

    #[test]
    fn database_might_match_wildcard_prefix() {
        let patterns = vec!["*.table1".to_string()];
        assert!(database_might_match("mydb", &patterns));
    }

    #[test]
    fn database_might_match_wildcard_all() {
        let patterns = vec!["*.*".to_string()];
        assert!(database_might_match("mydb", &patterns));
    }

    #[test]
    fn database_might_match_no_match() {
        let patterns = vec!["otherdb".to_string(), "otherdb.table1".to_string()];
        assert!(!database_might_match("mydb", &patterns));
    }

    #[test]
    fn database_might_match_empty_patterns() {
        let patterns: Vec<String> = vec![];
        assert!(!database_might_match("mydb", &patterns));
    }

    #[test]
    fn is_included_no_globset() {
        assert!(is_included(None, "mydb", "table1"));
    }

    #[test]
    fn is_included_matching_glob() {
        let mut builder = GlobSetBuilder::new();
        builder.add(Glob::new("mydb.table1").expect("builder add"));
        let globset = builder.build().expect("builder build");
        assert!(is_included(Some(&globset), "mydb", "table1"));
    }

    #[test]
    fn is_included_non_matching_glob() {
        let mut builder = GlobSetBuilder::new();
        builder.add(Glob::new("otherdb.table1").expect("builder add"));
        let globset = builder.build().expect("builder build");
        assert!(!is_included(Some(&globset), "mydb", "table1"));
    }

    #[test]
    fn is_included_wildcard_glob() {
        let mut builder = GlobSetBuilder::new();
        builder.add(Glob::new("*.table1").expect("builder add"));
        let globset = builder.build().expect("builder build");
        assert!(is_included(Some(&globset), "mydb", "table1"));
    }
}
