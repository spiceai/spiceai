/*
Copyright 2024 The Spice.ai OSS Authors

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

use datafusion::sql::TableReference;
use snafu::prelude::*;
use spicepod::component::view as spicepod_view;
use std::fs;

use super::dataset::Dataset;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load SQL file {file}: {source}"))]
    UnableToLoadSqlFile {
        file: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "The view is uninitialized, please report a bug at https://github.com/spiceai/spiceai"
    ))]
    Uninitialized,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, PartialEq)]
pub struct View {
    pub name: TableReference,
    /// Inline SQL that describes a view.
    sql: Option<String>,
    /// Reference to a SQL file that describes a view.
    sql_ref: Option<String>,
}

impl TryFrom<spicepod_view::View> for View {
    type Error = crate::Error;

    fn try_from(view: spicepod_view::View) -> Result<Self, Self::Error> {
        let table_reference = Dataset::parse_table_reference(&view.name)?;

        if view.sql.is_none() && view.sql_ref.is_none() {
            return Err(crate::Error::NeedToSpecifySQLView {
                name: table_reference.to_string(),
            });
        }

        Ok(View {
            name: table_reference,
            sql: view.sql,
            sql_ref: view.sql_ref,
        })
    }
}

impl View {
    pub fn try_new(name: &str) -> Result<Self, crate::Error> {
        Ok(Self {
            name: Dataset::parse_table_reference(name)?,
            sql: None,
            sql_ref: None,
        })
    }

    pub fn view_sql(&self) -> Result<String> {
        if let Some(sql) = &self.sql {
            return Ok(sql.clone());
        }

        if let Some(sql_ref) = &self.sql_ref {
            return Self::load_sql_ref(sql_ref);
        }

        Err(Error::Uninitialized)
    }

    fn load_sql_ref(sql_ref: &str) -> Result<String> {
        let sql =
            fs::read_to_string(sql_ref).context(UnableToLoadSqlFileSnafu { file: sql_ref })?;
        Ok(sql)
    }
}
