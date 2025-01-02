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

use datafusion::sql::TableReference;
use snafu::prelude::*;
use spicepod::component::view as spicepod_view;
use std::fs;

use super::{dataset::Dataset, validate_identifier};

#[derive(Debug, Clone, PartialEq)]
pub struct View {
    pub name: TableReference,
    pub sql: String,
}

impl TryFrom<spicepod_view::View> for View {
    type Error = crate::Error;

    fn try_from(view: spicepod_view::View) -> Result<Self, Self::Error> {
        validate_identifier(&view.name).context(crate::ComponentSnafu)?;

        let table_reference = Dataset::parse_table_reference(&view.name)?;

        let sql = if let Some(view_sql) = &view.sql {
            view_sql.to_string()
        } else if let Some(sql_ref) = &view.sql_ref {
            Self::load_sql_ref(sql_ref)?
        } else {
            return Err(crate::Error::NeedToSpecifySQLView {
                name: table_reference.to_string(),
            });
        };

        Ok(View {
            name: table_reference,
            sql,
        })
    }
}

impl View {
    pub fn try_new(name: &str, sql: String) -> Result<Self, crate::Error> {
        Ok(Self {
            name: Dataset::parse_table_reference(name)?,
            sql,
        })
    }

    fn load_sql_ref(sql_ref: &str) -> crate::Result<String> {
        let sql = fs::read_to_string(sql_ref)
            .context(crate::UnableToLoadSqlFileSnafu { file: sql_ref })?;
        Ok(sql)
    }
}
