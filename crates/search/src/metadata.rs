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

use std::sync::Arc;

use arrow::datatypes::FieldRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetadataColumn {
    Filterable(FieldRef),
    NonFilterable(FieldRef),
}

impl MetadataColumn {
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Filterable(field) | Self::NonFilterable(field) => field.name(),
        }
    }

    #[must_use]
    pub fn field(&self) -> FieldRef {
        match self {
            Self::Filterable(field) | Self::NonFilterable(field) => Arc::clone(field),
        }
    }

    #[must_use]
    pub fn type_display(&self) -> &'static str {
        match self {
            MetadataColumn::Filterable(_) => "filterable",
            MetadataColumn::NonFilterable(_) => "non-filterable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetadataColumns(Vec<MetadataColumn>);

impl MetadataColumns {
    #[must_use]
    pub fn none() -> Self {
        Self(vec![])
    }

    pub fn iter(&self) -> impl Iterator<Item = &MetadataColumn> {
        self.0.iter()
    }
}

impl IntoIterator for MetadataColumns {
    type Item = MetadataColumn;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<Vec<MetadataColumn>> for MetadataColumns {
    fn from(columns: Vec<MetadataColumn>) -> Self {
        Self(columns)
    }
}

impl MetadataColumns {
    #[must_use]
    pub fn filterable(&self) -> Vec<FieldRef> {
        self.0
            .iter()
            .filter_map(|c| match c {
                MetadataColumn::Filterable(field) => Some(Arc::clone(field)),
                MetadataColumn::NonFilterable(_) => None,
            })
            .collect()
    }

    #[must_use]
    pub fn non_filterable(&self) -> Vec<FieldRef> {
        self.0
            .iter()
            .filter_map(|c| match c {
                MetadataColumn::Filterable(_) => None,
                MetadataColumn::NonFilterable(field) => Some(Arc::clone(field)),
            })
            .collect()
    }

    #[must_use]
    pub fn non_filterable_names(&self) -> Vec<String> {
        self.non_filterable()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    #[must_use]
    pub fn filterable_names(&self) -> Vec<String> {
        self.filterable().iter().map(|f| f.name().clone()).collect()
    }

    #[must_use]
    pub fn all_names(&self) -> Vec<String> {
        self.non_filterable()
            .iter()
            .map(|f| f.name().clone())
            .chain(self.filterable().iter().map(|f| f.name().clone()))
            .collect()
    }
}
