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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataColumn {
    Location(Option<Arc<str>>),
    LastModified,
    Size,
}

impl MetadataColumn {
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Location(_) => "location",
            Self::LastModified => "last_modified",
            Self::Size => "size",
        }
    }

    #[must_use]
    pub fn field(&self) -> Field {
        let data_type = match self {
            Self::Location(_) => DataType::Utf8,
            Self::LastModified => {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
            }
            Self::Size => DataType::UInt64,
        };

        Field::new(self.name(), data_type, false)
    }
}