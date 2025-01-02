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

use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

use crate::component::dataset;

use super::{Acceleration, IndexType, OnConflictBehavior};

impl Acceleration {
    fn on_conflict_targets(&self) -> Vec<ColumnReference> {
        let mut on_conflict_targets: Vec<ColumnReference> = Vec::new();
        for (column, index_type) in &self.indexes {
            if *index_type == IndexType::Unique {
                on_conflict_targets.push(column.clone());
            }
        }
        if let Some(primary_key) = &self.primary_key {
            on_conflict_targets.push(primary_key.clone());
        }

        on_conflict_targets
    }

    pub fn on_conflict(&self) -> dataset::Result<Option<OnConflict>> {
        let on_conflict_all_targets = self.on_conflict_targets();

        if self.on_conflict.len() > 1 && on_conflict_all_targets.len() != self.on_conflict.len() {
            return dataset::OnConflictTargetMismatchSnafu{
                extra_detail: format!("The number of column references gathered from primary key/unique indexes ({}) does not match the number of on_conflict targets ({})", on_conflict_all_targets.len(), self.on_conflict.len())
            }.fail();
        }

        if self.on_conflict.len() > 1
            && !self
                .on_conflict
                .iter()
                .all(|c| *c.1 == OnConflictBehavior::Drop)
        {
            return dataset::OnConflictTargetMismatchSnafu {
                extra_detail: String::new(),
            }
            .fail();
        } else if self.on_conflict.len() > 1 {
            return Ok(Some(OnConflict::DoNothingAll));
        }

        let Some(on_conflict) = self.on_conflict.iter().next() else {
            return Ok(None);
        };

        match on_conflict.1 {
            OnConflictBehavior::Drop => Ok(Some(OnConflict::DoNothing(on_conflict.0.clone()))),
            OnConflictBehavior::Upsert => Ok(Some(OnConflict::Upsert(on_conflict.0.clone()))),
        }
    }
}
