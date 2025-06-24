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

use crate::datafusion::indexes::full_text::FullTextDatabaseIndex;

pub(crate) mod full_text;

pub enum Index {
    FullText(FullTextDatabaseIndex),
}

pub enum IndexQueryResult {
}

pub trait Index1 {
    fn resolve_plan(&self, filters: &[Expr], limit: Option<usize>) -> Arc<dyn ExecutionPlan>;
    async fn query(&self, )
}

// resolve "search me" -> vector -> primary keys (topK=10)
// scan primary keys

// SELECT * FROM underlying WHERE primary_key IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

// underlying.scan()

// sort
//   left join (order preserving?)
//      underlying.scan()
//      index_scan() -> primary keys + score

impl Index {
    pub fn is_full_text(&self) -> bool {
        matches!(self, Self::FullText(_))
    }

    pub fn index_type(&self) -> &str {
        match self {
            Self::FullText(_) => "full_text",
        }
    }
}

impl From<FullTextDatabaseIndex> for Index {
    fn from(value: FullTextDatabaseIndex) -> Self {
        Index::FullText(value)
    }
}
