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

use crate::embeddings::udtf::{
    VECTOR_SEARCH_UDTF_NAME, VectorSearchTableFunc, VectorSearchTableFuncArgs,
};
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;

use datafusion::sql::TableReference;
use search::generation::CandidateGeneration;

use crate::datafusion::DataFusion;

pub struct VectorUDTFGeneration {
    df: Arc<DataFusion>,
    tbl: TableReference,
    embedding_column: String,
    is_chunked: bool,
}

impl VectorUDTFGeneration {
    pub fn new(
        df: &Arc<DataFusion>,
        tbl: &TableReference,
        embedding_column: &str,
        is_chunked: bool,
    ) -> Self {
        Self {
            df: Arc::clone(df),
            tbl: tbl.clone(),
            embedding_column: embedding_column.to_string(),
            is_chunked,
        }
    }
}

#[async_trait::async_trait]
impl CandidateGeneration for VectorUDTFGeneration {
    fn search(&self, query: String) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let udtf_args = VectorSearchTableFunc::to_expr(&VectorSearchTableFuncArgs {
            tbl: self.tbl.clone(),
            query,
            column: Some(self.embedding_column.clone()),
            limit: None,
            include_score: Some(true),
        });
        self.df
            .ctx
            .table_function(VECTOR_SEARCH_UDTF_NAME)?
            .create_table_provider(udtf_args.as_slice())
    }

    fn value_derived_from(&self) -> String {
        self.embedding_column.clone()
    }

    fn value_projection_name(&self) -> String {
        if self.is_chunked {
            "_match".to_string()
        } else {
            self.embedding_column.clone()
        }
    }
}
