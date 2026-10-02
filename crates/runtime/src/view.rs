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
use crate::{
    component::view::View, embeddings::index::table::wrap_table_as_index,
    search::full_text::table::add_full_text_search_to_table,
};
use ::datafusion::sql::parser;
use datafusion::{
    catalog::TableProvider,
    datasource::ViewTable,
    error::{DataFusionError, Result},
    prelude::SessionContext,
};
use runtime_search::embeddings::{table::EmbeddingTable, warm_index_on_zero_results};
use snafu::ResultExt;
use spicepod::component::embeddings::ColumnEmbeddingConfig;
use std::sync::Arc;

pub(crate) use runtime_datafusion::dependent_tables::get_dependent_table_names;

pub(crate) async fn prepare_view(
    ctx: &SessionContext,
    statement: &parser::Statement,
    view: &Arc<View>,
) -> Result<Arc<dyn TableProvider>> {
    let plan = ctx.state().statement_to_plan(statement.clone()).await?;
    let view_table = ViewTable::new(plan, Some(view.sql.to_string()));
    let mut tbl_provider = Arc::new(view_table) as Arc<dyn TableProvider>;

    // Add any embedding columns (and vector engine, if applicable)
    if view.has_embeddings() {
        let file_format = view.params.get("file_format").map(String::as_str);
        if let Some(ref vectors) = view.vectors
            && vectors.enabled
        {
            let on_zero_results = warm_index_on_zero_results(view.acceleration.as_ref());

            tbl_provider = wrap_table_as_index(
                &Arc::new(ctx.clone()),
                &view.runtime.embeds(),
                &view.runtime.secrets(),
                &view.name,
                &view.columns,
                file_format,
                tbl_provider,
                vectors,
                on_zero_results,
            )
            .await?;
        } else {
            tbl_provider = EmbeddingTable::from_spicepod_columns(
                tbl_provider,
                view.columns
                    .iter()
                    .flat_map(|col| {
                        col.embeddings.iter().map(|emb| ColumnEmbeddingConfig {
                            column: col.name.clone(),
                            model: emb.model.clone(),
                            primary_keys: emb.row_ids.clone(),
                            chunking: emb.chunking.clone(),
                            vector_size: emb.vector_size,
                            aggregation: emb.aggregation,
                            max_elements_per_row: emb.max_elements_per_row,
                        })
                    })
                    .collect(),
                &view.runtime.embeds(),
                file_format,
            )
            .await
            .boxed()
            .map_err(DataFusionError::External)?;
        }
    }

    // Configure full-text search
    if view.has_full_text_column() {
        tbl_provider =
            add_full_text_search_to_table(&tbl_provider, &view.columns, &view.name, false)?
                as Arc<dyn TableProvider>;
    }

    Ok(tbl_provider)
}
