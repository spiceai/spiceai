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
    component::view::View, dataaccelerator::spice_sys::dataset_checkpointer,
    embeddings::index::table::wrap_table_as_index,
    search::full_text::table::add_full_text_search_to_table, status::RuntimeStatus,
};
use ::datafusion::sql::parser;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    common::TableReference,
    datasource::ViewTable,
    error::{DataFusionError, Result},
    prelude::SessionContext,
};
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_acceleration::sidecar::OpenOption;
use runtime_acceleration::snapshot::SnapshotBehavior;
use runtime_component::dataset::acceleration::RefreshMode;
use runtime_search::embeddings::{table::EmbeddingTable, warm_index_on_zero_results};
use runtime_table::refresh_source::{RefreshSource, RefreshSourceError};
use snafu::ResultExt;
use spicepod::component::embeddings::ColumnEmbeddingConfig;
use std::sync::Arc;

pub(crate) use runtime_datafusion::dependent_tables::get_dependent_table_names;

pub(crate) async fn prepare_view(
    ctx: &SessionContext,
    statement: &parser::Statement,
    view: &Arc<View>,
) -> Result<Arc<dyn TableProvider>> {
    // Keep the spellings of float tokens the planned `f64` literals lose, as the
    // query planner does, so a view filtering a decimal column on such a literal
    // matches what its SQL says.
    let plan = runtime_datafusion::analyzer_rule::exact_float_decimal_cast::FloatLiteralSpellings::collect(statement)
        .retain(ctx.state().statement_to_plan(statement.clone()).await?)?;
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

/// The source of an accelerated view registered from its existing acceleration
/// before the tables it reads have loaded: the view's own plan, built once every
/// table it reads is ready. `read_provider` waits for them rather than failing, so
/// the plan resolves as soon as they are ready instead of at the next backoff step.
pub(crate) struct ViewRefreshSource {
    ctx: Arc<SessionContext>,
    statement: parser::Statement,
    view: Arc<View>,
    reads: Vec<TableReference>,
    status: Arc<RuntimeStatus>,
}

impl ViewRefreshSource {
    pub(crate) fn new_arc(
        ctx: Arc<SessionContext>,
        statement: parser::Statement,
        view: Arc<View>,
        reads: Vec<TableReference>,
        status: Arc<RuntimeStatus>,
    ) -> Arc<Self> {
        Arc::new(Self {
            ctx,
            statement,
            view,
            reads,
            status,
        })
    }
}

impl std::fmt::Debug for ViewRefreshSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewRefreshSource")
            .field("view", &self.view.name)
            .field("reads", &self.reads)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl RefreshSource for ViewRefreshSource {
    fn resolve_refresh_mode(&self, _requested: Option<RefreshMode>) -> RefreshMode {
        // `ViewBuilder::try_from` rejects every refresh mode but `full`.
        RefreshMode::Full
    }

    async fn read_provider(
        &self,
    ) -> std::result::Result<Arc<dyn TableProvider>, RefreshSourceError> {
        crate::datafusion::wait_until_dependent_tables_are_ready(
            &self.view.name,
            &self.reads,
            &self.status,
        )
        .await;
        prepare_view(&self.ctx, &self.statement, &self.view)
            .await
            .map_err(|e| Box::new(e) as RefreshSourceError)
    }

    async fn checkpointer(&self) -> Option<Arc<dyn DatasetCheckpointer>> {
        dataset_checkpointer(
            self.view.as_ref(),
            self.view.runtime.accelerator_engine_registry(),
            OpenOption::OpenExisting,
            SnapshotBehavior::Disabled,
        )
        .await
        .ok()
    }
}
