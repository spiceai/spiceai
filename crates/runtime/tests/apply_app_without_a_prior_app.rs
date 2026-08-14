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

//! A runtime can start with no configuration at all: a Cloud-connected instance
//! waiting for its first deployment, or a watched directory with no
//! `spicepod.yaml` yet. Its component load then runs against no app, so nothing
//! is registered — and the app that arrives afterwards has to be loaded, not just
//! stored. Storing it alone leaves the instance reporting components it never
//! loaded and writing every query to a task-history table it never built.

use std::sync::Arc;

use app::App;
use datafusion::sql::TableReference;
use runtime::Runtime;
use spicepod::component::view::View;

fn view(name: &str, sql: &str) -> View {
    View {
        name: name.to_string(),
        description: None,
        metadata: std::collections::HashMap::default(),
        columns: Vec::new(),
        sql: Some(sql.to_string()),
        sql_ref: None,
        acceleration: None,
        ready_state: spicepod::component::dataset::ReadyState::default(),
        vectors: None,
        params: None,
        depends_on: Vec::new(),
    }
}

/// Idempotence rests on this runtime having registered the table, never on the
/// name being taken.
///
/// The distinction matters because the arriving-app path registers an app's own
/// components before this runs, so a name check would treat anything already
/// occupying `runtime.task_history` as the internal table, report success, and
/// send every task-history write to it. Whether a spicepod can claim that name is
/// a question for component validation; the guard is written so that the answer
/// does not matter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_table_this_runtime_did_not_register_is_a_conflict_not_a_no_op() {
    // Built with an app so task history has configuration to read, and left
    // uninitialized: this is the window the arriving-app path opens, where
    // components are registered before task history is brought up.
    let rt = Arc::new(
        Runtime::builder()
            .with_app_opt(Some(Arc::new(App::default())))
            .build()
            .await,
    );

    // Something else occupies the name before task history is initialized.
    let occupied = TableReference::partial("runtime", "task_history");
    rt.datafusion()
        .ctx
        .register_table(
            occupied.clone(),
            Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(
                arrow::datatypes::Schema::empty(),
            ))),
        )
        .expect("occupy the task-history name");
    assert!(
        rt.datafusion().table_exists(&occupied),
        "the name is taken before initialization runs"
    );

    let error = Arc::clone(&rt)
        .init_task_history()
        .await
        .expect_err("a table this runtime did not register must not be adopted");
    let rendered = error.to_string();
    assert!(
        rendered.contains("already registered") && rendered.contains("Rename"),
        "the conflict must be reported with a way out: {rendered}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_app_that_arrives_after_an_empty_start_is_loaded_not_only_stored() {
    let rt = Arc::new(Runtime::builder().with_app_opt(None).build().await);
    // The load an empty start performs: with no app, it registers nothing.
    Arc::clone(&rt).load_components().await;

    let mut arrived = App::default();
    arrived.views.push(view("arrived", "SELECT 1 AS answer"));
    assert!(
        Arc::clone(&rt).apply_app(Arc::new(arrived)).await,
        "an app arriving at a runtime that had none is a change"
    );

    assert!(
        rt.datafusion()
            .table_exists(&TableReference::partial("public", "arrived")),
        "the arriving app's components must be registered, not only recorded"
    );

    // Task history is configured by the app, so an empty start skips it
    // entirely while every later query writes to it.
    assert!(
        rt.datafusion()
            .table_exists(&TableReference::partial("runtime", "task_history")),
        "task history must be initialized once the runtime has an app to configure it from"
    );
}
