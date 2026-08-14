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

/// Each case below builds a whole runtime and runs its component load, so they
/// are driven one at a time from a single test rather than left to contend for a
/// shared process. Splitting them into separate `#[tokio::test]` functions makes
/// the file pass under a process-per-test runner and fail under a plain
/// `cargo test`, which is a trap for whoever runs the obvious command.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_app_arriving_after_an_empty_start() {
    an_app_that_arrives_after_an_empty_start_is_loaded_not_only_stored().await;
    an_app_less_start_does_not_emit_task_history().await;
    a_table_this_runtime_did_not_register_is_a_conflict_not_a_no_op().await;
    an_arriving_app_decides_whether_queries_emit_task_history().await;
    an_arriving_app_that_enables_task_history_gets_a_table_and_emission().await;
    a_second_initialization_restores_emission_it_found_turned_off().await;
}

/// Emission and the table have to agree. Both were decided when `DataFusion` was
/// built, from the configuration an empty start does not have — so an arriving app
/// that disables task history must also stop queries emitting, or every query
/// keeps writing to a table nothing created.
async fn an_arriving_app_decides_whether_queries_emit_task_history() {
    let rt = Arc::new(Runtime::builder().with_app_opt(None).build().await);
    Arc::clone(&rt).load_components().await;

    let mut disabled = App::default();
    disabled.runtime.task_history.enabled = false;
    Arc::clone(&rt).apply_app(Arc::new(disabled)).await;

    assert!(
        !rt.datafusion()
            .table_exists(&TableReference::partial("runtime", "task_history")),
        "a disabled arriving app creates no table"
    );
    assert!(
        !rt.datafusion().task_history_emission_enabled(),
        "and queries must stop emitting rows nothing will store"
    );
}

/// An app-less start leaves nothing to record into, so emission has to be off
/// until an app arrives — otherwise every query reports a table that was
/// deliberately never created, which is the failure this whole path exists to
/// stop.
async fn an_app_less_start_does_not_emit_task_history() {
    let rt = Arc::new(Runtime::builder().with_app_opt(None).build().await);
    Arc::clone(&rt).load_components().await;

    assert!(
        !rt.datafusion()
            .table_exists(&TableReference::partial("runtime", "task_history")),
        "an app-less start creates no table"
    );
    assert!(
        !rt.datafusion().task_history_emission_enabled(),
        "so nothing may emit into it"
    );
}

/// The other direction: an arriving app that enables it gets both the table and
/// the emission that fills it.
async fn an_arriving_app_that_enables_task_history_gets_a_table_and_emission() {
    let rt = Arc::new(Runtime::builder().with_app_opt(None).build().await);
    Arc::clone(&rt).load_components().await;

    Arc::clone(&rt).apply_app(Arc::new(App::default())).await;

    assert!(
        rt.datafusion()
            .table_exists(&TableReference::partial("runtime", "task_history")),
        "an enabled arriving app brings the table up"
    );
    assert!(
        rt.datafusion().task_history_emission_enabled(),
        "and queries emit into it"
    );
}

/// Emission off is only ever the right answer for as long as there is no table.
///
/// Callers may reach initialization before the app is installed — a cluster
/// executor's component load races its own bind — and that call correctly turns
/// emission off, having found no configuration. The call made once the app is
/// there has to undo it, or the executor comes up with a task-history table that
/// nothing ever writes to. This is also why nothing outside `init_task_history`
/// decides the flag: a caller that gated on it would read an answer about a
/// moment that has passed.
async fn a_second_initialization_restores_emission_it_found_turned_off() {
    let rt = Arc::new(
        Runtime::builder()
            .with_app_opt(Some(Arc::new(App::default())))
            .build()
            .await,
    );

    Arc::clone(&rt)
        .init_task_history()
        .await
        .expect("initialize task history");
    assert!(
        rt.datafusion().task_history_emission_enabled(),
        "a registered table is emitted into"
    );

    // What a racing call that ran before the app was installed leaves behind.
    rt.datafusion().set_task_history_enabled(false);

    Arc::clone(&rt)
        .init_task_history()
        .await
        .expect("a second initialization is a no-op, not a failure");
    assert!(
        rt.datafusion()
            .table_exists(&TableReference::partial("runtime", "task_history")),
        "the table this runtime registered is still its own"
    );
    assert!(
        rt.datafusion().task_history_emission_enabled(),
        "so emission must come back on rather than stay off for the process's life"
    );
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

    // Reporting is not protection. The exporter resolves this table by name when
    // it writes, so what keeps internal rows out of the occupying table is that
    // emission stops — a query built after this carries no task-history emitter.
    assert!(
        !rt.datafusion().task_history_emission_enabled(),
        "a conflict must stop the runtime writing to a table it does not own"
    );
}

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
