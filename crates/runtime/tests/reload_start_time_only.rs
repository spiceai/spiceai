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

//! Most of `runtime.*` is consumed once at startup, so a spicepod reload that
//! edits it leaves the process running the value it booted with. The reload
//! reports each such section by name instead of diverging from the file in
//! silence.

use std::io::Write;
use std::sync::Arc;

use app::App;
use parking_lot::Mutex;
use runtime::Runtime;
use spicepod::component::runtime::RuntimeReadyState;
use tracing_subscriber::fmt::MakeWriter;

#[tokio::test(flavor = "current_thread")]
async fn a_reload_reports_the_start_time_only_sections_it_changes() {
    let rt = Arc::new(Runtime::builder().build().await);

    let booted = App::default();
    Arc::clone(&rt).apply_app(Arc::new(booted.clone())).await;

    let mut reloaded = booted;
    reloaded.runtime.cors.enabled = true;
    reloaded.runtime.ready_state = RuntimeReadyState::OnRegistration;
    reloaded.runtime.shutdown_timeout = Some("45s".to_string());

    let logs = capture(Arc::clone(&rt), reloaded).await;

    assert!(
        logs.contains("`runtime.cors` changed"),
        "expected the reload to report `runtime.cors`, got: {logs}"
    );
    assert!(
        logs.contains("`runtime.ready_state` changed"),
        "expected the reload to report `runtime.ready_state`, got: {logs}"
    );
    assert!(
        logs.contains("Restart spiced to apply it."),
        "expected the reload to say a restart is required, got: {logs}"
    );
    assert!(
        !logs.contains("`runtime.shutdown_timeout`"),
        "`runtime.shutdown_timeout` is read from the current app at shutdown, so a reload applies it: {logs}"
    );
    assert!(
        !logs.contains("`runtime.tls`"),
        "an unedited section must not be reported: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_reload_that_changes_nothing_start_time_only_reports_nothing() {
    let rt = Arc::new(Runtime::builder().build().await);

    let booted = App::default();
    Arc::clone(&rt).apply_app(Arc::new(booted.clone())).await;

    let mut reloaded = booted;
    reloaded.name = "reloaded".to_string();

    let logs = capture(Arc::clone(&rt), reloaded).await;

    assert!(
        !logs.contains("Restart spiced to apply it."),
        "a reload that edits no start-time-only section must not ask for a restart: {logs}"
    );
}

/// Applies `reloaded` to `rt` with a capturing subscriber installed, and returns
/// what was logged. `set_default` scopes the subscriber to this thread, which is
/// where the tests' current-thread runtime polls the apply.
async fn capture(rt: Arc<Runtime>, reloaded: App) -> String {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();

    {
        let _guard = tracing::subscriber::set_default(subscriber);
        rt.apply_app(Arc::new(reloaded)).await;
    }

    let captured = logs.0.lock().clone();
    String::from_utf8_lossy(&captured).into_owned()
}

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
