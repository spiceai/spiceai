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

//! Events emitted while the runtime is built reach the installed subscriber.
//!
//! Regression test for #13537: the build ran between the temporary subscriber
//! `main` ends and the global one `init_tracing` installs, so its warnings —
//! the coordinated accelerator memory budget, invalid `runtime.query.*` values
//! — were dropped at every log level.
//!
//! One test in this binary: `set_global_default` succeeds once per process.

use std::sync::{Arc, Mutex};

use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// Sink for the probe subscriber, so the test can assert on what reached it.
#[derive(Clone, Default)]
struct ProbeWriter(Arc<Mutex<Vec<u8>>>);

impl ProbeWriter {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("probe buffer poisoned")).into_owned()
    }
}

impl std::io::Write for ProbeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("probe buffer poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for ProbeWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn build_time_warnings_reach_the_installed_subscriber() {
    let probe = ProbeWriter::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(probe.clone()),
        ),
    )
    .expect("installs the global subscriber");

    let dir = tempfile::tempdir().expect("creates the spicepod directory");
    std::fs::write(
        dir.path().join("spicepod.yaml"),
        "version: v1\nkind: Spicepod\nname: build-logging\nruntime:\n  query:\n    timeout: notaduration\n",
    )
    .expect("writes the spicepod");

    let app = app::AppBuilder::build_from_path(dir.path())
        .await
        .expect("loads the spicepod");
    let _rt = runtime::Runtime::builder().with_app(app).build().await;

    let logged = probe.contents();
    assert!(
        logged.contains("No query timeout will be applied."),
        "the invalid-timeout warning must reach the installed subscriber, got: {logged}"
    );
    // Emitted through `in_tracing_context`, which must defer to the subscriber
    // already installed rather than shadow it with a temporary one.
    assert!(
        logged.contains("Initialized sql results cache"),
        "the caching line must reach the installed subscriber, got: {logged}"
    );
}
