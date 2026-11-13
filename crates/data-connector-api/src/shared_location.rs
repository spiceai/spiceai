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

//! Whether instances share a dataset's acceleration through one state
//! location, which decides what a change source's consumer is named for.

use runtime_component::dataset::DatasetSpec;
use runtime_component::dataset::acceleration::{Engine, Mode};

use crate::parameters::ConnectorContext;

/// The state location `dataset`'s acceleration is shared through, when it is:
/// Cayenne in file mode under `runtime.params.cayenne_vnext`, with
/// `runtime.state.location` set, keeps the acceleration in that location for
/// every instance on it. Only the instance that writes the dataset consumes its
/// change source, so the source's consumer (a Postgres slot, a MySQL binlog
/// client) is named for the location rather than for an instance: adding a
/// replica adds no consumer, and a takeover resumes the one its predecessor
/// used. An unset location is each instance's own `.spice/data`, which nothing
/// shares.
#[must_use]
pub fn shared_state_location(
    context: &dyn ConnectorContext,
    dataset: &DatasetSpec,
) -> Option<String> {
    let app = context.app();
    let vnext = app
        .runtime
        .params
        .get("cayenne_vnext")
        .is_some_and(|value| value == "enabled");
    let in_location = dataset.acceleration.as_ref().is_some_and(|acceleration| {
        acceleration.enabled
            && acceleration.engine == Engine::Cayenne
            && acceleration.mode == Mode::File
    });
    if !(vnext && in_location) {
        return None;
    }
    app.runtime
        .state
        .as_ref()
        .map(|state| state.location.clone())
}
