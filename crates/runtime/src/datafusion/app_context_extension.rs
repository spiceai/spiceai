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
use app::App;
use runtime_request_context::Extension;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppContextExtension {
    app: Option<Arc<App>>,
}

impl AppContextExtension {
    #[must_use]
    pub fn new(app: Option<Arc<App>>) -> Self {
        Self { app }
    }

    #[must_use]
    pub fn app(&self) -> Option<Arc<App>> {
        self.app.clone()
    }
}

impl Extension for AppContextExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
