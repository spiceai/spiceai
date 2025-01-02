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

use std::{fmt::Display, str::FromStr, sync::Arc};

use spicepod::component::runtime::UserAgentCollection;

use super::App;

impl App {
    /// Get a parameter from the app's runtime params, with a default value if the parameter is not set or is not valid.
    ///
    /// Returns `default_value` if the parameter is not set or is not valid.
    ///
    /// If the parameter is set but is not valid, logs a warning and returns `default_value`.
    #[must_use]
    pub fn get_runtime_param<T>(app: &Option<Arc<Self>>, param: &str, default_value: T) -> T
    where
        T: Display + FromStr,
    {
        let Some(value) = app.as_ref().and_then(|app| app.runtime.params.get(param)) else {
            return default_value;
        };

        if let Ok(parsed_value) = value.parse::<T>() {
            parsed_value
        } else {
            eprintln!("runtime.params.{param} is not valid, defaulting to {default_value}");
            default_value
        }
    }

    #[must_use]
    pub fn user_agent_collection(&self) -> UserAgentCollection {
        self.runtime.telemetry.user_agent_collection
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppBuilder;
    use std::{collections::HashMap, sync::Arc};

    fn create_app_with_params(params: HashMap<String, String>) -> Arc<App> {
        Arc::new(AppBuilder::new("test").with_runtime_params(params).build())
    }

    #[test]
    fn test_get_runtime_param() {
        // Test case 1: Parameter is not set
        let app = Some(create_app_with_params(HashMap::new()));
        assert!(App::get_runtime_param(&app, "test_param", true));
        assert!(!App::get_runtime_param(&app, "test_param", false));

        // Test case 2: Parameter is set to "true"
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "true".to_string());
        let app = Some(create_app_with_params(params));
        assert!(App::get_runtime_param(&app, "test_param", false));

        // Test case 3: Parameter is set to "false"
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "false".to_string());
        let app = Some(create_app_with_params(params));
        assert!(!App::get_runtime_param(&app, "test_param", true));

        // Test case 4: Parameter is set to an invalid boolean value
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "not_a_bool".to_string());
        let app = Some(create_app_with_params(params));
        assert!(App::get_runtime_param(&app, "test_param", true));
        assert!(!App::get_runtime_param(&app, "test_param", false));

        // Test case 5: App is None
        assert!(App::get_runtime_param(&None, "test_param", true));
        assert!(!App::get_runtime_param(&None, "test_param", false));
    }
}
