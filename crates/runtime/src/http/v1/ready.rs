/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::status::RuntimeStatus;
use axum::{
    http::status,
    response::{IntoResponse, Response},
    Extension,
};

pub(crate) async fn get(Extension(status): Extension<Arc<RuntimeStatus>>) -> Response {
    if status.is_ready() {
        return (status::StatusCode::OK, "ready").into_response();
    }
    (status::StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
}
