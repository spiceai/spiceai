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

use std::sync::Arc;

use crate::status::RuntimeStatus;
use axum::{
    http::status,
    response::{IntoResponse, Response},
    Extension,
};

/// Check Readiness
///
/// Check the runtime status of all the components of the runtime. If the service is ready, it returns an HTTP 200 status with the message "ready". If not, it returns a 503 status with the message "not ready".
///
/// The behavior for when an accelerated dataset is considered ready is configurable via the `ready_state` parameter. See [Data refresh](https://spiceai.org/docs/components/data-accelerators/data-refresh#ready-state) for more details.
///
/// ### Readiness Probe
/// In production deployments, the /v1/ready endpoint can be used as a readiness probe for a Spice deployment to ensure traffic is routed to the Spice runtime only after all datasets have finished loading.
///
/// Example Kubernetes readiness probe:
/// ```yaml
/// readinessProbe:
///  httpGet:
///    path: /v1/ready
///    port: 8090
/// ```
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/ready",
    operation_id = "ready",
    tag = "Ready",
    responses(
        (status = 200, description = "Service is ready", content((String = "text/plain", example = "ready"))),
        (status = 503, description = "Service is not ready", content((String = "text/plain", example = "not ready")))
    )
))]
pub(crate) async fn get(Extension(status): Extension<Arc<RuntimeStatus>>) -> Response {
    if status.is_ready() {
        return (status::StatusCode::OK, "ready").into_response();
    }
    (status::StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
}
