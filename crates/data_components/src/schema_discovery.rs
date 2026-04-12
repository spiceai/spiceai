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

//! Generic parallel schema discovery with pluggable permission checks.
//!
//! Connectors that can retrieve table schemas from two independent sources
//! (e.g. `information_schema` and `DESCRIBE TABLE`) use [`discover_schema`]
//! to probe both in parallel and apply a deterministic decision matrix.

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use std::future::Future;

// ── Permissions trait ──────────────────────────────────────────────

/// Outcome of a provider-specific permissions check.
#[derive(Debug)]
pub enum PermissionCheckResult {
    /// The principal has read access (or the check is inconclusive / advisory).
    Allowed,
    /// The principal is definitively denied read access.
    Denied { reason: String },
    /// The permissions API itself failed (network error, 500, etc.).
    /// Treated as inconclusive — proceed with schema probes.
    Unavailable { reason: String },
}

/// Provider-specific permissions check for a dataset/table.
///
/// Implementations call whatever API the provider offers (UC effective-permissions,
/// Snowflake SHOW GRANTS, etc.) and classify the result.
#[async_trait]
pub trait DatasetPermissions: Send + Sync {
    async fn check_read_permission(&self, table_name: &str) -> PermissionCheckResult;
}

/// A no-op implementation for providers that don't have a dedicated permissions API.
pub struct NoPermissionsCheck;

#[async_trait]
impl DatasetPermissions for NoPermissionsCheck {
    async fn check_read_permission(&self, _table_name: &str) -> PermissionCheckResult {
        PermissionCheckResult::Allowed
    }
}

// ── Schema probes ──────────────────────────────────────────────────

/// Result of a single schema probe (information_schema or direct SQL).
#[derive(Debug)]
pub enum SchemaProbeResult {
    /// Schema successfully retrieved.
    Ok(SchemaRef),
    /// Access denied — the principal lacks permission for this specific query.
    AccessDenied(String),
    /// Query failed for a non-permission reason.
    Failed(Box<dyn std::error::Error + Send + Sync>),
}

/// Warnings emitted during schema discovery.
#[derive(Debug)]
pub enum SchemaDiscoveryWarning {
    /// `information_schema` was unavailable; fell back to direct query.
    MetadataFallback { reason: String, nuance: String },
    /// Permissions API was unavailable; proceeding without validation.
    PermissionsUnavailable { reason: String },
}

/// Outcome of parallel schema discovery.
#[derive(Debug)]
pub struct SchemaDiscoveryResult {
    pub schema: SchemaRef,
    pub warnings: Vec<SchemaDiscoveryWarning>,
}

/// Runs schema discovery by executing three probes in parallel:
/// 1. `metadata_probe` — query `information_schema` (preferred, has nullability)
/// 2. `direct_probe` — `DESCRIBE TABLE` / `SHOW COLUMNS` (fallback)
/// 3. `permissions.check_read_permission` — provider-specific permission API
///
/// # Decision matrix (evaluated in order)
///
/// | permissions | direct_probe | metadata_probe | Outcome |
/// |-------------|-------------|----------------|---------|
/// | Denied      | *           | *              | permanent error |
/// | *           | AccessDenied| *              | permanent error |
/// | *           | *           | Ok             | use metadata schema |
/// | *           | Ok          | Failed/Denied  | warn + use direct schema |
/// | *           | Failed      | Failed         | propagate error |
pub async fn discover_schema(
    table_name: &str,
    metadata_probe: impl Future<Output = SchemaProbeResult> + Send,
    direct_probe: impl Future<Output = SchemaProbeResult> + Send,
    permissions: &dyn DatasetPermissions,
) -> Result<SchemaDiscoveryResult, Box<dyn std::error::Error + Send + Sync>> {
    let (metadata_result, direct_result, perm_result) = tokio::join!(
        metadata_probe,
        direct_probe,
        permissions.check_read_permission(table_name),
    );

    let mut warnings = Vec::new();

    // 1. Permissions API says Denied → permanent error
    if let PermissionCheckResult::Denied { reason } = &perm_result {
        return Err(format!(
            "Access denied for table '{table_name}': {reason}"
        )
        .into());
    }

    // Track permissions unavailability as a warning
    if let PermissionCheckResult::Unavailable { reason } = &perm_result {
        warnings.push(SchemaDiscoveryWarning::PermissionsUnavailable {
            reason: reason.clone(),
        });
    }

    // 2. direct_probe returns AccessDenied → permanent error (can't query the table)
    if let SchemaProbeResult::AccessDenied(reason) = &direct_result {
        return Err(format!(
            "Access denied for table '{table_name}': {reason}"
        )
        .into());
    }

    // 3. metadata_probe succeeds → use it
    if let SchemaProbeResult::Ok(schema) = metadata_result {
        return Ok(SchemaDiscoveryResult { schema, warnings });
    }

    // 4. metadata_probe failed, direct_probe succeeds → warn + use direct
    if let SchemaProbeResult::Ok(schema) = direct_result {
        warnings.push(SchemaDiscoveryWarning::MetadataFallback {
            reason: "information_schema query failed".to_string(),
            nuance: "Falling back to DESCRIBE TABLE. Column nullability will default to nullable."
                .to_string(),
        });
        return Ok(SchemaDiscoveryResult { schema, warnings });
    }

    // 5. Both probes failed → propagate the most informative error
    // Prefer the direct probe error since it's the fallback path
    match direct_result {
        SchemaProbeResult::Failed(e) => Err(e),
        // AccessDenied already handled above; Ok already handled above
        _ => Err(format!("Failed to discover schema for table '{table_name}'").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), false))
                .collect::<Vec<_>>(),
        ))
    }

    fn metadata_schema() -> SchemaRef {
        make_schema(&[("id", DataType::Int32), ("name", DataType::Utf8)])
    }

    fn direct_schema() -> SchemaRef {
        make_schema(&[("id", DataType::Int32), ("name", DataType::Utf8)])
    }

    struct MockPermissions(PermissionCheckResult);

    #[async_trait]
    impl DatasetPermissions for MockPermissions {
        async fn check_read_permission(&self, _table_name: &str) -> PermissionCheckResult {
            match &self.0 {
                PermissionCheckResult::Allowed => PermissionCheckResult::Allowed,
                PermissionCheckResult::Denied { reason } => PermissionCheckResult::Denied {
                    reason: reason.clone(),
                },
                PermissionCheckResult::Unavailable { reason } => {
                    PermissionCheckResult::Unavailable {
                        reason: reason.clone(),
                    }
                }
            }
        }
    }

    // ── Matrix test 1: All OK → prefer metadata schema ──

    #[tokio::test]
    async fn test_all_ok_prefers_metadata() {
        let result = discover_schema(
            "catalog.schema.table",
            async { SchemaProbeResult::Ok(metadata_schema()) },
            async { SchemaProbeResult::Ok(direct_schema()) },
            &MockPermissions(PermissionCheckResult::Allowed),
        )
        .await
        .expect("should succeed");

        assert_eq!(result.schema, metadata_schema());
        assert!(result.warnings.is_empty());
    }

    // ── Matrix test 2: Metadata OK, direct AccessDenied → permanent error ──

    #[tokio::test]
    async fn test_metadata_ok_direct_denied_is_permanent_error() {
        let result = discover_schema(
            "catalog.schema.table",
            async { SchemaProbeResult::Ok(metadata_schema()) },
            async { SchemaProbeResult::AccessDenied("INSUFFICIENT_PERMISSIONS".into()) },
            &MockPermissions(PermissionCheckResult::Allowed),
        )
        .await;

        let err = result.expect_err("should be permanent error");
        assert!(
            err.to_string().contains("Access denied"),
            "error should mention access denied: {err}"
        );
    }

    // ── Matrix test 3: Metadata AccessDenied, direct OK → warning + direct schema ──

    #[tokio::test]
    async fn test_metadata_denied_direct_ok_warns_and_falls_back() {
        let result = discover_schema(
            "catalog.schema.table",
            async { SchemaProbeResult::AccessDenied("no info_schema access".into()) },
            async { SchemaProbeResult::Ok(direct_schema()) },
            &MockPermissions(PermissionCheckResult::Allowed),
        )
        .await
        .expect("should succeed with fallback");

        assert_eq!(result.schema, direct_schema());
        assert!(
            result.warnings.iter().any(|w| matches!(
                w,
                SchemaDiscoveryWarning::MetadataFallback { nuance, .. }
                    if nuance.contains("DESCRIBE TABLE")
            )),
            "should warn about metadata fallback"
        );
    }

    // ── Matrix test 4: Both AccessDenied → permanent error ──

    #[tokio::test]
    async fn test_both_denied_is_permanent_error() {
        let result = discover_schema(
            "catalog.schema.table",
            async { SchemaProbeResult::AccessDenied("info_schema denied".into()) },
            async { SchemaProbeResult::AccessDenied("table denied".into()) },
            &MockPermissions(PermissionCheckResult::Allowed),
        )
        .await;

        let err = result.expect_err("should be permanent error");
        assert!(
            err.to_string().contains("Access denied"),
            "error should mention access denied: {err}"
        );
    }

    // ── Matrix test 5: Metadata Failed (non-permission), direct OK → warning + direct schema ──

    #[tokio::test]
    async fn test_metadata_failed_direct_ok_warns_and_falls_back() {
        let result = discover_schema(
            "catalog.schema.table",
            async {
                SchemaProbeResult::Failed("UNSUPPORTED_DATA_SOURCE".to_string().into())
            },
            async { SchemaProbeResult::Ok(direct_schema()) },
            &MockPermissions(PermissionCheckResult::Allowed),
        )
        .await
        .expect("should succeed with fallback");

        assert_eq!(result.schema, direct_schema());
        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w, SchemaDiscoveryWarning::MetadataFallback { .. })),
            "should warn about metadata fallback"
        );
    }

    // ── Matrix test 6: Metadata OK, direct Failed (non-permission) → use metadata ──

    #[tokio::test]
    async fn test_metadata_ok_direct_failed_uses_metadata() {
        let result = discover_schema(
            "catalog.schema.table",
            async { SchemaProbeResult::Ok(metadata_schema()) },
            async { SchemaProbeResult::Failed("network error".to_string().into()) },
            &MockPermissions(PermissionCheckResult::Allowed),
        )
        .await
        .expect("should succeed");

        assert_eq!(result.schema, metadata_schema());
        assert!(result.warnings.is_empty());
    }

    // ── Matrix test 7: Both Failed (non-permission) → propagate error ──

    #[tokio::test]
    async fn test_both_failed_propagates_error() {
        let result = discover_schema(
            "catalog.schema.table",
            async { SchemaProbeResult::Failed("info_schema error".to_string().into()) },
            async { SchemaProbeResult::Failed("describe error".to_string().into()) },
            &MockPermissions(PermissionCheckResult::Allowed),
        )
        .await;

        let err = result.expect_err("should propagate error");
        assert!(
            err.to_string().contains("describe error"),
            "should propagate direct probe error: {err}"
        );
    }

    // ── Matrix test 8: Permissions Denied, both probes OK → permanent error ──

    #[tokio::test]
    async fn test_permissions_denied_overrides_probes() {
        let result = discover_schema(
            "catalog.schema.table",
            async { SchemaProbeResult::Ok(metadata_schema()) },
            async { SchemaProbeResult::Ok(direct_schema()) },
            &MockPermissions(PermissionCheckResult::Denied {
                reason: "UC says no SELECT privilege".into(),
            }),
        )
        .await;

        let err = result.expect_err("should be permanent error from permissions");
        assert!(
            err.to_string().contains("Access denied"),
            "error should mention access denied: {err}"
        );
        assert!(
            err.to_string().contains("UC says no SELECT privilege"),
            "error should contain permissions reason: {err}"
        );
    }

    // ── Matrix test 9: Permissions Unavailable, metadata OK → use metadata + warning ──

    #[tokio::test]
    async fn test_permissions_unavailable_metadata_ok() {
        let result = discover_schema(
            "catalog.schema.table",
            async { SchemaProbeResult::Ok(metadata_schema()) },
            async { SchemaProbeResult::Ok(direct_schema()) },
            &MockPermissions(PermissionCheckResult::Unavailable {
                reason: "UC API returned 500".into(),
            }),
        )
        .await
        .expect("should succeed despite permissions being unavailable");

        assert_eq!(result.schema, metadata_schema());
        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w, SchemaDiscoveryWarning::PermissionsUnavailable { .. })),
            "should warn about permissions being unavailable"
        );
    }

    // ── Matrix test 10: Permissions Allowed, metadata fails, direct OK → warning + direct ──

    #[tokio::test]
    async fn test_permissions_allowed_metadata_fails_direct_ok() {
        let result = discover_schema(
            "catalog.schema.table",
            async { SchemaProbeResult::AccessDenied("no info_schema access".into()) },
            async { SchemaProbeResult::Ok(direct_schema()) },
            &MockPermissions(PermissionCheckResult::Allowed),
        )
        .await
        .expect("should succeed with fallback");

        assert_eq!(result.schema, direct_schema());
        assert!(
            result.warnings.iter().any(|w| matches!(
                w,
                SchemaDiscoveryWarning::MetadataFallback { nuance, .. }
                    if nuance.contains("DESCRIBE TABLE")
            )),
            "should warn about metadata fallback"
        );
    }

    // ── NoPermissionsCheck always returns Allowed ──

    #[tokio::test]
    async fn test_no_permissions_check_returns_allowed() {
        let result = NoPermissionsCheck
            .check_read_permission("any.table")
            .await;
        assert!(matches!(result, PermissionCheckResult::Allowed));
    }
}
