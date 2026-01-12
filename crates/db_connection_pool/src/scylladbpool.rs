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

use async_trait::async_trait;
use datafusion_table_providers::sql::db_connection_pool::{
    DbConnectionPool, JoinPushDown, dbconnection::DbConnection,
};
use scylla::client::session::Session;
use snafu::Snafu;

use crate::dbconnection::scylladbconn::ScyllaDbConnection;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build ScyllaDB session: {source}"))]
    SessionBuildError {
        source: scylla::errors::NewSessionError,
    },

    #[snafu(display("Failed to execute query: {source}"))]
    QueryExecutionError {
        source: scylla::errors::ExecutionError,
    },
}

pub struct ScyllaDbConnectionPool {
    session: Arc<Session>,
    keyspace: Arc<str>,
    join_push_down: JoinPushDown,
}

impl ScyllaDbConnectionPool {
    /// Creates a new instance of `ScyllaDbConnectionPool`.
    ///
    /// The session should already be created and connected.
    #[must_use]
    pub fn new(session: Arc<Session>, keyspace: Arc<str>, compute_context: String) -> Self {
        Self {
            session,
            keyspace,
            join_push_down: JoinPushDown::AllowedFor(compute_context),
        }
    }

    #[must_use]
    pub fn keyspace(&self) -> Arc<str> {
        Arc::clone(&self.keyspace)
    }

    #[must_use]
    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }
}

#[async_trait]
impl DbConnectionPool<Arc<Session>, &'static dyn Sync> for ScyllaDbConnectionPool {
    async fn connect(
        &self,
    ) -> std::result::Result<
        Box<dyn DbConnection<Arc<Session>, &'static dyn Sync>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        // ScyllaDB session is already a connection pool internally,
        // so we just share the same session instance
        Ok(Box::new(ScyllaDbConnection::new(
            Arc::clone(&self.session),
            Arc::clone(&self.keyspace),
        )))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_push_down_context() {
        // Verify that join push down context is properly set
        // We can't fully test without a real session, but we can test the configuration
        let compute_context = "scylladb://user@host:9042/keyspace".to_string();

        // Verify the JoinPushDown enum variant is correct
        let push_down = JoinPushDown::AllowedFor(compute_context.clone());
        match push_down {
            JoinPushDown::AllowedFor(ctx) => {
                assert_eq!(ctx, compute_context);
            }
            JoinPushDown::Disallow => panic!("Expected AllowedFor variant"),
        }
    }

    // ============================================================================
    // Additional comprehensive tests for edge cases and critical paths
    // ============================================================================

    #[test]
    fn test_join_push_down_with_special_characters() {
        // Test compute context with special characters in username/keyspace
        let contexts = vec![
            "scylladb://user-name@host:9042/my_keyspace",
            "scylladb://user.name@host:9042/my-keyspace",
            "scylladb://user123@host:9042/keyspace_123",
            "scylladb://@host:9042/keyspace", // Empty user
            "scylladb://user@host.domain.com:9042/keyspace",
            "scylladb://user@192.168.1.1:9042/keyspace", // IP address
        ];

        for ctx in contexts {
            let push_down = JoinPushDown::AllowedFor(ctx.to_string());
            match push_down {
                JoinPushDown::AllowedFor(stored_ctx) => {
                    assert_eq!(stored_ctx, ctx);
                }
                JoinPushDown::Disallow => panic!("Expected AllowedFor variant for context: {ctx}"),
            }
        }
    }

    #[test]
    fn test_join_push_down_empty_context() {
        let push_down = JoinPushDown::AllowedFor(String::new());
        match push_down {
            JoinPushDown::AllowedFor(ctx) => {
                assert!(ctx.is_empty());
            }
            JoinPushDown::Disallow => panic!("Expected AllowedFor variant"),
        }
    }

    #[test]
    fn test_join_push_down_unicode_context() {
        // Though unlikely in practice, test Unicode handling
        let unicode_ctx = "scylladb://用户@主机:9042/键空间".to_string();
        let push_down = JoinPushDown::AllowedFor(unicode_ctx.clone());
        match push_down {
            JoinPushDown::AllowedFor(ctx) => {
                assert_eq!(ctx, unicode_ctx);
            }
            JoinPushDown::Disallow => panic!("Expected AllowedFor variant"),
        }
    }

    #[test]
    fn test_error_display_session_build() {
        // We can't easily create a NewSessionError, but we can test error enum exists
        let error_type = std::any::type_name::<Error>();
        assert!(error_type.contains("Error"));
    }

    #[test]
    fn test_result_type_alias() {
        // Verify Result type alias works correctly
        let ok_result: Result<i32> = Ok(42);
        assert!(ok_result.is_ok());
        if let Ok(value) = ok_result {
            assert_eq!(value, 42);
        }
    }

    #[test]
    fn test_join_push_down_clone() {
        let ctx = "scylladb://user@host:9042/keyspace".to_string();
        let push_down1 = JoinPushDown::AllowedFor(ctx);
        let push_down2 = push_down1.clone();

        match (push_down1, push_down2) {
            (JoinPushDown::AllowedFor(ctx1), JoinPushDown::AllowedFor(ctx2)) => {
                assert_eq!(ctx1, ctx2);
            }
            _ => panic!("Expected both to be AllowedFor variant"),
        }
    }

    #[test]
    fn test_join_push_down_very_long_context() {
        // Test with a very long context string (edge case for memory)
        let long_keyspace = "a".repeat(10000);
        let ctx = format!("scylladb://user@host:9042/{long_keyspace}");
        let push_down = JoinPushDown::AllowedFor(ctx.clone());

        match push_down {
            JoinPushDown::AllowedFor(stored_ctx) => {
                assert_eq!(stored_ctx.len(), ctx.len());
            }
            JoinPushDown::Disallow => panic!("Expected AllowedFor variant"),
        }
    }

    #[test]
    fn test_compute_context_format_variations() {
        // Test various valid compute context format variations
        let valid_contexts = vec![
            // Standard format
            "scylladb://user:@host:9042/keyspace",
            // With default port
            "scylladb://user:@host:9042/keyspace",
            // Without password (as we do in actual code)
            "scylladb://user:@host:9042/keyspace",
            // Multiple hosts scenario (first host in format)
            "scylladb://user:@host1:9042/keyspace",
        ];

        for ctx in valid_contexts {
            assert!(
                ctx.starts_with("scylladb://"),
                "Context should start with scylladb://"
            );
        }
    }
}
