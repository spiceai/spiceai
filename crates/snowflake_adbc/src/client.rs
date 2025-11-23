/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::auth::{generate_jwt_token, AuthConfig};
use crate::error::{Error, Result};
use crate::types::{
    ChunkInfo, LoginRequest, LoginRequestData, LoginResponse, QueryRequest, QueryResponse,
};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

const CLIENT_APP_ID: &str = "spiced";
const CLIENT_APP_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Snowflake client using Arrow format over HTTP.
/// 
/// This implements Snowflake's ADBC-compatible Arrow protocol, where query results
/// are returned as Arrow IPC batches over HTTP rather than JSON.
#[derive(Debug, Clone)]
pub struct SnowflakeClient {
    http_client: reqwest::Client,
    account: String,
    host: String,
    token: Arc<RwLock<Option<String>>>,
    master_token: Arc<RwLock<Option<String>>>,
    session_id: Arc<RwLock<Option<i64>>>,
    sequence_id: Arc<RwLock<u64>>,
}

impl SnowflakeClient {
    pub async fn new(account: &str) -> Result<Self> {
        let host = if account.contains('.') {
            format!("https://{}.snowflakecomputing.com", account)
        } else {
            format!("https://{}.snowflakecomputing.com", account)
        };

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .http2_prior_knowledge() // Use HTTP/2 for better streaming
            .build()
            .map_err(|e| Error::ConnectionFailed {
                message: format!("Failed to create HTTP client: {}", e),
            })?;

        Ok(Self {
            http_client,
            account: account.to_string(),
            host,
            token: Arc::new(RwLock::new(None)),
            master_token: Arc::new(RwLock::new(None)),
            session_id: Arc::new(RwLock::new(None)),
            sequence_id: Arc::new(RwLock::new(0)),
        })
    }

    pub async fn authenticate(
        &self,
        auth_config: &AuthConfig,
        warehouse: Option<&str>,
        database: Option<&str>,
        schema: Option<&str>,
        role: Option<&str>,
    ) -> Result<()> {
        let mut session_params = serde_json::Map::new();
        
        if let Some(wh) = warehouse {
            session_params.insert("WAREHOUSE".to_string(), json!(wh));
        }
        if let Some(db) = database {
            session_params.insert("DATABASE".to_string(), json!(db));
        }
        if let Some(sch) = schema {
            session_params.insert("SCHEMA".to_string(), json!(sch));
        }
        if let Some(r) = role {
            session_params.insert("ROLE".to_string(), json!(r));
        }

        let (login_name, password, token, authenticator) = match auth_config {
            AuthConfig::Password { username, password } => {
                (Some(username.clone()), Some(password.clone()), None, Some("SNOWFLAKE".to_string()))
            }
            AuthConfig::Jwt { username, private_key } => {
                let jwt_token = generate_jwt_token(username, &self.account, private_key)?;
                (Some(username.clone()), None, Some(jwt_token), Some("SNOWFLAKE_JWT".to_string()))
            }
        };

        let request = LoginRequest {
            data: LoginRequestData {
                account_name: self.account.clone(),
                login_name,
                password,
                token,
                authenticator,
                session_parameters: if session_params.is_empty() {
                    None
                } else {
                    Some(json!(session_params))
                },
                client_app_id: CLIENT_APP_ID.to_string(),
                client_app_version: CLIENT_APP_VERSION.to_string(),
            },
        };

        let url = format!("{}/session/v1/login-request", self.host);
        let response = self
            .http_client
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(USER_AGENT, format!("{}/{}", CLIENT_APP_ID, CLIENT_APP_VERSION))
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::ConnectionFailed {
                message: format!("HTTP request failed: {}", e),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::AuthenticationFailed {
                message: format!("Authentication failed with status {}: {}", status, error_text),
            });
        }

        let login_response: LoginResponse = response.json().await.map_err(|e| {
            Error::AuthenticationFailed {
                message: format!("Failed to parse login response: {}", e),
            }
        })?;

        if !login_response.success {
            return Err(Error::AuthenticationFailed {
                message: login_response.message.unwrap_or_else(|| "Unknown error".to_string()),
            });
        }

        *self.token.write().await = login_response.data.token;
        *self.master_token.write().await = login_response.data.master_token;
        *self.session_id.write().await = login_response.data.session_id;

        // Set Arrow format for query results via ALTER SESSION
        // Try both parameter names since documentation is unclear
        let _  = self.execute_query_arrow("ALTER SESSION SET QUERY_RESULT_FORMAT = 'ARROW'").await;

        Ok(())
    }

    /// Execute a query and request Arrow format results.
    /// 
    /// This sets the query result format to "arrow" to get Arrow IPC batches
    /// instead of JSON results.
    pub async fn execute_query_arrow(&self, sql: &str) -> Result<QueryResponse> {
        let token = self.token.read().await;
        let token = token.as_ref().ok_or(Error::NotInitialized)?;

        let mut seq_id = self.sequence_id.write().await;
        *seq_id += 1;
        let sequence_id = *seq_id;
        drop(seq_id);

        let request = QueryRequest {
            sql_text: sql.to_string(),
            async_exec: false,
            sequence_id,
            parameters: None,
            bindings: None,
        };

        let url = format!("{}/queries/v1/query-request?requestId={}", self.host, uuid::Uuid::new_v4());
        
        eprintln!("DEBUG: Sending query with Accept: application/snowflake");
        
        let response = self
            .http_client
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(USER_AGENT, format!("{}/{}", CLIENT_APP_ID, CLIENT_APP_VERSION))
            .header(AUTHORIZATION, format!("Snowflake Token=\"{}\"", token))
            // Request Arrow format results
            .header("Accept", "application/snowflake")
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::ConnectionFailed {
                message: format!("HTTP request failed: {}", e),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::QueryFailed {
                message: format!("Query failed with status {}: {}", status, error_text),
            });
        }

        let query_response: QueryResponse = response.json().await.map_err(|e| {
            Error::QueryFailed {
                message: format!("Failed to parse query response: {}", e),
            }
        })?;

        if !query_response.success {
            return Err(Error::QueryFailed {
                message: query_response.message.unwrap_or_else(|| "Unknown error".to_string()),
            });
        }

        Ok(query_response)
    }

    pub async fn download_arrow_chunk(&self, chunk: &ChunkInfo) -> Result<bytes::Bytes> {
        let token = self.token.read().await;
        let token = token.as_ref().ok_or(Error::NotInitialized)?;

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(CLIENT_APP_ID));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Snowflake Token=\"{}\"", token)).map_err(|e| {
                Error::InvalidState {
                    message: format!("Invalid token format: {}", e),
                }
            })?,
        );
        // Accept Arrow format
        headers.insert("Accept", HeaderValue::from_static("application/snowflake"));

        let response = self
            .http_client
            .get(&chunk.url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| Error::ConnectionFailed {
                message: format!("Failed to download chunk: {}", e),
            })?;

        if !response.status().is_success() {
            return Err(Error::QueryFailed {
                message: format!("Failed to download chunk: {}", response.status()),
            });
        }

        response.bytes().await.map_err(|e| Error::ConnectionFailed {
            message: format!("Failed to read chunk bytes: {}", e),
        })
    }
}
