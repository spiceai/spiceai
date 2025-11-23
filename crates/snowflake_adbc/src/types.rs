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

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequest {
    pub data: LoginRequestData,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct LoginRequestData {
    pub account_name: String,
    pub login_name: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub authenticator: Option<String>,
    pub session_parameters: Option<serde_json::Value>,
    pub client_app_id: String,
    pub client_app_version: String,
}

#[derive(Debug, Deserialize)]
pub struct LoginResponse {
    pub data: LoginResponseData,
    pub code: Option<String>,
    pub message: Option<String>,
    pub success: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponseData {
    pub token: Option<String>,
    pub master_token: Option<String>,
    pub session_id: Option<i64>,
    pub parameters: Option<Vec<NameValueParameter>>,
}

#[derive(Debug, Deserialize)]
pub struct NameValueParameter {
    pub name: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    pub sql_text: String,
    pub async_exec: bool,
    pub sequence_id: u64,
    pub parameters: Option<serde_json::Value>,
    pub bindings: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct QueryResponse {
    pub data: QueryResponseData,
    pub code: Option<String>,
    pub message: Option<String>,
    pub success: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponseData {
    pub query_id: String,
    pub returned: i64,
    pub total: i64,
    pub query_result_format: String,
    pub parameters: Option<Vec<NameValueParameter>>,
    pub row_type: Option<Vec<RowType>>,
    pub row_set: Option<Vec<Vec<Option<String>>>>,
    pub row_set_base64: Option<String>,
    pub chunks: Option<Vec<ChunkInfo>>,
    pub qrmk: Option<String>,
    pub chunk_headers: Option<serde_json::Value>,
    pub sql_state: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RowType {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub length: Option<i64>,
    pub precision: Option<i64>,
    pub scale: Option<i64>,
    pub nullable: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ChunkInfo {
    pub url: String,
    pub row_count: i64,
    pub uncompressed_size: Option<i64>,
    pub compressed_size: Option<i64>,
}
