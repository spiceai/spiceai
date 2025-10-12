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

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::Error;
use crate::Runtime;

/// Management event that can be received from Spice Cloud
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "event")]
pub enum ManagementEvent {
    #[serde(rename = "sql")]
    Sql { data: SqlEventData },
    #[serde(rename = "ping")]
    Ping,
    #[serde(other)]
    Unknown,
}

/// SQL event data containing the query to execute
#[derive(Debug, Deserialize, Serialize)]
pub struct SqlEventData {
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Transport protocol for connecting to Spice Cloud
#[derive(Debug, Clone, PartialEq)]
pub enum TransportProtocol {
    /// HTTP Server-Sent Events (SSE)
    HttpSse,
    /// Apache Arrow Flight RPC
    ArrowFlight,
}

impl TransportProtocol {
    /// Detect transport protocol from endpoint URL
    pub fn from_endpoint(endpoint: &str) -> Self {
        if endpoint.starts_with("grpc://")
            || endpoint.starts_with("grpc+tls://")
            || endpoint.contains(":443") && !endpoint.starts_with("http")
        {
            Self::ArrowFlight
        } else {
            Self::HttpSse
        }
    }

    /// Get default endpoint for this transport protocol
    pub fn default_endpoint(&self) -> &'static str {
        match self {
            Self::HttpSse => "https://data.spiceai.io/v1/connect",
            Self::ArrowFlight => "flight.spiceai.io:443",
        }
    }

    /// Parse transport from string ("flight" or "http-sse")
    pub fn from_string(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "flight" => Some(Self::ArrowFlight),
            "http-sse" => Some(Self::HttpSse),
            _ => None,
        }
    }
}

/// Trait for transport implementations
#[async_trait]
pub trait Transport: Send + Sync {
    /// Start the transport connection and listen for events
    async fn start(&self) -> Result<(), Error>;

    /// Get the transport protocol type
    fn protocol(&self) -> TransportProtocol;
}

/// Factory for creating transport instances
pub struct TransportFactory;

impl TransportFactory {
    /// Create a transport based on the endpoint URL
    pub async fn create(
        runtime: Arc<Runtime>,
        endpoint: String,
        api_key: String,
    ) -> Result<Box<dyn Transport>, Error> {
        let protocol = TransportProtocol::from_endpoint(&endpoint);

        match protocol {
            TransportProtocol::HttpSse => {
                let transport =
                    super::http_sse_transport::HttpSseTransport::new(runtime, endpoint, api_key);
                Ok(Box::new(transport))
            }
            TransportProtocol::ArrowFlight => {
                let transport =
                    super::flight_transport::FlightTransport::new(runtime, endpoint, api_key)
                        .await?;
                Ok(Box::new(transport))
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_transport_protocol_detection() {
        assert_eq!(
            TransportProtocol::from_endpoint("https://data.spiceai.io/v1/connect"),
            TransportProtocol::HttpSse
        );

        assert_eq!(
            TransportProtocol::from_endpoint("http://localhost:8080/connect"),
            TransportProtocol::HttpSse
        );

        assert_eq!(
            TransportProtocol::from_endpoint("grpc+tls://flight.spiceai.io:443"),
            TransportProtocol::ArrowFlight
        );

        assert_eq!(
            TransportProtocol::from_endpoint("grpc://localhost:50051"),
            TransportProtocol::ArrowFlight
        );

        assert_eq!(
            TransportProtocol::from_endpoint("flight.spiceai.io:443"),
            TransportProtocol::ArrowFlight
        );
    }

    #[test]
    fn test_default_endpoints() {
        assert_eq!(
            TransportProtocol::HttpSse.default_endpoint(),
            "https://data.spiceai.io/v1/connect"
        );

        assert_eq!(
            TransportProtocol::ArrowFlight.default_endpoint(),
            "flight.spiceai.io:443"
        );
    }

    #[test]
    fn test_transport_from_string() {
        assert_eq!(
            TransportProtocol::from_string("flight"),
            Some(TransportProtocol::ArrowFlight)
        );

        assert_eq!(
            TransportProtocol::from_string("FLIGHT"),
            Some(TransportProtocol::ArrowFlight)
        );

        assert_eq!(
            TransportProtocol::from_string("http-sse"),
            Some(TransportProtocol::HttpSse)
        );

        assert_eq!(
            TransportProtocol::from_string("HTTP-SSE"),
            Some(TransportProtocol::HttpSse)
        );

        // Unsupported values should return None
        assert_eq!(TransportProtocol::from_string("sse"), None);
        assert_eq!(TransportProtocol::from_string("http"), None);
        assert_eq!(TransportProtocol::from_string("grpc"), None);
        assert_eq!(TransportProtocol::from_string("arrow-flight"), None);
        assert_eq!(TransportProtocol::from_string("unknown"), None);
    }

    #[test]
    fn test_management_event_deserialization() {
        // Test SQL event
        let sql_event =
            r#"{"event":"sql","data":{"query":"SELECT * FROM table","request_id":"123"}}"#;
        let event: ManagementEvent = serde_json::from_str(sql_event).unwrap();
        match event {
            ManagementEvent::Sql { data } => {
                assert_eq!(data.query, "SELECT * FROM table");
                assert_eq!(data.request_id, Some("123".to_string()));
            }
            _ => panic!("Expected SQL event"),
        }

        // Test SQL event without request_id
        let sql_event_no_id = r#"{"event":"sql","data":{"query":"SELECT 1"}}"#;
        let event: ManagementEvent = serde_json::from_str(sql_event_no_id).unwrap();
        match event {
            ManagementEvent::Sql { data } => {
                assert_eq!(data.query, "SELECT 1");
                assert_eq!(data.request_id, None);
            }
            _ => panic!("Expected SQL event"),
        }

        // Test ping event
        let ping_event = r#"{"event":"ping"}"#;
        let event: ManagementEvent = serde_json::from_str(ping_event).unwrap();
        assert!(matches!(event, ManagementEvent::Ping));

        // Test unknown event
        let unknown_event = r#"{"event":"unknown_type","data":{}}"#;
        let event: ManagementEvent = serde_json::from_str(unknown_event).unwrap();
        assert!(matches!(event, ManagementEvent::Unknown));
    }
}
