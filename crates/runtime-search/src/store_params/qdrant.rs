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

use std::str::FromStr;
use std::sync::Arc;

use qdrant::QdrantConnection;
use runtime_parameters::TypedParams;
use secrecy::{ExposeSecret, SecretString};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QdrantDistanceMetric {
    Cosine,
    Euclid,
    Dot,
    Manhattan,
}

impl QdrantDistanceMetric {
    #[must_use]
    pub fn distance(self) -> qdrant::proto::Distance {
        match self {
            QdrantDistanceMetric::Cosine => qdrant::proto::Distance::Cosine,
            QdrantDistanceMetric::Euclid => qdrant::proto::Distance::Euclid,
            QdrantDistanceMetric::Dot => qdrant::proto::Distance::Dot,
            QdrantDistanceMetric::Manhattan => qdrant::proto::Distance::Manhattan,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            QdrantDistanceMetric::Cosine => "cosine",
            QdrantDistanceMetric::Euclid => "euclidean",
            QdrantDistanceMetric::Dot => "dot",
            QdrantDistanceMetric::Manhattan => "manhattan",
        }
    }
}

impl FromStr for QdrantDistanceMetric {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "cosine" => Ok(QdrantDistanceMetric::Cosine),
            "euclid" | "euclidean" => Ok(QdrantDistanceMetric::Euclid),
            "dot" | "dot_product" => Ok(QdrantDistanceMetric::Dot),
            "manhattan" => Ok(QdrantDistanceMetric::Manhattan),
            other => Err(format!(
                "Expected one of: cosine | euclid | dot | manhattan. Found {other}."
            )),
        }
    }
}

#[derive(Debug, TypedParams)]
#[params(prefix = "qdrant")]
pub struct QdrantVectorParams {
    pub endpoint: String,
    #[param(autoload_secret)]
    pub api_key: Option<SecretString>,
    pub collection: Option<String>,
    pub distance_metric: Option<QdrantDistanceMetric>,
    #[param(runtime, parse_with = duration_parse::parse_duration)]
    pub timeout: Option<std::time::Duration>,
    #[param(default = "1000")]
    pub batch_write_rows: usize,
}

impl QdrantVectorParams {
    #[must_use]
    pub fn connection(&self) -> QdrantConnection {
        QdrantConnection {
            endpoint: self.endpoint.clone(),
            api_key: self
                .api_key
                .as_ref()
                .map(|k| ExposeSecret::expose_secret(k).to_string()),
            connect_timeout: self.timeout,
        }
    }
}

/// Builds a Qdrant client from validated vector-store parameters.
///
/// # Errors
///
/// Returns an error if the client cannot be built from the endpoint,
/// API key, and timeout configured on `params`.
pub fn client_from_params(
    params: &QdrantVectorParams,
) -> Result<Arc<qdrant::Qdrant>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(Arc::new(qdrant::Qdrant::new(&params.connection())?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_metric_parses_aliases_and_round_trips() {
        assert_eq!(
            "cosine".parse::<QdrantDistanceMetric>().expect("cosine"),
            QdrantDistanceMetric::Cosine
        );
        assert_eq!(
            "euclidean"
                .parse::<QdrantDistanceMetric>()
                .expect("euclidean"),
            QdrantDistanceMetric::Euclid
        );
        assert_eq!(
            "dot_product".parse::<QdrantDistanceMetric>().expect("dot"),
            QdrantDistanceMetric::Dot
        );
        assert_eq!(
            "manhattan"
                .parse::<QdrantDistanceMetric>()
                .expect("manhattan"),
            QdrantDistanceMetric::Manhattan
        );
        assert!(
            "unknown".parse::<QdrantDistanceMetric>().is_err(),
            "expected unknown distance metric to fail parsing"
        );

        for metric in [
            QdrantDistanceMetric::Cosine,
            QdrantDistanceMetric::Euclid,
            QdrantDistanceMetric::Dot,
            QdrantDistanceMetric::Manhattan,
        ] {
            assert_eq!(
                metric
                    .as_str()
                    .parse::<QdrantDistanceMetric>()
                    .expect("round trip"),
                metric
            );
        }
    }
}
