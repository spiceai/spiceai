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
use csv::Writer;
use flight_client::FlightClient;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tonic_0_9_0::transport::Channel;
use tonic_health::{pb::health_client::HealthClient, ServingStatus};

use axum::{
    extract::Query,
    http::status,
    response::{IntoResponse, Response},
    Extension, Json,
};

use crate::{config, status::ComponentStatus};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct QueryParams {
    #[serde(default = "default_format")]
    format: Format,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Json,
    Csv,
}

#[derive(Deserialize, Serialize)]
pub struct ConnectionDetails {
    name: &'static str,
    endpoint: String,
    status: ComponentStatus,
}

fn default_format() -> Format {
    Format::Json
}

pub(crate) async fn get(
    Extension(cfg): Extension<Arc<config::Config>>,
    Extension(with_metrics): Extension<Option<SocketAddr>>,
    Query(params): Query<QueryParams>,
) -> Response {
    let cfg = cfg.as_ref();
    let flight_url = cfg.flight_bind_address.to_string();

    let details = vec![
        ConnectionDetails {
            name: "http",
            endpoint: cfg.http_bind_address.to_string(),
            status: ComponentStatus::Ready,
        },
        ConnectionDetails {
            name: "flight",
            status: get_flight_status(&flight_url).await,
            endpoint: flight_url,
        },
        ConnectionDetails {
            name: "metrics",
            endpoint: with_metrics.map_or("N/A".to_string(), |addr| addr.to_string()),
            status: match with_metrics {
                Some(metrics_url) => match get_metrics_status(&metrics_url.to_string()).await {
                    Ok(status) => status,
                    Err(e) => {
                        tracing::error!("Error getting metrics status from {metrics_url}: {e}");
                        ComponentStatus::Error
                    }
                },
                None => ComponentStatus::Disabled,
            },
        },
        ConnectionDetails {
            name: "opentelemetry",
            status: match get_opentelemetry_status(
                cfg.open_telemetry_bind_address.to_string().as_str(),
            )
            .await
            {
                Ok(status) => status,
                Err(e) => {
                    tracing::error!(
                        "Error getting opentelemetry status from {}: {}",
                        cfg.open_telemetry_bind_address,
                        e
                    );
                    ComponentStatus::Error
                }
            },
            endpoint: cfg.open_telemetry_bind_address.to_string(),
        },
    ];

    match params.format {
        Format::Json => (status::StatusCode::OK, Json(details)).into_response(),
        Format::Csv => match convert_details_to_csv(&details) {
            Ok(csv) => (status::StatusCode::OK, csv).into_response(),
            Err(e) => {
                tracing::error!("Error converting to CSV: {e}");
                (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        },
    }
}

fn convert_details_to_csv(
    details: &[ConnectionDetails],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut w = Writer::from_writer(vec![]);
    for d in details {
        let _ = w.serialize(d);
    }
    w.flush()?;
    Ok(String::from_utf8(w.into_inner()?)?)
}

async fn get_flight_status(flight_addr: &str) -> ComponentStatus {
    tracing::trace!("Checking flight status at {flight_addr}");
    match FlightClient::new(&format!("http://{flight_addr}"), "", "").await {
        Ok(_) => ComponentStatus::Ready,
        Err(e) => {
            tracing::error!("Error connecting to flight when checking status: {e}");
            ComponentStatus::Error
        }
    }
}

async fn get_metrics_status(
    metrics_addr: &str,
) -> Result<ComponentStatus, Box<dyn std::error::Error>> {
    let resp = reqwest::get(format!("http://{metrics_addr}/health")).await?;
    if resp.status().is_success() && resp.text().await? == "OK" {
        Ok(ComponentStatus::Ready)
    } else {
        Ok(ComponentStatus::Error)
    }
}
async fn get_opentelemetry_status(
    addr: &str,
) -> Result<ComponentStatus, Box<dyn std::error::Error>> {
    let channel = Channel::from_shared(format!("http://{addr}"))?
        .connect()
        .await?;

    let mut client = HealthClient::new(channel);

    let resp = client
        .check(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await?;

    if resp.into_inner().status == ServingStatus::Serving as i32 {
        Ok(ComponentStatus::Ready)
    } else {
        Ok(ComponentStatus::Error)
    }
}
