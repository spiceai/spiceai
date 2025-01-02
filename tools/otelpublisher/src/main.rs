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

use clap::Parser;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::{
        metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest,
    },
    common::v1::{any_value::Value, AnyValue, InstrumentationScope, KeyValue},
    metrics::v1::{
        metric::Data, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics,
    },
    resource::v1::Resource,
};
use tonic::{
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig},
    IntoRequest,
};

#[derive(Parser)]
#[clap(about = "Spice.ai Open Telemetry Publisher Utility")]
pub struct Args {
    #[arg(
        long,
        value_name = "OTEL_ENDPOINT",
        default_value = "http://localhost:50052"
    )]
    pub otel_endpoint: String,

    /// Path to the root certificate file to use to verify server's TLS certificate
    #[arg(long, value_name = "TLS_ROOT_CERTIFICATE_FILE")]
    pub tls_root_certificate_file: Option<String>,

    /// API key for the Open Telemetry endpoint
    #[arg(long, value_name = "API_KEY")]
    pub api_key: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Set up the Otel client
    let mut otel_endpoint = args.otel_endpoint;
    let channel = if let Some(tls_root_certificate_file) = args.tls_root_certificate_file {
        let tls_root_certificate = std::fs::read(tls_root_certificate_file)?;
        let tls_root_certificate = tonic::transport::Certificate::from_pem(tls_root_certificate);
        let client_tls_config = ClientTlsConfig::new().ca_certificate(tls_root_certificate);
        if otel_endpoint == "http://localhost:50052" {
            otel_endpoint = "https://localhost:50052".to_string();
        }
        Channel::from_shared(otel_endpoint)?
            .tls_config(client_tls_config)?
            .connect()
            .await
    } else {
        Channel::from_shared(otel_endpoint)?.connect().await
    }?;
    let mut client = MetricsServiceClient::new(channel);

    let mut request = ExportMetricsServiceRequest::default();
    request.resource_metrics.push(ResourceMetrics {
        resource: Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("test".to_string())),
                }),
            }],
            ..Default::default()
        }),
        scope_metrics: vec![ScopeMetrics {
            scope: Some(InstrumentationScope {
                name: "test".to_string(),
                ..Default::default()
            }),
            metrics: vec![Metric {
                name: "test".to_string(),
                data: Some(Data::Gauge(Gauge {
                    data_points: vec![NumberDataPoint {
                        value: Some(number_data_point::Value::AsInt(1)),
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            }],
            ..Default::default()
        }],
        schema_url: String::new(),
    });

    let mut request = request.into_request();

    if let Some(api_key) = args.api_key {
        let metadata_value = match MetadataValue::try_from(api_key) {
            Ok(metadata_value) => metadata_value,
            Err(e) => panic!("Invalid API key: {e}"),
        };
        request.metadata_mut().insert("x-api-key", metadata_value);
    }

    client.export(request).await?;

    println!("Data sent to Open Telemetry endpoint.");

    Ok(())
}
