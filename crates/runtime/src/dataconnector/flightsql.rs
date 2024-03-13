use std::collections::HashMap;
use std::pin::Pin;
use std::{future::Future, sync::Arc};

use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::Ticket;
use async_trait::async_trait;
use spicepod::component::dataset::Dataset;
use tonic::transport::Channel;

use crate::secrets::Secret;

use flight_client::tls::new_tls_flight_channel;

use super::DataConnector;
use arrow::error::ArrowError;
use futures::stream::TryStreamExt;

#[derive(Debug, Clone)]
pub struct FlightSQL {
    pub client: FlightSqlServiceClient<Channel>,
}

#[async_trait]
impl DataConnector for FlightSQL {
    fn new(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>> + Send>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let endpoint: String = params
                .as_ref() // &Option<HashMap<String, String>>
                .as_ref() // Option<&HashMap<String, String>>
                .and_then(|params| params.get("endpoint").cloned())
                .ok_or_else(|| super::Error::UnableToCreateDataConnector {
                    source: "Missing required parameter: endpoint".into(),
                })?;

            let flight_channel = new_tls_flight_channel(&endpoint)
                .await
                .map_err(|e| super::Error::UnableToCreateDataConnector { source: e.into() })?;

            let mut client = FlightSqlServiceClient::new(flight_channel);

            if let Some(s) = secret {
                let _ = client
                    .handshake(
                        s.get("username").unwrap_or_default(),
                        s.get("password").unwrap_or_default(),
                    )
                    .await;
            };

            Ok(Self { client })
        })
    }

    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        let dataset_path = dataset.path().clone();
        let client = self.client.clone();

        Box::pin(async move {
            let mut batches = vec![];
            if let Ok(flight_info) = client
                .clone()
                .execute(format!("SELECT * FROM {dataset_path}"), None)
                .await
            {
                for ep in &flight_info.endpoint {
                    if let Some(tkt) = &ep.ticket {
                        match batch_from_ticket(&mut client.clone(), tkt.to_owned()).await {
                            Ok(flight_data) => batches.extend(flight_data),
                            Err(err) => {
                                tracing::error!(
                                    "Failed to read batch from flight client: {:?}",
                                    err
                                );
                                break;
                            }
                        }
                    };
                }
            }
            batches
        })
    }

    fn has_table_provider(&self) -> bool {
        false // TODO: Implement this.
    }

    async fn get_table_provider(
        &self,
        _dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        Err(super::Error::UnableToGetTableProvider {
            source: "Not implemented".into(),
        })
    }
}

async fn batch_from_ticket(
    client: &mut FlightSqlServiceClient<Channel>,
    ticket: Ticket,
) -> Result<Vec<arrow::record_batch::RecordBatch>, ArrowError> {
    let flight_data = client
        .do_get(ticket)
        .await?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|err| ArrowError::CastError(format!("Cannot collect flight data: {err:#?}")))?;
    Ok(flight_data)
}
