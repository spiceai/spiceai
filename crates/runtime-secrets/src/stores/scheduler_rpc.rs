use crate::{AnyErrorResult, SecretStore};
use async_trait::async_trait;
use futures::StreamExt;
use prost::{Message, bytes};
use runtime_proto::{ExecutorExpandSecretRequest, ExecutorExpandSecretResponse};
use secrecy::SecretString;
use snafu::ResultExt;

/// Used by cluster mode to resolve secrets declared in the scheduler
/// via flight RPC
pub struct SchedulerRPCSecretStore {
    executor_id: String,
    flight_client: arrow_flight::FlightClient,
}

impl SchedulerRPCSecretStore {
    #[must_use]
    pub fn new(flight_client: arrow_flight::FlightClient, executor_id: String) -> Self {
        Self {
            executor_id,
            flight_client,
        }
    }

    fn client(&self) -> arrow_flight::FlightClient {
        let meta = self.flight_client.metadata().clone();
        let mut client =
            arrow_flight::FlightClient::new_from_inner(self.flight_client.inner().clone());
        *client.metadata_mut() = meta;
        client
    }
}

#[async_trait]
impl SecretStore for SchedulerRPCSecretStore {
    async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
        tracing::trace!("SchedulerRPCSecretStore: Requesting secret {}", key);

        let request = ExecutorExpandSecretRequest {
            executor_id: self.executor_id.clone(),
            key: key.to_string(),
        };

        let action = arrow_flight::Action {
            r#type: "ExpandSecret".to_string(),
            body: bytes::Bytes::from(request.encode_to_vec()),
        };

        let response = self.client().do_action(action).await.boxed()?.next().await;

        match response {
            Some(Ok(mut bytes)) => Ok(Some(SecretString::from(
                ExecutorExpandSecretResponse::decode(&mut bytes)?.value,
            ))),
            Some(Err(e)) => Err(e.into()),
            None => Err("Secrets RPC returned no response".into()),
        }
    }
}
