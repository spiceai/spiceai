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

use tonic::Status;

use crate::{datafusion::Error, flight::Service};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionAcceleratedDatasetRefreshRequest {
    #[prost(string, tag = "1")]
    pub dataset_name: ::prost::alloc::string::String,
}

pub(crate) async fn do_action_accelerated_dataset_refresh(
    flight_svc: &Service,
    cmd: ActionAcceleratedDatasetRefreshRequest,
) -> Result<(), Status> {
    tracing::trace!("do_action_accelerated_dataset_refresh: {cmd:?}");

    let dataset_name = &cmd.dataset_name;

    match flight_svc.datafusion.refresh_table(&cmd.dataset_name).await {
        Ok(()) => {
            tracing::info!("Dataset refresh triggered for {dataset_name}.");
            Ok(())
        }
        Err(e) => {
            let err_message = format!("Error refreshing dataset {dataset_name}: {e}");
            tracing::error!(err_message);
            match e {
                Error::UnableToGetTable { .. } | Error::NotAcceleratedTable { .. } => {
                    Err(Status::invalid_argument(err_message))
                }
                _ => Err(Status::internal(err_message)),
            }
        }
    }
}
