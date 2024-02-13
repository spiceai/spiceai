use crate::auth::AuthProvider;

use std::collections::HashMap;
use std::{future::Future, pin::Pin};
use std::{sync::Arc, time::Duration};

use super::{DataConnector, DataUpdate, UpdateType};
use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_stream::stream;
use futures_core::stream::BoxStream;
use spicepod::component::dataset::Dataset;

#[allow(clippy::module_name_repetitions)]
pub struct DebugSource {}

impl DataConnector for DebugSource {
    fn new(
        _auth_provider: AuthProvider,
        _params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>>>> {
        Box::pin(async move { Ok(Self {}) })
    }

    fn supports_data_streaming(&self, _dataset: &Dataset) -> bool {
        true
    }

    fn get_all_data(
        &self,
        _dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<RecordBatch>> + Send>> {
        Box::pin(async move {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Int32, false),
            ]));
            if let Ok(batch) = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                    Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
                ],
            ) {
                vec![batch]
            } else {
                vec![]
            }
        })
    }

    fn stream_data_updates<'a>(&self, dataset: &Dataset) -> BoxStream<'a, DataUpdate> {
        let sleep_duration = dataset.refresh_interval();
        let sleep_duration = match sleep_duration {
            Some(duration) => duration,
            None => Duration::from_secs(1),
        };
        Box::pin(stream! {
          loop {
              tokio::time::sleep(sleep_duration).await;

              // Register test in-memory data.
              let schema = Arc::new(Schema::new(vec![
                  Field::new("a", DataType::Utf8, false),
                  Field::new("b", DataType::Int32, false),
              ]));
              if let Ok(batch) = RecordBatch::try_new(
                  schema,
                  vec![
                      Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                      Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
                  ],
              ) {
                yield DataUpdate {
                  log_sequence_number: None,
                  update_type: UpdateType::Append,
                  data: vec![batch],
                };
              };
          }
        })
    }
}
