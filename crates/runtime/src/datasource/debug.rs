use super::{DataSource, DataUpdate, UpdateType};
use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_stream::stream;
use futures_core::stream::BoxStream;
use std::{sync::Arc, time::Duration};

#[allow(clippy::module_name_repetitions)]
pub struct DebugSource {
    pub sleep_duration: Duration,
}

impl DataSource for DebugSource {
    fn new<T: crate::auth::Auth>(auth: T) -> Self {
        Self {
            sleep_duration: Duration::from_secs(1),
        }
    }

    fn supports_data_streaming(&self, dataset: &str) -> bool {
        true
    }

    fn get_all_data(&self, dataset: &str) -> Vec<RecordBatch> {
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
    }

    fn stream_data_updates<'a>(&self, dataset: &str) -> BoxStream<'a, DataUpdate> {
        let sleep_duration = self.sleep_duration;
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
