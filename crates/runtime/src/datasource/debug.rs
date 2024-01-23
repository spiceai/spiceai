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

impl super::DataSource for DebugSource {
    fn get_data<'a>(&self) -> BoxStream<'a, super::DataUpdate> {
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
                yield super::DataUpdate {
                  log_sequence_number: 0,
                  data: vec![batch],
                };
              };
          }
        })
    }
}
