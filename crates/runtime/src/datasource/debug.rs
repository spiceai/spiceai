use async_stream::stream;
use futures_core::stream::Stream;
use std::time::Duration;

#[allow(clippy::module_name_repetitions)]
pub struct DebugSource {
    pub sleep_duration: Duration,
}

impl super::DataSource for DebugSource {
    fn get_data(&mut self) -> impl Stream<Item = super::DataUpdate> {
        stream! {
          loop {
              tokio::time::sleep(self.sleep_duration).await;
              yield super::DataUpdate {
                  log_sequence_number: 0,
                  data: vec![],
              };
          }
        }
    }
}
