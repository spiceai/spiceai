use std::time::SystemTime;

use change_event::ChangeEvent;
use futures::{stream, Stream};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};

use crate::dataupdate::{changes::ChangeEvent as DataUpdateChangeEvent, DataUpdate, UpdateType};

use super::refresh_task::RefreshTask;

mod change_event;

impl RefreshTask {
    #[allow(clippy::missing_panics_doc)]
    pub fn get_changes_stream(
        &self,
    ) -> impl Stream<Item = super::Result<(Option<SystemTime>, DataUpdate)>> + '_ {
        let group_id = "consume_kafka";
        let brokers = "localhost:19092";
        let Ok(consumer) = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers)
            .set("auto.offset.reset", "smallest")
            .create::<StreamConsumer>()
        else {
            panic!("Failed to create consumer");
        };

        tracing::info!("Consumer created");

        if consumer
            .subscribe(&["acceleration.public.customer_addresses"])
            .is_err()
        {
            panic!("Failed to subscribe to topic");
        };

        let stream = stream::unfold(consumer, |consumer| async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        let timestamp = SystemTime::now();
                        let Some(payload_bytes) = m.payload() else {
                            // This is a tombstone message that occurs after a deletion message and can be ignored since we've already processed the delete.
                            continue;
                        };
                        let mut change_event =
                            ChangeEvent::from_bytes(payload_bytes).expect("Change event");
                        change_event.set_schema(self.schema());

                        let data_update_change_event =
                            vec![DataUpdateChangeEvent::from(change_event)];

                        let change_batch =
                            DataUpdateChangeEvent::to_record_batch(&data_update_change_event);

                        let data_update = DataUpdate {
                            schema: change_batch.schema(),
                            data: vec![change_batch],
                            update_type: UpdateType::Changes,
                        };

                        return Some((Ok((Some(timestamp), data_update)), consumer));
                    }
                    Err(e) => {
                        tracing::error!("Error receiving message: {:?}", e);
                        return None;
                    }
                }
            }
        });

        stream
    }
}
