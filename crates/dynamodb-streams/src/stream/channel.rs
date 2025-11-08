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
use tokio::sync::oneshot::{self, Receiver, Sender, error::TryRecvError};
use tracing::error;

pub fn new() -> (ProducerChannel, ConsumerChannel) {
    let (tx_init, rx_init) = oneshot::channel::<()>();
    let (tx_close, rx_close) = oneshot::channel::<()>();
    (
        ProducerChannel::new(tx_init, rx_close),
        ConsumerChannel::new(tx_close, rx_init),
    )
}

#[derive(Debug)]
pub struct ProducerChannel {
    sender: Option<Sender<()>>,

    receiver: Receiver<()>,
}

impl ProducerChannel {
    fn new(sender: Sender<()>, receiver: Receiver<()>) -> Self {
        Self {
            sender: Some(sender),
            receiver,
        }
    }

    /// Send `Initialized` event to the channel half.
    pub fn send_init(&mut self) {
        if let Some(tx) = self.sender.take()
            && let Err(err) = tx.send(())
        {
            error!(
                "Unexpected error during sending initialized event: {:?}",
                err
            );
        }
    }

    /// Return true if the `Stop polling` event is received.
    pub fn should_close(&mut self) -> bool {
        !matches!(self.receiver.try_recv(), Err(TryRecvError::Empty))
    }
}

#[derive(Debug)]
pub struct ConsumerChannel {
    sender: Option<Sender<()>>,

    receiver: Receiver<()>,
}

impl ConsumerChannel {
    fn new(sender: Sender<()>, receiver: Receiver<()>) -> Self {
        Self {
            sender: Some(sender),
            receiver,
        }
    }

    /// Send `Stop polling` event to the stream. The passed closure is executed only when
    /// sending event fails.
    pub fn close(&mut self, f: impl FnOnce()) {
        if let Some(tx) = self.sender.take() {
            let _ = tx.send(()).map_err(|()| f());
        }
    }

    /// Return true if the stream is ready to polling.
    pub fn initialized(&mut self) -> bool {
        matches!(self.receiver.try_recv(), Err(TryRecvError::Closed))
    }
}
