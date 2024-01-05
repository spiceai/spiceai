mod client;
mod flight;
mod prices;
mod tls;

pub use client::SpiceClient as Client;

// Further public exports and integrations
pub use futures::StreamExt;
