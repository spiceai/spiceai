#![doc = include_str!("../README.md")]

mod client;
mod config;
mod flight;
mod tls;
mod util;

pub use client::SpiceClient as Client;
pub use client::SpiceClientBuilder as ClientBuilder;

// Further public exports and integrations
pub use futures::StreamExt;
