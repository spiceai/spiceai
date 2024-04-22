#![doc = include_str!("../README.md")]

mod client;
mod config;
mod flight;
mod tls;

pub use client::SpiceClient as Client;

// Further public exports and integrations
pub use futures::StreamExt;
