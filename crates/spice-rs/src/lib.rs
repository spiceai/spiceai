mod client;
mod flight;
mod prices;
mod tls;

pub use client::{new_spice_client, SpiceClient as Client};

// Further public exports and integrations
