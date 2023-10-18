
mod client;
mod flight;
mod prices; 
mod tls;

pub use client::{SpiceClient as Client};
pub use flight::{SqlFlightClient};
pub use client::new_spice_client;

// Further public exports and integrations
