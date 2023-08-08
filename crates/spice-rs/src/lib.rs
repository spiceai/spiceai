
mod client;
mod flight;

pub use client::{Client, RequestOptions};
pub use flight::{FlightClient, FlightDescriptor, Ticket, FlightInfo, FlightEndpoint, Location, FlightData};
pub use flight::DescriptorType;

// Further public exports and integrations
