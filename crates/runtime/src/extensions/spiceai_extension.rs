use super::{Extension, Runtime};

pub struct SpiceExtension {}

impl Extension for SpiceExtension {
    fn name(&self) -> &'static str {
        "spiceai"
    }

    fn initialize(&mut self, _runtime: Box<&mut dyn Runtime>) {
        tracing::info!("Initializing Spiceai Extension");
    }

    fn on_start(&mut self, _runtime: Box<&mut dyn Runtime>) {
        tracing::info!("Starting Spiceai Extension");
    }
}
