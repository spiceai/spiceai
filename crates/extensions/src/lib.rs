pub trait Extension {
    fn name(&self) -> &'static str;
    fn initialize(&mut self, runtime: Box<&mut dyn Runtime>);
    fn on_start(&mut self, runtime: Box<&mut dyn Runtime>);
}

pub trait Runtime {
    fn register_extension(&mut self, extension: Box<&mut dyn Extension>);
}
