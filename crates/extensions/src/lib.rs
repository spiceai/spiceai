// Within your extension, you can do "whatever you want"
// 
// During initialize, you can set runtime state

pub trait Extension {
    fn name(&self) -> &'static str;
    fn initialize(&mut self, runtime: &mut Runtime, extension_config: Option<ExtensionConfig>) -> Result<()>;
    // initialize will register its own data connectors/data accelerators/secrets provider
    // fn data_connectors(&self) -> Option<Vec<Box<dyn DataConnector>>>;
    // fn data_accelerators(&self) -> Option<Vec<Box<dyn DataConnector>>>;
    // fn rt.set_operator(&self) -> Option<Box<dyn Operator>>;
}

/// yaml
/// 
/// extensions:
///   spiceai:
///     use_secret_api: true

// Only one operator is allowed globally
pub trait Operator {
    fn name(&self) -> &'static str;
    async fn initialize(&mut self) -> Result<()>;
    // this initialize will change runtime state to set the data connector for the metrics replication
}


impl Runtime {
  fn set_operator(&mut self, operator: Box<dyn Operator>) -> Result<()> {
    self.operator = Some(operator);
  }
}