use keyring::Result;

pub struct KeyringSecretStore;

impl KeyringSecretStore {
    fn get(&self, name: &str) -> Result<()> {
        // let entry = Entry::new(name, "my_app");

        Ok(())
    }

    fn init(&mut self) -> Result<()> {
        // TBD:
        Ok(())
    }
}
