
use keyring::{Entry, Result};

impl KeyringSecretStore {
    fn get(&self, name: &str) -> AuthProvider {
        // TBD:
    }

    fn init(&mut self) -> Result<()> {
        // TBD:
        Ok(())
    }
}