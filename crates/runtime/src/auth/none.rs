use super::AuthProvider;

#[allow(clippy::module_name_repetitions)]
pub struct NoneAuth {}

impl AuthProvider for NoneAuth {
    fn get_token(&self) -> String {
        String::new()
    }
}

impl NoneAuth {
    #[must_use]
    pub fn new() -> Self {
        NoneAuth {}
    }
}

impl Default for NoneAuth {
    fn default() -> Self {
        Self::new()
    }
}
