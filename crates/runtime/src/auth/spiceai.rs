use super::Auth;

pub struct SpiceAuth {
    api_key: String,
}

impl Auth for SpiceAuth {
    fn get_token(&self) -> String {
        self.api_key.clone()
    }
}

impl SpiceAuth {
    #[must_use]
    pub fn new(api_key: String) -> Self {
        SpiceAuth { api_key }
    }
}
