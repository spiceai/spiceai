use super::Auth;
use super::AuthProvider;

pub struct SpiceAuth {
    api_key: String,
}

impl AuthProvider for SpiceAuth {
    #[must_use]
    fn new(auth: &Auth) -> Self
    where
        Self: Sized,
    {
        SpiceAuth {
            api_key: auth.key.clone(),
        }
    }

    fn get_token(&self) -> String {
        self.api_key.clone()
    }
}
