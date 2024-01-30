use super::Auth;
use super::AuthProvider;

#[allow(clippy::module_name_repetitions)]
pub struct DremioAuth {
    username: String,
    password: String,
}

impl AuthProvider for DremioAuth {
    #[must_use]
    fn new(auth: &Auth) -> Self
    where
        Self: Sized,
    {
        DremioAuth {
            username: auth.username.clone().unwrap_or_default(),
            password: auth.password.clone().unwrap_or_default(),
        }
    }

    fn get_username(&self) -> String {
        self.username.clone()
    }

    fn get_password(&self) -> String {
        self.password.clone()
    }
}
