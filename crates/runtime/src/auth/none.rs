use super::AuthProvider;

#[allow(clippy::module_name_repetitions)]
pub struct NoneAuth {}

impl AuthProvider for NoneAuth {
    #[must_use]
    fn new(_auth: &super::Auth) -> Self
    where
        Self: Sized,
    {
        NoneAuth {}
    }
}
