use super::Auth;

pub struct NoneAuth {}

impl Auth for NoneAuth {
    fn get_token(&self) -> String {
        "".to_string()
    }
}

impl NoneAuth {
    #[must_use]
    pub fn new() -> Self {
        NoneAuth {}
    }
}
