use runtime_auth::AuthPrincipal;

pub struct Anonymous;

impl AuthPrincipal for Anonymous {
    fn username(&self) -> &'static str {
        "anonymous"
    }

    fn groups(&self) -> &[&str] {
        &[]
    }
}
