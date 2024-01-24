pub trait Auth {
    fn get_token(&self) -> String;
}

pub struct AuthProviders {}

pub mod none;
pub mod spiceai;

impl AuthProviders {
    pub fn get_auth(name: &str) -> Box<dyn Auth> {
        match name {
            "spiceai" => {
                todo!("SpiceAI auth not implemented yet")
            }
            _ => Box::new(none::NoneAuth::new()),
        }
    }
}
