use darling::{util::SpannedValue, FromAttributes};

#[derive(Debug, Default, FromAttributes)]
#[darling(attributes(mysql))]
pub struct Mysql {
    #[darling(default)]
    pub json: SpannedValue<bool>,
    #[darling(default)]
    pub rename: Option<String>,
    #[darling(default)]
    pub deserialize_with: Option<SpannedValue<syn::Path>>,
    #[darling(default)]
    pub serialize_with: Option<SpannedValue<syn::Path>>,
}

impl Mysql {
    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        if let Some(ref deserialize_with) = self.deserialize_with {
            if *self.json {
                return Err(crate::Error::FromRowConflictingAttributes(
                    self.json.span(),
                    deserialize_with.span(),
                ));
            }
        }

        if let Some(ref serialize_with) = self.serialize_with {
            if *self.json {
                return Err(crate::Error::FromRowConflictingAttributes(
                    self.json.span(),
                    serialize_with.span(),
                ));
            }
        }

        Ok(())
    }
}
