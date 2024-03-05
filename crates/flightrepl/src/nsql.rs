use snafu::prelude::*;
use std::collections::HashMap;

use arrow::datatypes::SchemaRef;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to run the NSQL model"))]
    FailedToRunModel { },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn load_model() -> Result<Option<String>, Box<dyn std::error::Error>> {
    Ok(Some("model".to_string()))
}

pub async fn run_nsql(
    user_query: String,
    model: String,
    schemas: &HashMap<String, SchemaRef>,
) -> Result<String, Box<dyn std::error::Error>> {
    let create_tables: String = schemas.iter().map(|(t, s)| {
        format!("CREATE TABLE {} ({})", t, s.fields.iter().map(|f| {
            println!("Field: {} {}", f.name(), f.data_type().to_string());
                format!("{} {}", f.name(), f.data_type().to_string())
            }).collect::<Vec<_>>()
            .join(", ")
        )

    }).collect::<Vec<_>>()
    .join(" ");
    Ok(format!("{} -- Using valid SQL, answer the following questions for the tables provided above. -- {}", create_tables, user_query))
}
