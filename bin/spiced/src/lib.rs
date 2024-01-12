#![allow(clippy::missing_errors_doc)]

use app::App;
use clap::Parser;
use std::error::Error;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    query: String,
}

pub fn run() -> Result<(), Box<dyn Error>> {
    match App::new(".") {
        Ok(app_definition) => {
            println!("app_definition: {app_definition:?}");
        }
        Err(err) => {
            eprintln!("err: {err:?}");
        }
    }

    Ok(())
}
