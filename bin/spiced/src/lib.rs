#![allow(clippy::missing_errors_doc)]

use clap::Parser;
use std::error::Error;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    query: String,
}

pub fn run() -> Result<(), Box<dyn Error>> {
    match spicepod::load(".") {
        Ok(spicepod_definition) => {
            println!("spicepod_definition: {spicepod_definition:?}");
        }
        Err(err) => {
            eprintln!("err: {err:?}");
        }
    }

    Ok(())
}
