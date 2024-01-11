use clap::Parser;
use spicepod;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    query: String,
}

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    match spicepod::load(".") {
        Ok(spicepod_definition) => {
            println!("spicepod_definition: {:?}", spicepod_definition);
        },
        Err(err) => {
            eprintln!("err: {:?}", err);
        }
    }

    Ok(())
}
