use clap::Parser;
use spiced::Args;

fn main() {
    let args = Args::parse();

    match spiced::run(args) {
        Ok(_) => {}
        Err(e) => println!("Error: {}", e),
    };
}
