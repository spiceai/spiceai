use clap::Parser;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    query: String,
}

pub fn run(_args: Args) -> Result<(), Box<dyn std::error::Error>> {
  println!("Hello, World!");

  Ok(())
}
