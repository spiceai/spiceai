use clap::Parser;
use flight_client::{Credentials, FlightClient};
use std::path::PathBuf;
use test_framework::{spiced::SpicedInstance, spicepod::load_spicepod};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the spicepod.yaml file
    #[arg(short('p'), long)]
    spicepod_path: PathBuf,

    /// Path to the spiced binary
    #[arg(short, long)]
    spiced_path: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut spiced_instance =
        SpicedInstance::start(args.spiced_path, load_spicepod(args.spicepod_path)?)?;

    std::thread::sleep(std::time::Duration::from_secs(10));

    let mut client = FlightClient::try_new(
        "http://localhost:50051".into(),
        Credentials::Anonymous,
        None,
    )
    .await?;

    let result = client.query("SELECT 1").await?;
    let batches = result.into_inner();
    println!("Batches: {:?}", batches);

    spiced_instance.stop()?;

    println!("Spiced instance stopped");
    Ok(())
}
