use clap::Parser;
use std::{path::PathBuf, time::Duration};
use test_framework::{flight::query_to_batches, spiced::SpicedInstance, spicepod::load_spicepod};

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

    spiced_instance
        .wait_for_ready(Duration::from_secs(10))
        .await?;

    std::thread::sleep(std::time::Duration::from_secs(10));

    let client = spiced_instance.flight_client().await?;

    let batches = query_to_batches(&client, "SELECT 1").await?;
    println!("Batches: {:?}", batches);

    spiced_instance.stop()?;

    println!("Spiced instance stopped");
    Ok(())
}
