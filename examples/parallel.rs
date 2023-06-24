use clap::Parser;
use nuscenes_data::{error::Result, Dataset};
use std::path::PathBuf;

#[derive(Parser)]
struct Opts {
    pub version: String,
    pub data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();

    // Change the path to your dataset directory
    let dataset = Dataset::load_async(&opts.version, &opts.data_dir).await?;
    Ok(())
}
