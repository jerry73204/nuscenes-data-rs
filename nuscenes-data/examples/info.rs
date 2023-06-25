use clap::Parser;
use nuscenes_data::{error::Result, DatasetLoader};
use std::path::PathBuf;

#[derive(Parser)]
struct Opts {
    pub version: String,
    pub data_dir: PathBuf,
    #[clap(long)]
    pub no_check: bool,
}

fn main() -> Result<()> {
    let opts = Opts::parse();

    // Change the path to your dataset directory
    let dataset = DatasetLoader {
        check: !opts.no_check,
        ..Default::default()
    }
    .load(&opts.version, &opts.data_dir)?;

    // Iterate over scenes chronologically
    for scene in dataset.scene_iter() {
        println!("read scene {}", scene.token);

        // Get associated log
        let log = scene.log();
        println!("captured at {}", log.date_captured);

        // Iterate over associated samples
        for sample in scene.sample_iter() {
            println!(
                "found sample {} in scene {} with timestamp {}",
                sample.token, scene.token, sample.timestamp
            );

            // Get the related scene back from sample
            assert_eq!(scene.token, sample.scene_token);

            // Iterate over associated annotations
            for annotation in sample.annotation_iter() {
                println!(
                    "found annotation {} in sample {}",
                    annotation.token, sample.token,
                );
            }

            // Iterate over associated data
            for sample_data in sample.sample_data_iter() {
                println!(
                    "found data {} in sample {}",
                    sample_data.token, sample.token
                );
            }
        }
    }

    Ok(())
}
