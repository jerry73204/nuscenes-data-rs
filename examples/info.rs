use clap::Parser;
use nuscenes_data::{error::Result, Dataset};
use std::path::PathBuf;

#[derive(Parser)]
struct Opts {
    pub version: String,
    pub data_dir: PathBuf,
}

fn main() -> Result<()> {
    let opts = Opts::parse();

    // Change the path to your dataset directory
    let dataset = Dataset::load(&opts.version, &opts.data_dir)?;

    // Iterate over scenes chronologically
    for scene in dataset.scene_map.values() {
        println!("read scene {}", scene.token);

        // Get associated log
        let log = &dataset.log_map[&scene.log_token];
        println!("captured at {}", log.date_captured);

        // Iterate over associated samples
        for &sample_tokens in &scene.sample_tokens {
            let sample = &dataset.sample_map[&sample_tokens];
            println!(
                "found sample {} in scene {} with timestamp {}",
                sample.token, scene.token, sample.timestamp
            );

            // Get the related scene back from sample
            assert_eq!(scene.token, sample.scene_token);

            // Iterate over associated annotations
            for &annotation_token in &sample.annotation_tokens {
                let annotation = &dataset.sample_annotation_map[&annotation_token];
                println!(
                    "found annotation {} in sample {}",
                    annotation.token, sample.token,
                );
            }

            // Iterate over associated data
            for &sample_data_token in &sample.sample_data_tokens {
                let data = &dataset.sample_data_map[&sample_data_token];
                println!("found data {} in sample {}", data.token, sample.token);

                // Load data
                // match data.load()? {
                //     LoadedSampleData::PointCloud(matrix) => {
                //         println!(
                //             "get point cloud from data {} with {} points",
                //             data.token,
                //             matrix.nrows()
                //         );
                //     }
                //     LoadedSampleData::Image(image) => {
                //         println!(
                //             "get image from data {} with shape {}x{}",
                //             data.token,
                //             image.width(),
                //             image.height()
                //         );
                //     }
                // }
            }
        }
    }

    Ok(())
}
