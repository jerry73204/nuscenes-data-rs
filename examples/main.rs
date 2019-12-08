// extern crate nuscenes_data;
use image::GenericImageView;
use nuscenes_data::{error::NuScenesDataResult, LoadedSampleData, NuScenesDataset};

fn main() -> NuScenesDataResult<()> {
    // Change the path to your dataset directory
    let dataset = NuScenesDataset::load("1.02", "/some/path/v1.02-train")?;

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
            assert_eq!(scene.token, sample.scene().token);

            // Iterate over associated annotations
            for annotation in sample.sample_annotation_iter() {
                println!(
                    "found annotation {} in sample {}",
                    annotation.token, sample.token,
                );
            }

            // Iterate over associated data
            for data in sample.sample_data_iter() {
                println!("found data {} in sample {}", data.token, sample.token);

                // Load data
                match data.load()? {
                    LoadedSampleData::PointCloud(matrix) => {
                        println!(
                            "get point cloud from data {} with {} points",
                            data.token,
                            matrix.nrows()
                        );
                    }
                    LoadedSampleData::Image(image) => {
                        println!(
                            "get image from data {} with shape {}x{}",
                            data.token,
                            image.width(),
                            image.height()
                        );
                    }
                }
            }
        }
    }

    Ok(())
}
