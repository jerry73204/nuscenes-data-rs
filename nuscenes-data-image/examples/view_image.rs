use anyhow::{anyhow, Result};
use clap::Parser;
use nuscenes_data::{serializable::FileFormat, DatasetLoader};
use nuscenes_data_image::SampleDataRefImageExt;
use show_image::{
    event::{VirtualKeyCode, WindowEvent},
    AsImageView,
};
use std::path::PathBuf;

#[derive(Parser)]
struct Opts {
    pub version: String,
    pub dataset_dir: PathBuf,
    #[clap(long)]
    pub no_check: bool,
}

#[show_image::main]
fn main() -> Result<()> {
    let Opts {
        version,
        dataset_dir,
        no_check,
    } = Opts::parse();

    // Load dataset
    eprintln!("Loading dataset...");
    let dataset = DatasetLoader {
        check: !no_check,
        ..Default::default()
    }
    .load(&version, dataset_dir)?;
    let records: Vec<_> = dataset
        .sample_data_iter()
        .filter(|data| data.fileformat == FileFormat::Jpg)
        .collect();
    eprintln!("Done loading dataset.");

    // Load the first image
    let mut index: usize = 0;
    let mut image = {
        let first = records
            .first()
            .ok_or_else(|| anyhow!("no image data found"))?;

        match first.load_dynamic_image() {
            Ok(image) => image,
            Err(err) => {
                eprintln!("unable to load {}: {err}", first.path().display());
                None
            }
        }
    };

    let window = show_image::create_window("nuscenes image viewer", Default::default())?;
    if let Some(image) = &image {
        window.set_image(&format!("{index:04}"), image.as_image_view()?)?;
    }

    for event in window.event_channel()? {
        let mut reload = false;

        // Check key events
        if let WindowEvent::KeyboardInput(event) = event {
            let Some(key_code) = event.input.key_code else {
                continue;
            };

            use VirtualKeyCode as V;

            match (key_code, event.input.state.is_pressed()) {
                (V::Left, true) => {
                    index = index.checked_sub(1).unwrap_or(records.len() - 1);
                    reload = true;
                }
                (V::Right, true) => {
                    index = (index + 1) % records.len();
                    reload = true;
                }
                (V::Escape, _) => break,
                _ => {}
            }
        }

        if reload {
            image = {
                let record = &records[index];
                match record.load_dynamic_image() {
                    Ok(image) => image,
                    Err(err) => {
                        eprintln!("unable to load {}: {err}", record.path().display());
                        None
                    }
                }
            };

            if let Some(image) = &image {
                window.set_image(&format!("{index:04}"), image.as_image_view()?)?;
            }
        }
    }

    Ok(())
}
