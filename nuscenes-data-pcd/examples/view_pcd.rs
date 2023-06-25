use anyhow::{anyhow, bail, Result};
use clap::Parser;
use kiss3d::{
    event::{Action, Key, Modifiers, WindowEvent},
    light::Light,
    nalgebra as na,
    window::{State, Window},
};
use nuscenes_data::{dataset::SampleDataRef, serializable::FileFormat, DatasetLoader};
use nuscenes_data_pcd::{prelude::*, PointCloud};
use std::path::PathBuf;

#[derive(Parser)]
struct Opts {
    pub version: String,
    pub dataset_dir: PathBuf,
    #[clap(long)]
    pub no_check: bool,
}

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
        .filter(|data| data.fileformat == FileFormat::Pcd)
        .collect();
    eprintln!("Done loading dataset.");

    // Initialize GUI state
    let gui = {
        let record = records
            .first()
            .ok_or_else(|| anyhow!("no point cloud samples found"))?;
        let points = match load_pcd(record) {
            Ok(points) => points,
            Err(err) => {
                eprintln!(
                    "Unable to load the file {} for sample data {}: {err}",
                    record.path().display(),
                    record.token
                );
                vec![]
            }
        };

        Gui {
            records,
            points,
            index: 0,
        }
    };

    // Run GUI
    let mut window = Window::new("nuscenes dataset point cloud viewer");
    window.set_light(Light::StickToCamera);
    window.render_loop(gui);

    Ok(())
}

struct Gui {
    records: Vec<SampleDataRef>,
    points: Vec<na::Point3<f32>>,
    index: usize,
}

impl State for Gui {
    fn step(&mut self, window: &mut Window) {
        // Process key events
        let mut go_next = false;
        let mut go_prev = false;

        for event in window.events().iter() {
            let WindowEvent::Key(key, action, modifiers) = event.value else {
                continue;
            };

            let has_ctrl = modifiers.contains(Modifiers::Control);
            let has_alt = modifiers.contains(Modifiers::Alt);
            let has_shift = modifiers.contains(Modifiers::Shift);
            let has_super = modifiers.contains(Modifiers::Super);

            use Action as A;
            use Key as K;

            match (key, action, has_ctrl, has_alt, has_shift, has_super) {
                (K::Left, A::Press, false, false, false, false) => {
                    go_prev = true;
                }
                (K::Right, A::Press, false, false, false, false) => {
                    go_next = true;
                }
                _ => {}
            }
        }

        // change record index
        let reload = match (go_prev, go_next) {
            (true, true) | (false, false) => false,
            (true, false) => {
                self.index = self.index.checked_sub(1).unwrap_or(self.records.len() - 1);
                true
            }
            (false, true) => {
                self.index = (self.index + 1) % self.records.len();
                true
            }
        };

        // Reload points if requested
        if reload {
            let record = &self.records[self.index];
            self.points = match load_pcd(record) {
                Ok(points) => points,
                Err(err) => {
                    eprintln!(
                        "Unable to load the file {} for sample data {}: {err}",
                        record.path().display(),
                        record.token
                    );
                    vec![]
                }
            };
        }

        // Rendering
        let color = na::Point3::new(1.0, 1.0, 1.0);
        self.points.iter().for_each(|point| {
            window.draw_point(point, &color);
        });
    }
}

fn load_pcd(record: &SampleDataRef) -> Result<Vec<na::Point3<f32>>> {
    let points: Vec<_> = match record.load_pcd()? {
        PointCloud::Pcd(points) => points
            .into_iter()
            .map(|p| na::Point3::new(p.x, p.y, p.z))
            .collect(),
        PointCloud::Bin(points) => points
            .into_iter()
            .map(|p| na::Point3::new(p.x, p.y, p.z))
            .collect(),
        PointCloud::NotSupported => bail!("file format not supported"),
    };
    Ok(points)
}
