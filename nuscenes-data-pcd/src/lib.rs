use anyhow::Result;
use nuscenes_data::{dataset::SampleDataRef, serializable::FileFormat};
use pcd_rs::{PcdDeserialize, PcdSerialize};

pub mod prelude {
    pub use super::SampleDataRefPcdExt;
}

#[derive(Debug, Clone, PartialEq, PcdSerialize, PcdDeserialize)]
pub struct Point {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub dyn_prop: u8,
    pub id: u8,
    pub rcs: f32,
    pub vx: f32,
    pub vy: f32,
    pub vx_comp: f32,
    pub vy_comp: f32,
    pub is_quality_valid: u8,
    pub ambig_state: u8,
    pub x_rms: u8,
    pub y_rms: u8,
    pub invalid_state: u8,
    pub pdh0: u8,
    pub vx_rms: u8,
    pub vy_rms: u8,
}

pub trait SampleDataRefPcdExt {
    fn load_pcd(&self) -> Result<Option<Vec<Point>>>;
}

impl SampleDataRefPcdExt for SampleDataRef {
    fn load_pcd(&self) -> Result<Option<Vec<Point>>> {
        if self.fileformat != FileFormat::Pcd {
            return Ok(None);
        }

        let reader = pcd_rs::Reader::open(self.path())?;
        let points: Result<Vec<_>> = reader.collect();
        Ok(Some(points?))
    }
}
