use anyhow::{ensure, Result};
use nuscenes_data::{dataset::SampleDataRef, serializable::FileFormat};
use pcd_rs::{PcdDeserialize, PcdSerialize};
use raw_parts::RawParts;
use std::{
    fs::File,
    io::{prelude::*, BufReader},
    mem,
};

pub mod prelude {
    pub use super::SampleDataRefPcdExt;
}

#[derive(Debug, Clone, PartialEq)]
pub enum PointCloud {
    Pcd(Vec<PcdPoint>),
    Bin(Vec<BinPoint>),
    NotSupported,
}

#[derive(Debug, Clone, PartialEq, PcdSerialize, PcdDeserialize)]
pub struct PcdPoint {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub dyn_prop: i8,
    pub id: i16,
    pub rcs: f32,
    pub vx: f32,
    pub vy: f32,
    pub vx_comp: f32,
    pub vy_comp: f32,
    pub is_quality_valid: i8,
    pub ambig_state: i8,
    pub x_rms: i8,
    pub y_rms: i8,
    pub invalid_state: i8,
    pub pdh0: i8,
    pub vx_rms: i8,
    pub vy_rms: i8,
}

#[derive(Debug, Clone, PartialEq)]
#[repr(packed)]
pub struct BinPoint {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub intensity: f32,
    pub ring_index: i32,
}

pub trait SampleDataRefPcdExt {
    fn load_pcd(&self) -> Result<PointCloud>;
}

impl SampleDataRefPcdExt for SampleDataRef {
    fn load_pcd(&self) -> Result<PointCloud> {
        if self.fileformat != FileFormat::Pcd {
            return Ok(PointCloud::NotSupported);
        }

        let Some(ext) = self.filename.extension() else {
            return Ok(PointCloud::NotSupported)
        };
        let path = self.path();

        let pcd = if ext == "pcd" {
            let reader = pcd_rs::Reader::open(path)?;
            let points: Result<Vec<_>> = reader.collect();
            PointCloud::Pcd(points?)
        } else if ext == "bin" {
            let point_len = mem::size_of::<BinPoint>();

            let buf = {
                let mut reader = BufReader::new(File::open(&path)?);
                let mut buf = vec![];
                let buf_len = reader.read_to_end(&mut buf)?;
                ensure!(buf_len % point_len == 0, "Unable to load this file {}. The file size is {buf_len}, which is not multiple of {point_len}", path.display());
                buf
            };

            // Transmute the byte vec to vec of points
            let points: Vec<BinPoint> = unsafe {
                // make sure the capacity is equal to the length of the buffer.
                let buf = buf.into_boxed_slice().into_vec();

                // transmute the vec
                let RawParts {
                    ptr,
                    length,
                    capacity,
                } = RawParts::from_vec(buf);
                debug_assert_eq!(length, capacity);

                RawParts {
                    ptr: ptr as *mut BinPoint,
                    length: length / point_len,
                    capacity: capacity / point_len,
                }
                .into_vec()
            };

            PointCloud::Bin(points)
        } else {
            PointCloud::NotSupported
        };

        Ok(pcd)
    }
}
