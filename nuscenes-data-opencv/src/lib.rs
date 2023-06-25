use nuscenes_data::{
    dataset::{MapRef, SampleDataRef},
    serializable::FileFormat,
};
use opencv::{
    self as cv,
    imgcodecs::{imread, IMREAD_COLOR},
    prelude::*,
};

pub mod prelude {
    pub use super::{MapRefImageExt, SampleDataRefImageExt};
}

pub trait MapRefImageExt {
    fn load_opencv_mat(&self) -> cv::Result<Mat>;
}

impl MapRefImageExt for MapRef {
    fn load_opencv_mat(&self) -> cv::Result<Mat> {
        let path = format!("{}", self.path().display());
        imread(&path, IMREAD_COLOR)
    }
}

pub trait SampleDataRefImageExt {
    fn load_opencv_mat(&self) -> cv::Result<Option<Mat>>;
}

impl SampleDataRefImageExt for SampleDataRef {
    fn load_opencv_mat(&self) -> cv::Result<Option<Mat>> {
        if self.fileformat != FileFormat::Jpg {
            return Ok(None);
        }

        let path = format!("{}", self.path().display());
        let mat = imread(&path, IMREAD_COLOR)?;
        Ok(Some(mat))
    }
}
