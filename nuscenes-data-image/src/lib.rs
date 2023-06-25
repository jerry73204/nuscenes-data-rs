pub use image;
use image::{DynamicImage, ImageResult};
use nuscenes_data::{
    dataset::{MapRef, SampleDataRef},
    serializable::FileFormat,
};

pub mod prelude {
    pub use super::{MapRefImageExt, SampleDataRefImageExt};
}

pub trait MapRefImageExt {
    fn load_dynamic_image(&self) -> ImageResult<DynamicImage>;
}

impl MapRefImageExt for MapRef {
    fn load_dynamic_image(&self) -> ImageResult<DynamicImage> {
        image::open(self.path())
    }
}

pub trait SampleDataRefImageExt {
    fn load_dynamic_image(&self) -> ImageResult<Option<DynamicImage>>;
}

impl SampleDataRefImageExt for SampleDataRef {
    fn load_dynamic_image(&self) -> ImageResult<Option<DynamicImage>> {
        if self.fileformat != FileFormat::Jpg {
            return Ok(None);
        }

        Ok(Some(image::open(self.path())?))
    }
}
