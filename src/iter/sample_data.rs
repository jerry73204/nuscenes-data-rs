use crate::{
    error::NuSceneDataResult,
    internal::SampleInternal,
    iter::{Iter, Iterated},
    meta::{CalibratedSensor, EgoPose, FileFormat, LongToken, SampleData},
};
use image::DynamicImage;
use std::{fs::File, io::Result as IoResult};

pub enum LoadedSampleData {
    Bin(),
    Jpeg(DynamicImage),
}

impl<'a> Iterated<'a, SampleData> {
    pub fn open(&self) -> IoResult<File> {
        File::open(self.dataset.dataset_dir.join(&self.inner.filename))
    }

    pub fn load(&self) -> NuSceneDataResult<LoadedSampleData> {
        let path = self.dataset.dataset_dir.join(&self.inner.filename);

        let data = match self.inner.fileformat {
            FileFormat::Bin => {
                // TODO
                unimplemented!();
            }
            FileFormat::Jpeg => {
                let image = image::open(path)?;
                LoadedSampleData::Jpeg(image)
            }
        };

        Ok(data)
    }

    pub fn sample(&self) -> Iterated<'a, SampleInternal> {
        self.refer(&self.dataset.sample_map[&self.inner.sample_token])
    }

    pub fn ego_pose(&self) -> Iterated<'a, EgoPose> {
        self.refer(&self.dataset.ego_pose_map[&self.inner.ego_pose_token])
    }

    pub fn calibrated_sensor(&self) -> Iterated<'a, CalibratedSensor> {
        self.refer(&self.dataset.calibrated_sensor_map[&self.inner.calibrated_sensor_token])
    }
}

impl<'a> Iterator for Iter<'a, LongToken, SampleData> {
    type Item = Iterated<'a, SampleData>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.sample_data_map[&token]))
    }
}
