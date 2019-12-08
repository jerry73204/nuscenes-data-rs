use crate::{
    base::{LoadedSampleData, PointCloudMatrix},
    error::{NuSceneDataError, NuSceneDataResult},
    internal::SampleInternal,
    iter::{Iter, Iterated},
    meta::{CalibratedSensor, EgoPose, FileFormat, LongToken, SampleData},
};
// use memmap::MmapOptions;
use nalgebra::{Dynamic, VecStorage, U5};
use safe_transmute::guard::SingleManyGuard;
use std::{
    fs::File,
    io::{prelude::*, BufReader, Result as IoResult},
};

impl<'a> Iterated<'a, SampleData> {
    pub fn open(&self) -> IoResult<File> {
        File::open(self.dataset.dataset_dir.join(&self.inner.filename))
    }

    pub fn load_raw(&self) -> NuSceneDataResult<Vec<u8>> {
        let path = self.dataset.dataset_dir.join(&self.inner.filename);
        let mut reader = BufReader::new(File::open(path)?);
        let mut buf = vec![];
        reader.read_to_end(&mut buf)?;
        Ok(buf)
    }

    pub fn load(&self) -> NuSceneDataResult<LoadedSampleData> {
        let path = self.dataset.dataset_dir.join(&self.inner.filename);

        let data = match self.inner.fileformat {
            FileFormat::Bin => {
                let bytes = self.load_raw()?;
                let values = safe_transmute::transmute_many::<f32, SingleManyGuard>(&bytes)
                    .map_err(|_| NuSceneDataError::CorruptedFile(path.clone()))?;
                if values.len() % 5 != 0 {
                    return Err(NuSceneDataError::CorruptedFile(path));
                }
                let n_rows = values.len() / 5;
                
                // TODO: this step takes one copy of the buffer. try to use more efficient impl.
                let storage = VecStorage::new(Dynamic::new(n_rows), U5, Vec::from(values));
                let matrix = PointCloudMatrix::from_data(storage);
                LoadedSampleData::PointCloud(matrix)
            }
            FileFormat::Jpeg => {
                let image = image::open(path)?;
                LoadedSampleData::Image(image)
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

    pub fn prev(&self) -> Option<Iterated<'a, SampleData>> {
        self.inner
            .prev
            .as_ref()
            .map(|token| self.refer(&self.dataset.sample_data_map[token]))
    }

    pub fn next(&self) -> Option<Iterated<'a, SampleData>> {
        self.inner
            .next
            .as_ref()
            .map(|token| self.refer(&self.dataset.sample_data_map[token]))
    }
}

impl<'a, It> Iterator for Iter<'a, SampleData, It>
where
    It: Iterator<Item = &'a LongToken>,
{
    type Item = Iterated<'a, SampleData>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.sample_data_map[&token]))
    }
}
