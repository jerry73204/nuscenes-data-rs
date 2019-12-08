use crate::{
    base::WithDataset,
    iter::Iter,
    serializable::{CalibratedSensor, LongToken, Sensor},
};

impl<'a> WithDataset<'a, CalibratedSensor> {
    pub fn sensor(&self) -> WithDataset<'a, Sensor> {
        self.refer(&self.dataset.sensor_map[&self.inner.sensor_token])
    }
}

impl<'a, It> Iterator for Iter<'a, CalibratedSensor, It>
where
    It: Iterator<Item = &'a LongToken>,
{
    type Item = WithDataset<'a, CalibratedSensor>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.calibrated_sensor_map[&token]))
    }
}
