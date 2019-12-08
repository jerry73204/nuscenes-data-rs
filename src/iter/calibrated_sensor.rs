use crate::{
    iter::{Iter, Iterated},
    meta::{CalibratedSensor, LongToken, Sensor},
};

impl<'a> Iterated<'a, CalibratedSensor> {
    pub fn sensor(&self) -> Iterated<'a, Sensor> {
        self.refer(&self.dataset.sensor_map[&self.inner.sensor_token])
    }
}

impl<'a, It> Iterator for Iter<'a, CalibratedSensor, It>
where
    It: Iterator<Item = &'a LongToken>,
{
    type Item = Iterated<'a, CalibratedSensor>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.calibrated_sensor_map[&token]))
    }
}
