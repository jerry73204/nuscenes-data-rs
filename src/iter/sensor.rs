use crate::{
    iter::{Iter, Iterated},
    meta::{LongToken, Sensor},
};

impl<'a> Iterator for Iter<'a, LongToken, Sensor> {
    type Item = Iterated<'a, Sensor>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.sensor_map[&token]))
    }
}
