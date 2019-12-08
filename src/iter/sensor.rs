use crate::{
    iter::{Iter, Iterated},
    meta::{LongToken, Sensor},
};

impl<'a, It> Iterator for Iter<'a, Sensor, It>
where
    It: Iterator<Item = &'a LongToken>,
{
    type Item = Iterated<'a, Sensor>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.sensor_map[&token]))
    }
}
