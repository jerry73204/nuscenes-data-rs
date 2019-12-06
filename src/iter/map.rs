use crate::{
    iter::{Iter, Iterated},
    meta::{Log, LongToken, Map, ShortToken},
};

impl<'a> Iterated<'a, Map> {
    pub fn log_iter(&self) -> Iter<'a, LongToken, Log> {
        self.refer_iter(self.inner.log_tokens.iter())
    }
}

impl<'a> Iterator for Iter<'a, ShortToken, Map> {
    type Item = Iterated<'a, Map>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.map_map[&token]))
    }
}
