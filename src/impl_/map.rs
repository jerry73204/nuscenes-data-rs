use crate::{
    base::WithDataset,
    iter::Iter,
    serializable::{Log, Map},
    token::{LongToken, ShortToken},
};
use std::slice::Iter as SliceIter;

impl<'a> WithDataset<'a, Map> {
    pub fn log_iter(&self) -> Iter<'a, Log, SliceIter<'a, LongToken>> {
        self.refer_iter(self.inner.log_tokens.iter())
    }
}

impl<'a, It> Iterator for Iter<'a, Map, It>
where
    It: Iterator<Item = ShortToken>,
{
    type Item = WithDataset<'a, Map>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.map_map[&token]))
    }
}
