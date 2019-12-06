use crate::{
    iter::{Iter, Iterated},
    meta::Visibility,
};

impl<'a> Iterator for Iter<'a, String, Visibility> {
    type Item = Iterated<'a, Visibility>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.visibility_map[token]))
    }
}
