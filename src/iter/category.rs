use crate::{
    iter::{Iter, Iterated},
    meta::{Category, LongToken},
};

impl<'a> Iterator for Iter<'a, LongToken, Category> {
    type Item = Iterated<'a, Category>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.category_map[&token]))
    }
}
