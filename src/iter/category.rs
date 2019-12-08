use crate::{
    iter::{Iter, Iterated},
    meta::{Category, LongToken},
};

impl<'a, It> Iterator for Iter<'a, Category, It>
where
    It: Iterator<Item = &'a LongToken>,
{
    type Item = Iterated<'a, Category>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.category_map[&token]))
    }
}
