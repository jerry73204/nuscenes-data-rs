use crate::{
    iter::{Iter, Iterated},
    meta::{Attribute, LongToken},
};

impl<'a, It> Iterator for Iter<'a, Attribute, It>
where
    It: Iterator<Item = &'a LongToken>,
{
    type Item = Iterated<'a, Attribute>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.attribute_map[&token]))
    }
}
