use crate::{
    internal::InstanceInternal,
    iter::{Iter, Iterated},
    meta::{Category, LongToken, SampleAnnotation},
};
use std::slice::Iter as SliceIter;

impl<'a> Iterated<'a, InstanceInternal> {
    pub fn category(&self) -> Iterated<'a, Category> {
        self.refer(&self.dataset.category_map[&self.inner.category_token])
    }

    pub fn sample_annotation_iter(&self) -> Iter<'a, SampleAnnotation, SliceIter<'a, LongToken>> {
        self.refer_iter(self.inner.annotation_tokens.iter())
    }
}

impl<'a, It> Iterator for Iter<'a, InstanceInternal, It>
where
    It: Iterator<Item = &'a LongToken>,
{
    type Item = Iterated<'a, InstanceInternal>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.instance_map[&token]))
    }
}
