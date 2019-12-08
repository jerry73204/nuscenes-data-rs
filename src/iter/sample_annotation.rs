use crate::{
    internal::{InstanceInternal, SampleInternal},
    iter::{Iter, Iterated},
    meta::{Attribute, LongToken, SampleAnnotation},
};
use std::slice::Iter as SliceIter;

impl<'a> Iterated<'a, SampleAnnotation> {
    pub fn sample(&self) -> Iterated<'a, SampleInternal> {
        self.refer(&self.dataset.sample_map[&self.inner.sample_token])
    }

    pub fn instance(&self) -> Iterated<'a, InstanceInternal> {
        self.refer(&self.dataset.instance_map[&self.inner.instance_token])
    }

    pub fn attribute_iter(&self) -> Iter<'a, Attribute, SliceIter<'a, LongToken>> {
        self.refer_iter(self.inner.attribute_tokens.iter())
    }

    pub fn prev(&self) -> Option<Iterated<'a, SampleAnnotation>> {
        self.inner
            .prev
            .as_ref()
            .map(|token| self.refer(&self.dataset.sample_annotation_map[token]))
    }

    pub fn next(&self) -> Option<Iterated<'a, SampleAnnotation>> {
        self.inner
            .next
            .as_ref()
            .map(|token| self.refer(&self.dataset.sample_annotation_map[token]))
    }
}

impl<'a, It> Iterator for Iter<'a, SampleAnnotation, It>
where
    It: Iterator<Item = &'a LongToken>,
{
    type Item = Iterated<'a, SampleAnnotation>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.sample_annotation_map[&token]))
    }
}
