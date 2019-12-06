use crate::{
    internal::InstanceInternal,
    iter::{Iter, Iterated},
    meta::{Category, LongToken, SampleAnnotation},
};

impl<'a> Iterated<'a, InstanceInternal> {
    pub fn category(&self) -> Iterated<'a, Category> {
        self.refer(&self.dataset.category_map[&self.inner.category_token])
    }

    pub fn sample_annotation_iter(&self) -> Iter<'a, LongToken, SampleAnnotation> {
        self.refer_iter(self.inner.annotation_tokens.iter())
    }
}

impl<'a> Iterator for Iter<'a, LongToken, InstanceInternal> {
    type Item = Iterated<'a, InstanceInternal>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.instance_map[&token]))
    }
}
