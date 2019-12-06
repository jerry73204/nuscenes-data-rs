use crate::{
    internal::{InstanceInternal, SampleInternal},
    iter::{Iter, Iterated},
    meta::{Attribute, LongToken, SampleAnnotation},
};

impl<'a> Iterated<'a, SampleAnnotation> {
    pub fn sample(&self) -> Iterated<'a, SampleInternal> {
        self.refer(&self.dataset.sample_map[&self.inner.sample_token])
    }

    pub fn instance(&self) -> Iterated<'a, InstanceInternal> {
        self.refer(&self.dataset.instance_map[&self.inner.instance_token])
    }

    pub fn attribute_iter(&self) -> Iter<'a, LongToken, Attribute> {
        self.refer_iter(self.inner.attribute_tokens.iter())
    }
}

impl<'a> Iterator for Iter<'a, LongToken, SampleAnnotation> {
    type Item = Iterated<'a, SampleAnnotation>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.sample_annotation_map[&token]))
    }
}
