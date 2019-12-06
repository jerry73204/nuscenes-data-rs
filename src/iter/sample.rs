use crate::{
    internal::{SampleInternal, SceneInternal},
    iter::{Iter, Iterated},
    meta::{LongToken, SampleAnnotation, SampleData},
};

impl<'a> Iterated<'a, SampleInternal> {
    pub fn sample_annotation_iter(&self) -> Iter<'a, LongToken, SampleAnnotation> {
        self.refer_iter(self.inner.annotation_tokens.iter())
    }

    pub fn sample_data_iter(&self) -> Iter<'a, LongToken, SampleData> {
        self.refer_iter(self.inner.sample_data_tokens.iter())
    }

    pub fn scene(&self) -> Iterated<'a, SceneInternal> {
        self.refer(&self.dataset.scene_map[&self.inner.scene_token])
    }
}

impl<'a> Iterator for Iter<'a, LongToken, SampleInternal> {
    type Item = Iterated<'a, SampleInternal>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.sample_map[&token]))
    }
}
