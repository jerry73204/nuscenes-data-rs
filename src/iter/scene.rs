use crate::{
    internal::{SampleInternal, SceneInternal},
    iter::{Iter, Iterated, LongToken},
    meta::Log,
};

impl<'a> Iterated<'a, SceneInternal> {
    pub fn log(&self) -> Iterated<'a, Log> {
        self.refer(&self.dataset.log_map[&self.inner.log_token])
    }

    pub fn sample_iter(&self) -> Iter<'a, LongToken, SampleInternal> {
        self.refer_iter(self.inner.sample_tokens.iter())
    }
}

impl<'a> Iterator for Iter<'a, LongToken, SceneInternal> {
    type Item = Iterated<'a, SceneInternal>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.scene_map[&token]))
    }
}
