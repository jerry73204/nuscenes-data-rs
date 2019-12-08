use crate::base::{NuScenesDataset, WithDataset};
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct Iter<'a, Value, It> {
    pub(crate) dataset: &'a NuScenesDataset,
    pub(crate) tokens_iter: It,
    pub(crate) _phantom: PhantomData<Value>,
}

impl<'a, Value, It> Iter<'a, Value, It>
where
    It: Iterator,
{
    pub(crate) fn refer(&self, referred: &'a Value) -> WithDataset<'a, Value> {
        WithDataset {
            dataset: self.dataset,
            inner: referred,
        }
    }
}
