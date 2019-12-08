use std::ops::Deref;

mod attribute;
mod calibrated_sensor;
mod category;
mod ego_pose;
mod instance;
mod log;
mod map;
mod sample;
mod sample_annotation;
mod sample_data;
mod scene;
mod sensor;
mod visibility;

use crate::{meta::LongToken, NuSceneDataset};
pub use attribute::*;
pub use calibrated_sensor::*;
pub use category::*;
pub use ego_pose::*;
pub use instance::*;
pub use log::*;
pub use map::*;
pub use sample::*;
pub use sample_annotation::*;
pub use sample_data::*;
pub use scene::*;
pub use sensor::*;
pub use visibility::*;

use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct Iter<'a, Value, It> {
    pub(crate) dataset: &'a NuSceneDataset,
    pub(crate) tokens_iter: It,
    pub(crate) _phantom: PhantomData<Value>,
}

impl<'a, Value, It> Iter<'a, Value, It>
where
    It: Iterator,
{
    fn refer(&self, referred: &'a Value) -> Iterated<'a, Value> {
        Iterated {
            dataset: self.dataset,
            inner: referred,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Iterated<'a, T> {
    dataset: &'a NuSceneDataset,
    inner: &'a T,
}

impl<'a, T> Iterated<'a, T> {
    fn refer<S>(&self, referred: &'a S) -> Iterated<'a, S> {
        Iterated {
            dataset: self.dataset,
            inner: referred,
        }
    }

    fn refer_iter<Value, It>(&self, tokens_iter: It) -> Iter<'a, Value, It> {
        Iter {
            dataset: self.dataset,
            tokens_iter,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T> Deref for Iterated<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}
