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

pub struct Iter<'a, Key, Value> {
    pub(crate) dataset: &'a NuSceneDataset,
    pub(crate) tokens_iter: std::slice::Iter<'a, Key>,
    pub(crate) _phantom: PhantomData<Value>,
}

impl<'a, Key, Value> Iter<'a, Key, Value> {
    fn refer(&self, referred: &'a Value) -> Iterated<'a, Value> {
        Iterated {
            dataset: self.dataset,
            inner: referred,
        }
    }
}

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

    fn refer_iter<Key, Value>(
        &self,
        referred_iter: std::slice::Iter<'a, Key>,
    ) -> Iter<'a, Key, Value> {
        Iter {
            dataset: self.dataset,
            tokens_iter: referred_iter,
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
