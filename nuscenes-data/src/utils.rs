use crate::serializable::Token;
use rayon::prelude::{FromParallelIterator, ParallelIterator};

pub(crate) trait WithToken {
    fn token(&self) -> Token;
}

pub trait ParallelIteratorExt {
    fn par_try_collect<C, T, E>(self) -> Result<C, E>
    where
        Self: ParallelIterator<Item = Result<T, E>>,
        C: FromParallelIterator<T>,
        T: Send,
        E: Send;
}

impl<I> ParallelIteratorExt for I {
    fn par_try_collect<C, T, E>(self) -> Result<C, E>
    where
        Self: ParallelIterator<Item = Result<T, E>>,
        C: FromParallelIterator<T>,
        T: Send,
        E: Send,
    {
        let collection: Result<C, E> = self.collect();
        collection
    }
}
