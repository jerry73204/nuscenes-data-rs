use crate::{
    iter::{Iter, Iterated},
    meta::{EgoPose, LongToken},
};

impl<'a, It> Iterator for Iter<'a, EgoPose, It>
where
    It: Iterator<Item = &'a LongToken>,
{
    type Item = Iterated<'a, EgoPose>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.ego_pose_map[&token]))
    }
}
