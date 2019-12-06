use crate::{
    iter::{Iter, Iterated},
    meta::{EgoPose, LongToken},
};

impl<'a> Iterator for Iter<'a, LongToken, EgoPose> {
    type Item = Iterated<'a, EgoPose>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.ego_pose_map[&token]))
    }
}
