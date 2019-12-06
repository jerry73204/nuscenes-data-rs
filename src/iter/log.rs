use crate::{
    iter::{Iter, Iterated},
    meta::{Log, LongToken},
};
use std::{fs::File, io::Result as IoResult};

impl<'a> Iterated<'a, Log> {
    pub fn open(&self) -> IoResult<Option<File>> {
        self.inner
            .logfile
            .as_ref()
            .map(|path| File::open(self.dataset.dataset_dir.join(path)))
            .transpose()
    }
}

impl<'a> Iterator for Iter<'a, LongToken, Log> {
    type Item = Iterated<'a, Log>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.log_map[&token]))
    }
}
