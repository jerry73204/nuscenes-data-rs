use crate::{base::WithDataset, iter::Iter, serializable::Log, token::LongToken};
use std::{fs::File, io::Result as IoResult};

impl<'a> WithDataset<'a, Log> {
    pub fn open(&self) -> IoResult<Option<File>> {
        self.inner
            .logfile
            .as_ref()
            .map(|path| File::open(self.dataset.dataset_dir.join(path)))
            .transpose()
    }
}

impl<'a, It> Iterator for Iter<'a, Log, It>
where
    It: Iterator<Item = &'a LongToken>,
{
    type Item = WithDataset<'a, Log>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokens_iter
            .next()
            .map(|token| self.refer(&self.dataset.log_map[&token]))
    }
}
