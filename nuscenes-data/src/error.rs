use std::{io, path::PathBuf};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("corrupted file: {0:?}")]
    CorruptedFile(PathBuf),
    #[error("corrupted file: {0}")]
    CorruptedDataset(String),
    #[error("I/O error: {0:?}")]
    IoError(io::Error),
    #[error("parseing error: {0}")]
    ParseError(String),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::IoError(error)
    }
}
