use std::{io, path::PathBuf};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("internal error, please report bug")]
    InternalBug,
    #[error("corrupted file: {0:?}")]
    CorruptedFile(PathBuf),
    #[error("corrupted file: {0}")]
    CorruptedDataset(String),
    #[error("I/O error: {0:?}")]
    IoError(io::Error),
    #[error("image error: {0:?}")]
    ImageError(image::ImageError),
    #[error("parseing error: {0}")]
    ParseError(String),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::IoError(error)
    }
}

impl From<image::ImageError> for Error {
    fn from(error: image::ImageError) -> Self {
        Self::ImageError(error)
    }
}
