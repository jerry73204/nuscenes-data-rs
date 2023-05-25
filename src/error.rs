use image::ImageError;
use std::{io::Error as IoError, path::PathBuf};

pub type NuScenesDataResult<T> = Result<T, NuScenesDataError>;

#[derive(Debug, thiserror::Error)]
pub enum NuScenesDataError {
    #[error("internal error, please report bug")]
    InternalBug,
    #[error("corrupted file: {0:?}")]
    CorruptedFile(PathBuf),
    #[error("corrupted file: {0}")]
    CorruptedDataset(String),
    #[error("I/O error: {0:?}")]
    IoError(IoError),
    #[error("image error: {0:?}")]
    ImageError(ImageError),
    #[error("parseing error: {0}")]
    ParseError(String),
}

impl From<IoError> for NuScenesDataError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<ImageError> for NuScenesDataError {
    fn from(error: ImageError) -> Self {
        match error {
            ImageError::IoError(io_err) => Self::IoError(io_err),
            _ => Self::ImageError(error),
        }
    }
}
