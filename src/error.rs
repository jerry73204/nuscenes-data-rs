use failure::Fail;
use image::ImageError;
use std::{io::Error as IoError, path::PathBuf};

pub type NuScenesDataResult<T> = Result<T, NuScenesDataError>;

#[derive(Debug, Fail)]
pub enum NuScenesDataError {
    #[fail(display = "internal error, please report bug")]
    InternalBug,
    #[fail(display = "corrupted file: {:?}", _0)]
    CorruptedFile(PathBuf),
    #[fail(display = "corrupted file: {}", _0)]
    CorruptedDataset(String),
    #[fail(display = "I/O error: {:?}", _0)]
    IoError(IoError),
    #[fail(display = "image error: {:?}", _0)]
    ImageError(ImageError),
    #[fail(display = "parseing error: {}", _0)]
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
