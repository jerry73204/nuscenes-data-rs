use failure::Fail;
use image::ImageError;
use std::{io::Error as IoError, path::PathBuf};

pub type NuSceneDataResult<T> = Result<T, NuSceneDataError>;

#[derive(Debug, Fail)]
pub enum NuSceneDataError {
    #[fail(display = "internal error, please report bug")]
    InternalBug,
    #[fail(display = "corrupted file: {:?}", _0)]
    CorruptedFile(PathBuf),
    #[fail(display = "I/O error: {:?}", _0)]
    IoError(IoError),
    #[fail(display = "image error: {:?}", _0)]
    ImageError(ImageError),
}

impl From<IoError> for NuSceneDataError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<ImageError> for NuSceneDataError {
    fn from(error: ImageError) -> Self {
        match error {
            ImageError::IoError(io_err) => Self::IoError(io_err),
            _ => Self::ImageError(error),
        }
    }
}

impl NuSceneDataError {
    pub fn internal_bug() -> Self {
        Self::InternalBug
    }
}
