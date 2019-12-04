use failure::Fail;

pub type NuSceneDataResult<T> = Result<T, NuSceneDataError>;

#[derive(Debug, Fail)]
pub enum NuSceneDataError {
    #[fail(display = "please report bug")]
    InternalBug,
}

impl NuSceneDataError {
    pub fn internal_bug() -> Self {
        Self::InternalBug
    }
}
