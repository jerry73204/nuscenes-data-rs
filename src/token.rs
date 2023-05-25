use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
};

use crate::error::NuScenesDataError;

pub const LONG_TOKEN_LENGTH: usize = 32;
pub const SHORT_TOKEN_LENGTH: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LongToken(pub [u8; LONG_TOKEN_LENGTH]);

impl Display for LongToken {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let LongToken(bytes) = self;
        let text = hex::encode(bytes);
        write!(formatter, "{}", text)
    }
}

impl TryFrom<&str> for LongToken {
    type Error = NuScenesDataError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        let bytes = hex::decode(text).map_err(|err| {
            NuScenesDataError::ParseError(format!("cannot decode token: {:?}", err))
        })?;
        if bytes.len() != LONG_TOKEN_LENGTH {
            let msg = format!(
                "invalid length: expected length {}, but found {}",
                LONG_TOKEN_LENGTH * 2,
                text.len()
            );
            return Err(NuScenesDataError::ParseError(msg));
        }
        let array = <[u8; LONG_TOKEN_LENGTH]>::try_from(bytes.as_slice()).unwrap();
        Ok(LongToken(array))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShortToken(pub [u8; SHORT_TOKEN_LENGTH]);

impl Display for ShortToken {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let ShortToken(bytes) = self;
        let text = hex::encode(bytes);
        write!(formatter, "{}", text)
    }
}

impl TryFrom<&str> for ShortToken {
    type Error = NuScenesDataError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        let bytes = hex::decode(text).map_err(|err| {
            NuScenesDataError::ParseError(format!("cannot decode token: {:?}", err))
        })?;
        if bytes.len() != SHORT_TOKEN_LENGTH {
            let msg = format!(
                "invalid length: expected length {}, but found {}",
                SHORT_TOKEN_LENGTH * 2,
                text.len()
            );
            return Err(NuScenesDataError::ParseError(msg));
        }
        let array = <[u8; SHORT_TOKEN_LENGTH]>::try_from(bytes.as_slice()).unwrap();
        Ok(ShortToken(array))
    }
}
