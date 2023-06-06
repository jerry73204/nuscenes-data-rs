use crate::error::NuScenesDataError;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    str::FromStr,
};

pub const TOKEN_LENGTH: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Token(pub [u8; TOKEN_LENGTH]);

impl Display for Token {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let Token(bytes) = self;
        let text = hex::encode(bytes);
        write!(formatter, "{}", text)
    }
}

impl FromStr for Token {
    type Err = NuScenesDataError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(text).map_err(|err| {
            NuScenesDataError::ParseError(format!("cannot decode token: {:?}", err))
        })?;
        if bytes.len() != TOKEN_LENGTH {
            let msg = format!(
                "invalid length: expected length {}, but found {}",
                TOKEN_LENGTH * 2,
                text.len()
            );
            return Err(NuScenesDataError::ParseError(msg));
        }
        let array = <[u8; TOKEN_LENGTH]>::try_from(bytes.as_slice()).unwrap();
        Ok(Token(array))
    }
}

impl Serialize for Token {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Token {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        let token: Self = text
            .parse()
            .map_err(|err| D::Error::custom(format!("{err}")))?;
        Ok(token)
    }
}
