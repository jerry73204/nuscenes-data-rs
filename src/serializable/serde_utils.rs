pub mod logfile {
    use serde::{
        de::{Error as DeserializeError, Visitor},
        Deserializer, Serialize, Serializer,
    };
    use std::{
        fmt::{Formatter, Result as FormatResult},
        path::PathBuf,
    };

    struct LogFileVisitor;

    impl<'de> Visitor<'de> for LogFileVisitor {
        type Value = Option<PathBuf>;

        fn expecting(&self, formatter: &mut Formatter) -> FormatResult {
            formatter.write_str("an empty string or a path to log file")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: DeserializeError,
        {
            let value = match value {
                "" => None,
                path_str => Some(PathBuf::from(path_str)),
            };

            Ok(value)
        }
    }

    pub fn serialize<S>(value: &Option<PathBuf>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(path) => path.serialize(serializer),
            None => serializer.serialize_str(""),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = deserializer.deserialize_any(LogFileVisitor)?;
        Ok(value)
    }
}

pub mod camera_intrinsic {
    use serde::{
        de::{Error as DeserializeError, SeqAccess, Visitor},
        ser::SerializeSeq,
        Deserializer, Serializer,
    };
    use std::fmt::{Formatter, Result as FormatResult};

    struct CameraIntrinsicVisitor;

    impl<'de> Visitor<'de> for CameraIntrinsicVisitor {
        type Value = Option<[[f64; 3]; 3]>;

        fn expecting(&self, formatter: &mut Formatter) -> FormatResult {
            formatter.write_str("an empty array or a 3x3 two-dimensional array")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut matrix = [[0.0; 3]; 3];
            let mut length = 0;

            for row_ref in &mut matrix {
                if let Some(row) = seq.next_element::<[f64; 3]>()? {
                    *row_ref = row;
                    length += 1;
                } else {
                    break;
                }
            }

            let value = match length {
                0 => None,
                3 => Some(matrix),
                _ => {
                    return Err(A::Error::invalid_length(length, &self));
                }
            };

            Ok(value)
        }
    }

    pub fn serialize<S>(value: &Option<[[f64; 3]; 3]>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(matrix) => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                for row in matrix {
                    seq.serialize_element(row)?;
                }
                seq.end()
            }
            None => {
                let seq = serializer.serialize_seq(Some(0))?;
                seq.end()
            }
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<[[f64; 3]; 3]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = deserializer.deserialize_any(CameraIntrinsicVisitor)?;
        Ok(value)
    }
}

pub mod opt_token {
    use crate::serializable::{Token, TOKEN_LENGTH};
    use serde::{
        de::{Error as DeserializeError, Unexpected},
        Deserialize, Deserializer, Serialize, Serializer,
    };
    use std::str::FromStr;

    pub fn serialize<S>(value: &Option<Token>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(token) => token.serialize(serializer),
            None => serializer.serialize_str(""),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Token>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;

        let value = if text.is_empty() {
            None
        } else {
            let token = Token::from_str(text.as_str()).map_err(|_err| {
                D::Error::invalid_value(
                    Unexpected::Str(&text),
                    &format!(
                        "an empty string or a hex string with {} characters",
                        TOKEN_LENGTH * 2
                    )
                    .as_str(),
                )
            })?;
            Some(token)
        };

        Ok(value)
    }
}

// mod opt_string_serde {
//     use serde::{Deserialize, Deserializer, Serialize, Serializer};

//     pub fn serialize<S>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         match value {
//             Some(string) => string.serialize(serializer),
//             None => serializer.serialize_str(""),
//         }
//     }

//     pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let string = String::deserialize(deserializer)?;

//         let value = match string.len() {
//             0 => None,
//             _ => Some(string),
//         };

//         Ok(value)
//     }
// }

pub mod timestamp {
    use chrono::NaiveDateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let timestamp = value.timestamp_nanos() as f64 / 1_000_000_000.0;
        serializer.serialize_f64(timestamp)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let timestamp_us = f64::deserialize(deserializer)?; // in us
        let timestamp_ns = (timestamp_us * 1000.0) as u64; // in ns
        let secs = timestamp_ns / 1_000_000_000;
        let nsecs = timestamp_ns % 1_000_000_000;
        let datetime = NaiveDateTime::from_timestamp_opt(secs as i64, nsecs as u32).unwrap();
        Ok(datetime)
    }
}
