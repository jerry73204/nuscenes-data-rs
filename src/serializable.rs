use crate::error::NuScenesDataError;
use chrono::naive::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::{
    convert::TryFrom,
    fmt::{Display, Formatter, Result as FormatResult},
    path::PathBuf,
};

pub const LONG_TOKEN_LENGTH: usize = 32;
pub const SHORT_TOKEN_LENGTH: usize = 16;

pub type CameraIntrinsic = Option<[[f64; 3]; 3]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LongToken([u8; LONG_TOKEN_LENGTH]);

impl Display for LongToken {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
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
pub struct ShortToken([u8; SHORT_TOKEN_LENGTH]);

impl Display for ShortToken {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Attribute {
    pub token: LongToken,
    pub description: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CalibratedSensor {
    pub token: LongToken,
    pub sensor_token: LongToken,
    pub rotation: [f64; 4],
    #[serde(with = "camera_intrinsic_serde")]
    pub camera_intrinsic: CameraIntrinsic,
    pub translation: [f64; 3],
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Category {
    pub token: LongToken,
    pub description: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EgoPose {
    pub token: LongToken,
    #[serde(with = "timestamp_serde")]
    pub timestamp: NaiveDateTime,
    pub rotation: [f64; 4],
    pub translation: [f64; 3],
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Instance {
    pub token: LongToken,
    pub nbr_annotations: usize,
    pub category_token: LongToken,
    pub first_annotation_token: LongToken,
    pub last_annotation_token: LongToken,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Log {
    pub token: LongToken,
    pub date_captured: NaiveDate,
    pub location: String,
    pub vehicle: String,
    #[serde(with = "logfile_serde")]
    pub logfile: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Map {
    pub token: ShortToken,
    pub log_tokens: Vec<LongToken>,
    pub filename: PathBuf,
    pub category: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Sample {
    pub token: LongToken,
    #[serde(with = "opt_long_token_serde")]
    pub next: Option<LongToken>,
    #[serde(with = "opt_long_token_serde")]
    pub prev: Option<LongToken>,
    pub scene_token: LongToken,
    #[serde(with = "timestamp_serde")]
    pub timestamp: NaiveDateTime,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SampleAnnotation {
    pub token: LongToken,
    pub num_lidar_pts: isize,
    pub num_radar_pts: isize,
    pub size: [f64; 3],
    pub rotation: [f64; 4],
    pub translation: [f64; 3],
    pub sample_token: LongToken,
    pub instance_token: LongToken,
    pub attribute_tokens: Vec<LongToken>,
    #[serde(with = "opt_string_serde")]
    pub visibility_token: Option<String>,
    #[serde(with = "opt_long_token_serde")]
    pub prev: Option<LongToken>,
    #[serde(with = "opt_long_token_serde")]
    pub next: Option<LongToken>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SampleData {
    pub token: LongToken,
    pub fileformat: FileFormat,
    pub is_key_frame: bool,
    pub filename: PathBuf,
    #[serde(with = "timestamp_serde")]
    pub timestamp: NaiveDateTime,
    pub sample_token: LongToken,
    pub ego_pose_token: LongToken,
    pub calibrated_sensor_token: LongToken,
    #[serde(with = "opt_long_token_serde")]
    pub prev: Option<LongToken>,
    #[serde(with = "opt_long_token_serde")]
    pub next: Option<LongToken>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Scene {
    pub token: LongToken,
    pub name: String,
    pub description: String,
    pub log_token: LongToken,
    pub nbr_samples: usize,
    pub first_sample_token: LongToken,
    pub last_sample_token: LongToken,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Sensor {
    pub token: LongToken,
    pub modality: Modality,
    pub channel: Channel,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Visibility {
    pub token: String,
    pub level: VisibilityLevel,
    pub description: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Modality {
    #[serde(rename = "camera")]
    Camera,
    #[serde(rename = "lidar")]
    Lidar,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileFormat {
    #[serde(rename = "bin")]
    Bin,
    #[serde(rename = "jpeg")]
    Jpeg,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VisibilityLevel {
    #[serde(rename = "v0-40")]
    V0_40,
    #[serde(rename = "v40-60")]
    V40_60,
    #[serde(rename = "v60-80")]
    V60_80,
    #[serde(rename = "v80-100")]
    V80_100,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    #[serde(rename = "CAM_BACK")]
    CamBack,
    #[serde(rename = "CAM_BACK_LEFT")]
    CamBackLeft,
    #[serde(rename = "CAM_BACK_RIGHT")]
    CamBackRight,
    #[serde(rename = "CAM_FRONT")]
    CamFront,
    #[serde(rename = "CAM_FRONT_LEFT")]
    CamFrontLeft,
    #[serde(rename = "CAM_FRONT_RIGHT")]
    CamFrontRight,
    #[serde(rename = "CAM_FRONT_ZOOMED")]
    CamFrontZoomed,
    #[serde(rename = "LIDAR_TOP")]
    LidarTop,
}

mod logfile_serde {
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

mod camera_intrinsic_serde {
    use super::CameraIntrinsic;
    use serde::{
        de::{Error as DeserializeError, SeqAccess, Visitor},
        ser::SerializeSeq,
        Deserializer, Serializer,
    };
    use std::fmt::{Formatter, Result as FormatResult};

    struct CameraIntrinsicVisitor;

    impl<'de> Visitor<'de> for CameraIntrinsicVisitor {
        type Value = CameraIntrinsic;

        fn expecting(&self, formatter: &mut Formatter) -> FormatResult {
            formatter.write_str("an empty array or a 3x3 two-dimensional array")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut matrix = [[0.0; 3]; 3];
            let mut length = 0;

            for ind in 0..3 {
                if let Some(row) = seq.next_element::<[f64; 3]>()? {
                    matrix[ind] = row;
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

    pub fn serialize<S>(value: &CameraIntrinsic, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(matrix) => {
                let mut seq = serializer.serialize_seq(Some(3))?;
                for ind in 0..3 {
                    seq.serialize_element(&matrix[ind])?;
                }
                seq.end()
            }
            None => {
                let seq = serializer.serialize_seq(Some(0))?;
                seq.end()
            }
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<CameraIntrinsic, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = deserializer.deserialize_any(CameraIntrinsicVisitor)?;
        Ok(value)
    }
}

mod long_token_serde {
    use super::LongToken;
    use serde::{
        de::{Error as DeserializeError, Unexpected, Visitor},
        Deserialize, Deserializer, Serialize, Serializer,
    };
    use std::{
        convert::TryFrom,
        fmt::{Formatter, Result as FormatResult},
    };

    struct LongTokenVisitor;

    impl<'de> Visitor<'de> for LongTokenVisitor {
        type Value = LongToken;

        fn expecting(&self, formatter: &mut Formatter) -> FormatResult {
            formatter.write_str("a hex string with 64 characters")
        }

        fn visit_str<E>(self, text: &str) -> Result<Self::Value, E>
        where
            E: DeserializeError,
        {
            LongToken::try_from(text).map_err(|_err| E::invalid_value(Unexpected::Str(text), &self))
        }
    }

    impl Serialize for LongToken {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let LongToken(bytes) = self;
            let text = hex::encode(bytes);
            serializer.serialize_str(&text)
        }
    }

    impl<'de> Deserialize<'de> for LongToken {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let token = deserializer.deserialize_string(LongTokenVisitor)?;
            Ok(token)
        }
    }
}

mod short_token_serde {
    use super::ShortToken;
    use serde::{
        de::{Error as DeserializeError, Unexpected, Visitor},
        Deserialize, Deserializer, Serialize, Serializer,
    };
    use std::{
        convert::TryFrom,
        fmt::{Formatter, Result as FormatResult},
    };

    struct ShortTokenVisitor;

    impl<'de> Visitor<'de> for ShortTokenVisitor {
        type Value = ShortToken;

        fn expecting(&self, formatter: &mut Formatter) -> FormatResult {
            formatter.write_str("a hex string with 64 characters")
        }

        fn visit_str<E>(self, text: &str) -> Result<Self::Value, E>
        where
            E: DeserializeError,
        {
            ShortToken::try_from(text)
                .map_err(|_err| E::invalid_value(Unexpected::Str(text), &self))
        }
    }

    impl Serialize for ShortToken {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let ShortToken(bytes) = self;
            let text = hex::encode(bytes);
            serializer.serialize_str(&text)
        }
    }

    impl<'de> Deserialize<'de> for ShortToken {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let token = deserializer.deserialize_string(ShortTokenVisitor)?;
            Ok(token)
        }
    }
}

mod opt_long_token_serde {
    use super::LongToken;
    use serde::{
        de::{Error as DeserializeError, Unexpected},
        Deserialize, Deserializer, Serialize, Serializer,
    };
    use std::convert::TryFrom;

    pub fn serialize<S>(value: &Option<LongToken>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(token) => token.serialize(serializer),
            None => serializer.serialize_str(""),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<LongToken>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // let value = deserializer.deserialize_any(OptionLongTokenVisitor)?;
        let text = String::deserialize(deserializer)?;

        let value = match text.len() {
            0 => None,
            _ => {
                let token = LongToken::try_from(text.as_str()).map_err(|_err| {
                    D::Error::invalid_value(
                        Unexpected::Str(&text),
                        &"an empty string or a hex string with 64 characters",
                    )
                })?;
                Some(token)
            }
        };

        Ok(value)
    }
}

mod opt_string_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(string) => string.serialize(serializer),
            None => serializer.serialize_str(""),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;

        let value = match string.len() {
            0 => None,
            _ => Some(string),
        };

        Ok(value)
    }
}

mod timestamp_serde {
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
        let datetime = NaiveDateTime::from_timestamp(secs as i64, nsecs as u32);
        Ok(datetime)
    }
}
