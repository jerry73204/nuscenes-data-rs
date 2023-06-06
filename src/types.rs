use crate::token::Token;
use chrono::naive::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribute {
    pub token: Token,
    pub description: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibratedSensor {
    pub token: Token,
    pub sensor_token: Token,
    pub rotation: [f64; 4],
    #[serde(with = "camera_intrinsic_serde")]
    pub camera_intrinsic: Option<[[f64; 3]; 3]>,
    pub translation: [f64; 3],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Category {
    pub token: Token,
    pub description: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EgoPose {
    pub token: Token,
    #[serde(with = "timestamp_serde")]
    pub timestamp: NaiveDateTime,
    pub rotation: [f64; 4],
    pub translation: [f64; 3],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub token: Token,
    pub nbr_annotations: usize,
    pub category_token: Token,
    pub first_annotation_token: Token,
    pub last_annotation_token: Token,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    pub token: Token,
    pub date_captured: NaiveDate,
    pub location: String,
    pub vehicle: String,
    #[serde(with = "logfile_serde")]
    pub logfile: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Map {
    pub token: Token,
    pub log_tokens: Vec<Token>,
    pub filename: PathBuf,
    pub category: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub token: Token,
    #[serde(with = "opt_short_token_serde")]
    pub next: Option<Token>,
    #[serde(with = "opt_short_token_serde")]
    pub prev: Option<Token>,
    pub scene_token: Token,
    #[serde(with = "timestamp_serde")]
    pub timestamp: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleAnnotation {
    pub token: Token,
    pub num_lidar_pts: isize,
    pub num_radar_pts: isize,
    pub size: [f64; 3],
    pub rotation: [f64; 4],
    pub translation: [f64; 3],
    pub sample_token: Token,
    pub instance_token: Token,
    pub attribute_tokens: Vec<Token>,
    #[serde(with = "opt_string_serde")]
    pub visibility_token: Option<String>,
    #[serde(with = "opt_short_token_serde")]
    pub prev: Option<Token>,
    #[serde(with = "opt_short_token_serde")]
    pub next: Option<Token>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleData {
    pub token: Token,
    pub fileformat: FileFormat,
    pub is_key_frame: bool,
    pub filename: PathBuf,
    #[serde(with = "timestamp_serde")]
    pub timestamp: NaiveDateTime,
    pub sample_token: Token,
    pub ego_pose_token: Token,
    pub calibrated_sensor_token: Token,
    #[serde(with = "opt_short_token_serde")]
    pub prev: Option<Token>,
    #[serde(with = "opt_short_token_serde")]
    pub next: Option<Token>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scene {
    pub token: Token,
    pub name: String,
    pub description: String,
    pub log_token: Token,
    pub nbr_samples: usize,
    pub first_sample_token: Token,
    pub last_sample_token: Token,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sensor {
    pub token: Token,
    pub modality: Modality,
    pub channel: Channel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Visibility {
    pub token: String,
    pub level: VisibilityLevel,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Modality {
    Camera,
    Lidar,
    Radar,
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Pcd,
    Jpg,
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum VisibilityLevel {
    V0_40,
    V40_60,
    V60_80,
    V80_100,
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Channel {
    CamBack,
    CamBackLeft,
    CamBackRight,
    CamFront,
    CamFrontLeft,
    CamFrontRight,
    CamFrontZoomed,
    LidarTop,
    RadarFront,
    RadarFrontLeft,
    RadarFrontRight,
    RadarBackLeft,
    RadarBackRight,
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

mod opt_short_token_serde {
    use crate::token::{Token, TOKEN_LENGTH};
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
        let datetime = NaiveDateTime::from_timestamp_opt(secs as i64, nsecs as u32).unwrap();
        Ok(datetime)
    }
}
