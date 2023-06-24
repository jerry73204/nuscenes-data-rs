use super::serde_utils;
use crate::serializable::{Token, VisibilityToken};
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
    #[serde(with = "serde_utils::camera_intrinsic")]
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
    #[serde(with = "serde_utils::timestamp")]
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
    #[serde(with = "serde_utils::logfile")]
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
    #[serde(with = "serde_utils::opt_token")]
    pub next: Option<Token>,
    #[serde(with = "serde_utils::opt_token")]
    pub prev: Option<Token>,
    pub scene_token: Token,
    #[serde(with = "serde_utils::timestamp")]
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
    // #[serde(with = "serde_utils::opt_string")]
    pub visibility_token: Option<VisibilityToken>,
    #[serde(with = "serde_utils::opt_token")]
    pub prev: Option<Token>,
    #[serde(with = "serde_utils::opt_token")]
    pub next: Option<Token>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleData {
    pub token: Token,
    pub fileformat: FileFormat,
    pub is_key_frame: bool,
    pub filename: PathBuf,
    #[serde(with = "serde_utils::timestamp")]
    pub timestamp: NaiveDateTime,
    pub sample_token: Token,
    pub ego_pose_token: Token,
    pub calibrated_sensor_token: Token,
    #[serde(with = "serde_utils::opt_token")]
    pub prev: Option<Token>,
    #[serde(with = "serde_utils::opt_token")]
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
    pub token: VisibilityToken,
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

pub(crate) trait WithToken {
    fn token(&self) -> Token;
}

macro_rules! impl_with_token {
    ($name:path) => {
        impl WithToken for $name {
            fn token(&self) -> Token {
                self.token
            }
        }
    };
}

impl_with_token!(Attribute);
impl_with_token!(CalibratedSensor);
impl_with_token!(Category);
impl_with_token!(EgoPose);
impl_with_token!(Instance);
impl_with_token!(Log);
impl_with_token!(Map);
impl_with_token!(Sample);
impl_with_token!(SampleAnnotation);
impl_with_token!(SampleData);
impl_with_token!(Scene);
impl_with_token!(Sensor);
