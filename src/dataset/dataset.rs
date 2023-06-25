use crate::{
    parsed::{InstanceInternal, SampleInternal, SceneInternal},
    serializable::{
        Attribute, CalibratedSensor, Category, EgoPose, Log, Map, SampleAnnotation, SampleData,
        Sensor, Token, Visibility, VisibilityToken,
    },
};
use std::{collections::HashMap, path::PathBuf};

#[derive(Debug, Clone)]
pub struct DatasetInner {
    pub version: String,
    pub dataset_dir: PathBuf,
    pub attribute_map: HashMap<Token, Attribute>,
    pub calibrated_sensor_map: HashMap<Token, CalibratedSensor>,
    pub category_map: HashMap<Token, Category>,
    pub ego_pose_map: HashMap<Token, EgoPose>,
    pub instance_map: HashMap<Token, InstanceInternal>,
    pub log_map: HashMap<Token, Log>,
    pub map_map: HashMap<Token, Map>,
    pub scene_map: HashMap<Token, SceneInternal>,
    pub sample_map: HashMap<Token, SampleInternal>,
    pub sample_annotation_map: HashMap<Token, SampleAnnotation>,
    pub sample_data_map: HashMap<Token, SampleData>,
    pub sensor_map: HashMap<Token, Sensor>,
    pub visibility_map: HashMap<VisibilityToken, Visibility>,
    pub sorted_ego_pose_tokens: Vec<Token>,
    pub sorted_sample_tokens: Vec<Token>,
    pub sorted_sample_data_tokens: Vec<Token>,
    pub sorted_scene_tokens: Vec<Token>,
}
