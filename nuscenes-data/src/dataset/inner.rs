use crate::{
    error::{Error, Result},
    serializable::{
        Attribute, CalibratedSensor, Category, EgoPose, Instance, Log, Map, Sample,
        SampleAnnotation, SampleData, Scene, Sensor, Token, Visibility, VisibilityToken,
    },
};
use chrono::NaiveDateTime;
use std::{collections::HashMap, path::PathBuf};

#[derive(Debug, Clone)]
pub struct DatasetInner {
    pub version: String,
    pub dataset_dir: PathBuf,
    pub attribute_map: HashMap<Token, Attribute>,
    pub calibrated_sensor_map: HashMap<Token, CalibratedSensor>,
    pub category_map: HashMap<Token, Category>,
    pub ego_pose_map: HashMap<Token, EgoPose>,
    pub instance_map: HashMap<Token, InstanceInner>,
    pub log_map: HashMap<Token, Log>,
    pub map_map: HashMap<Token, Map>,
    pub scene_map: HashMap<Token, SceneInner>,
    pub sample_map: HashMap<Token, SampleInner>,
    pub sample_annotation_map: HashMap<Token, SampleAnnotation>,
    pub sample_data_map: HashMap<Token, SampleData>,
    pub sensor_map: HashMap<Token, Sensor>,
    pub visibility_map: HashMap<VisibilityToken, Visibility>,
    pub sorted_ego_pose_tokens: Vec<Token>,
    pub sorted_sample_tokens: Vec<Token>,
    pub sorted_sample_data_tokens: Vec<Token>,
    pub sorted_scene_tokens: Vec<Token>,
}

#[derive(Debug, Clone)]
pub struct SampleInner {
    pub token: Token,
    pub next: Option<Token>,
    pub prev: Option<Token>,
    pub timestamp: NaiveDateTime,
    pub scene_token: Token,
    pub annotation_tokens: Vec<Token>,
    pub sample_data_tokens: Vec<Token>,
}

impl SampleInner {
    pub fn from(
        sample: Sample,
        annotation_tokens: Vec<Token>,
        sample_data_tokens: Vec<Token>,
    ) -> Self {
        let Sample {
            token,
            next,
            prev,
            scene_token,
            timestamp,
        } = sample;

        Self {
            token,
            next,
            prev,
            scene_token,
            timestamp,
            annotation_tokens,
            sample_data_tokens,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstanceInner {
    pub token: Token,
    pub category_token: Token,
    pub annotation_tokens: Vec<Token>,
}

impl InstanceInner {
    pub fn from(
        instance: Instance,
        sample_annotation_map: &HashMap<Token, SampleAnnotation>,
    ) -> Result<Self> {
        let Instance {
            token,
            nbr_annotations,
            category_token,
            first_annotation_token,
            last_annotation_token,
        } = instance;

        let mut annotation_token_opt = Some(first_annotation_token);
        let mut annotation_tokens = vec![];

        while let Some(annotation_token) = annotation_token_opt {
            let annotation = &sample_annotation_map
                .get(&annotation_token)
                .expect("internal error: invalid annotation_token");
            assert_eq!(
                annotation_token, annotation.token,
                "internal error: annotation.token mismatch"
            );
            annotation_tokens.push(annotation_token);
            annotation_token_opt = annotation.next;
        }

        if annotation_tokens.len() != nbr_annotations {
            let msg = format!(
                "the instance with token {} assures nbr_annotations = {}, but in fact {}",
                token,
                nbr_annotations,
                annotation_tokens.len()
            );
            return Err(Error::CorruptedDataset(msg));
        }
        if annotation_tokens.last().unwrap() != &last_annotation_token {
            let msg = format!(
                "the instance with token {} assures last_annotation_token = {}, but in fact {}",
                token,
                last_annotation_token,
                annotation_tokens.last().unwrap()
            );
            return Err(Error::CorruptedDataset(msg));
        }

        let ret = Self {
            token,
            category_token,
            annotation_tokens,
        };
        Ok(ret)
    }
}

#[derive(Debug, Clone)]
pub struct SceneInner {
    pub token: Token,
    pub name: String,
    pub description: String,
    pub log_token: Token,
    pub sample_tokens: Vec<Token>,
}

impl SceneInner {
    pub fn from(scene: Scene, sample_map: &HashMap<Token, Sample>) -> Result<Self> {
        let Scene {
            token,
            name,
            description,
            log_token,
            nbr_samples,
            first_sample_token,
            last_sample_token,
        } = scene;

        let mut sample_tokens = vec![];
        let mut sample_token_opt = Some(first_sample_token);

        while let Some(sample_token) = sample_token_opt {
            let sample = &sample_map[&sample_token];
            assert_eq!(
                sample.token, sample_token,
                "internal error: sample.token mismatch"
            );
            sample_tokens.push(sample_token);
            sample_token_opt = sample.next;
        }

        if sample_tokens.len() != nbr_samples {
            let msg = format!(
                "the sample with token {} assures nbr_samples = {}, but in fact {}",
                token,
                nbr_samples,
                sample_tokens.len()
            );
            return Err(Error::CorruptedDataset(msg));
        }
        if *sample_tokens.last().unwrap() != last_sample_token {
            let msg = format!(
                "the sample with token {} assures last_sample_token = {}, but in fact {}",
                token,
                last_sample_token,
                sample_tokens.last().unwrap()
            );
            return Err(Error::CorruptedDataset(msg));
        }

        let ret = Self {
            token,
            name,
            description,
            log_token,
            sample_tokens,
        };
        Ok(ret)
    }
}
