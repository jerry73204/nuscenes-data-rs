use crate::{
    error::{NuScenesDataError, NuScenesDataResult},
    serializable::{Instance, Sample, SampleAnnotation, Scene},
    token::LongToken,
};
use chrono::NaiveDateTime;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SampleInternal {
    pub token: LongToken,
    pub next: Option<LongToken>,
    pub prev: Option<LongToken>,
    pub timestamp: NaiveDateTime,
    pub scene_token: LongToken,
    pub annotation_tokens: Vec<LongToken>,
    pub sample_data_tokens: Vec<LongToken>,
}

impl SampleInternal {
    pub fn from(
        sample: Sample,
        annotation_tokens: Vec<LongToken>,
        sample_data_tokens: Vec<LongToken>,
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
pub struct InstanceInternal {
    pub token: LongToken,
    pub category_token: LongToken,
    pub annotation_tokens: Vec<LongToken>,
}

impl InstanceInternal {
    pub fn from(
        instance: Instance,
        sample_annotation_map: &HashMap<LongToken, SampleAnnotation>,
    ) -> NuScenesDataResult<Self> {
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
                .ok_or(NuScenesDataError::InternalBug)?;
            if annotation_token != annotation.token {
                return Err(NuScenesDataError::InternalBug);
            }
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
            return Err(NuScenesDataError::CorruptedDataset(msg));
        }
        if annotation_tokens.last().unwrap() != &last_annotation_token {
            let msg = format!(
                "the instance with token {} assures last_annotation_token = {}, but in fact {}",
                token,
                last_annotation_token,
                annotation_tokens.last().unwrap()
            );
            return Err(NuScenesDataError::CorruptedDataset(msg));
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
pub struct SceneInternal {
    pub token: LongToken,
    pub name: String,
    pub description: String,
    pub log_token: LongToken,
    pub sample_tokens: Vec<LongToken>,
}

impl SceneInternal {
    pub fn from(scene: Scene, sample_map: &HashMap<LongToken, Sample>) -> NuScenesDataResult<Self> {
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
            if sample.token != sample_token {
                return Err(NuScenesDataError::InternalBug);
            }
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
            return Err(NuScenesDataError::CorruptedDataset(msg));
        }
        if *sample_tokens.last().unwrap() != last_sample_token {
            let msg = format!(
                "the sample with token {} assures last_sample_token = {}, but in fact {}",
                token,
                last_sample_token,
                sample_tokens.last().unwrap()
            );
            return Err(NuScenesDataError::CorruptedDataset(msg));
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
