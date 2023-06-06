use crate::{
    error::{NuScenesDataError, NuScenesDataResult},
    parsed::{InstanceInternal, SampleInternal, SceneInternal},
    token::Token,
    types::{
        Attribute, CalibratedSensor, Category, EgoPose, Instance, Log, Map, Sample,
        SampleAnnotation, SampleData, Scene, Sensor, Visibility,
    },
};

use chrono::NaiveDateTime;
use image::DynamicImage;
use itertools::Itertools;
use nalgebra::MatrixXx5;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    time::Instant,
};

pub type PointCloudMatrix = MatrixXx5<f32>;

#[derive(Debug, Clone)]
pub struct NuScenesDataset {
    pub version: String,
    pub dataset_dir: PathBuf,
    pub attribute: HashMap<Token, Attribute>,
    pub calibrated_sensor: HashMap<Token, CalibratedSensor>,
    pub category: HashMap<Token, Category>,
    pub ego_pose: HashMap<Token, EgoPose>,
    pub instance: HashMap<Token, InstanceInternal>,
    pub log: HashMap<Token, Log>,
    pub map: HashMap<Token, Map>,
    pub scene: HashMap<Token, SceneInternal>,
    pub sample: HashMap<Token, SampleInternal>,
    pub sample_annotation: HashMap<Token, SampleAnnotation>,
    pub sample_data: HashMap<Token, SampleData>,
    pub sensor: HashMap<Token, Sensor>,
    pub visibility: HashMap<String, Visibility>,
    pub sorted_ego_pose_tokens: Vec<Token>,
    pub sorted_sample_tokens: Vec<Token>,
    pub sorted_sample_data_tokens: Vec<Token>,
    pub sorted_scene_tokens: Vec<Token>,
}

impl NuScenesDataset {
    /// Gets version of the dataset.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Gets the directory of dataset.
    pub fn dir(&self) -> &Path {
        &self.dataset_dir
    }

    /// Load the dataset directory.
    ///
    /// ```rust
    /// use nuscenes_data::NuScenesDataset;
    ///
    /// fn main() -> NuscenesDataResult<()> {
    ///     let dataset = NuScenesDataset::load("1.02", "/path/to/your/dataset")?;
    ///     OK(())
    /// }
    /// ```
    pub fn load<P>(version: &str, dir: P) -> NuScenesDataResult<Self>
    where
        P: AsRef<Path>,
    {
        let dataset_dir = dir.as_ref();

        let meta_dir_name = version;
        let meta_dir = dataset_dir.join(meta_dir_name);

        let since = Instant::now();
        let attribute_list: Vec<Attribute> = {
            let attribute_path = meta_dir.join("attribute.json");
            load_json(attribute_path)?
        };
        let calibrated_sensor_list: Vec<CalibratedSensor> = {
            let calibrated_sensor_path = meta_dir.join("calibrated_sensor.json");
            load_json(calibrated_sensor_path)?
        };
        let category_list: Vec<Category> = {
            let category_path = meta_dir.join("category.json");
            load_json(category_path)?
        };
        let ego_pose_list: Vec<EgoPose> = {
            let ego_pose_path = meta_dir.join("ego_pose.json");
            load_json(ego_pose_path)?
        };
        let instance_list: Vec<Instance> = {
            let instance_path = meta_dir.join("instance.json");
            load_json(instance_path)?
        };
        let log_list: Vec<Log> = {
            let log_path = meta_dir.join("log.json");
            load_json(log_path)?
        };
        let map_list: Vec<Map> = {
            let map_path = meta_dir.join("map.json");
            load_json(map_path)?
        };
        let sample_list: Vec<Sample> = {
            let sample_path = meta_dir.join("sample.json");
            load_json(sample_path)?
        };
        let sample_annotation_list: Vec<SampleAnnotation> = {
            let sample_annotation_path = meta_dir.join("sample_annotation.json");
            load_json(sample_annotation_path)?
        };
        let sample_data_list: Vec<SampleData> = {
            let sample_data_path = meta_dir.join("sample_data.json");
            load_json(sample_data_path)?
        };
        let scene_list: Vec<Scene> = {
            let scene_path = meta_dir.join("scene.json");
            load_json(scene_path)?
        };
        let sensor_list: Vec<Sensor> = {
            let sensor_path = meta_dir.join("sensor.json");
            load_json(sensor_path)?
        };
        let visibility_list: Vec<Visibility> = {
            let visibility_path = meta_dir.join("visibility.json");
            load_json(visibility_path)?
        };
        dbg!(since.elapsed());

        // index items by tokens
        let attribute_map: HashMap<Token, Attribute> = attribute_list
            .into_iter()
            .map(|attribute| (attribute.token, attribute))
            .collect();
        let calibrated_sensor_map: HashMap<Token, CalibratedSensor> = calibrated_sensor_list
            .into_iter()
            .map(|calibrated_sensor| (calibrated_sensor.token, calibrated_sensor))
            .collect();
        let category_map: HashMap<Token, Category> = category_list
            .into_iter()
            .map(|category| (category.token, category))
            .collect();
        let ego_pose_map: HashMap<Token, EgoPose> = ego_pose_list
            .into_iter()
            .map(|ego_pos| (ego_pos.token, ego_pos))
            .collect();
        let instance_map: HashMap<Token, Instance> = instance_list
            .into_iter()
            .map(|instance| (instance.token, instance))
            .collect();
        let log_map: HashMap<Token, Log> =
            log_list.into_iter().map(|log| (log.token, log)).collect();
        let map_map: HashMap<Token, Map> =
            map_list.into_iter().map(|map| (map.token, map)).collect();
        let sample_annotation_map: HashMap<Token, SampleAnnotation> = sample_annotation_list
            .into_iter()
            .map(|sample| (sample.token, sample))
            .collect();
        let sample_data_map: HashMap<Token, SampleData> = sample_data_list
            .into_iter()
            .map(|sample| (sample.token, sample))
            .collect();
        let sample_map: HashMap<Token, Sample> = sample_list
            .into_iter()
            .map(|sample| (sample.token, sample))
            .collect();
        let scene_map: HashMap<Token, Scene> = scene_list
            .into_iter()
            .map(|scene| (scene.token, scene))
            .collect();
        let sensor_map: HashMap<Token, Sensor> = sensor_list
            .into_iter()
            .map(|sensor| (sensor.token, sensor))
            .collect();
        let visibility_map: HashMap<String, Visibility> = visibility_list
            .into_iter()
            .map(|visibility| (visibility.token.clone(), visibility))
            .collect();

        // check calibrated sensor integrity
        for (_, calibrated_sensor) in calibrated_sensor_map.iter() {
            if !sensor_map.contains_key(&calibrated_sensor.sensor_token) {
                let msg = format!(
                    "the token {} does not refer to any sensor",
                    calibrated_sensor.sensor_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }
        }

        // check instance integrity
        for (instance_token, instance) in instance_map.iter() {
            if !sample_annotation_map.contains_key(&instance.first_annotation_token) {
                let msg = format!(
                    "the token {} does not refer to any sample annotation",
                    instance.first_annotation_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            if !sample_annotation_map.contains_key(&instance.last_annotation_token) {
                let msg = format!(
                    "the token {} does not refer to any sample annotation",
                    instance.last_annotation_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            if !category_map.contains_key(&instance.category_token) {
                let msg = format!(
                    "the token {} does not refer to any sample category",
                    instance.category_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            let mut annotation_token = &instance.first_annotation_token;
            let mut prev_annotation_token = None;
            let mut count = 0;

            loop {
                let annotation = match sample_annotation_map.get(annotation_token) {
                    Some(annotation) => annotation,
                    None => {
                        match prev_annotation_token {
                            Some(prev) => return Err(NuScenesDataError::CorruptedDataset(format!("the sample_annotation with token {} points to next token {} that does not exist", prev, annotation_token))),
                            None => return Err(NuScenesDataError::CorruptedDataset(format!("the instance with token {} points to first_annotation_token {} that does not exist", instance_token, annotation_token))),
                        }
                    }
                };

                if prev_annotation_token != annotation.prev.as_ref() {
                    let msg = format!(
                        "the prev field is not correct in sample annotation with token {}",
                        annotation_token
                    );
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
                count += 1;

                prev_annotation_token = Some(annotation_token);
                annotation_token = match &annotation.next {
                    Some(next) => next,
                    None => {
                        if &instance.last_annotation_token != annotation_token {
                            let msg = format!("the last_annotation_token is not correct in instance with token {}",
                                                  instance_token);
                            return Err(NuScenesDataError::CorruptedDataset(msg));
                        }

                        if count != instance.nbr_annotations {
                            let msg = format!(
                                "the nbr_annotations is not correct in instance with token {}",
                                instance_token
                            );
                            return Err(NuScenesDataError::CorruptedDataset(msg));
                        }
                        break;
                    }
                };
            }
        }

        // check map integrity
        for (_, map) in map_map.iter() {
            for token in map.log_tokens.iter() {
                if !log_map.contains_key(token) {
                    let msg = format!("the token {} does not refer to any log", token);
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
            }
        }

        // check scene integrity
        for (scene_token, scene) in scene_map.iter() {
            if !log_map.contains_key(&scene.log_token) {
                let msg = format!("the token {} does not refer to any log", scene.log_token);
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            if !sample_map.contains_key(&scene.first_sample_token) {
                let msg = format!(
                    "the token {} does not refer to any sample",
                    scene.first_sample_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            if !sample_map.contains_key(&scene.last_sample_token) {
                let msg = format!(
                    "the token {} does not refer to any sample",
                    scene.last_sample_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            let mut prev_sample_token = None;
            let mut sample_token = &scene.first_sample_token;
            let mut count = 0;

            loop {
                let sample = match sample_map.get(sample_token) {
                    Some(sample) => sample,
                    None => {
                        match prev_sample_token {
                            Some(prev) => return Err(NuScenesDataError::CorruptedDataset(format!("the sample with token {} points to a next token {} that does not exist", prev, sample_token))),
                            None => return Err(NuScenesDataError::CorruptedDataset(format!("the scene with token {} points to first_sample_token {} that does not exist", scene_token, sample_token))),
                        }
                    }
                };
                if prev_sample_token != sample.prev.as_ref() {
                    let msg = format!(
                        "the prev field in sample with token {} is not correct",
                        sample_token
                    );
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
                prev_sample_token = Some(sample_token);
                count += 1;

                sample_token = match &sample.next {
                    Some(next) => next,
                    None => {
                        if sample_token != &scene.last_sample_token {
                            let msg = format!(
                                "the last_sample_token is not correct in scene with token {}",
                                scene_token
                            );
                            return Err(NuScenesDataError::CorruptedDataset(msg));
                        }
                        if count != scene.nbr_samples {
                            let msg = format!(
                                "the nbr_samples in scene with token {} is not correct",
                                scene_token
                            );
                            return Err(NuScenesDataError::CorruptedDataset(msg));
                        }
                        break;
                    }
                };
            }
        }

        // check sample integrity
        for (_, sample) in sample_map.iter() {
            if !scene_map.contains_key(&sample.scene_token) {
                let msg = format!(
                    "the token {} does not refer to any scene",
                    sample.scene_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            if let Some(token) = &sample.prev {
                if !sample_map.contains_key(token) {
                    let msg = format!("the token {} does not refer to any sample", token);
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
            }

            if let Some(token) = &sample.next {
                if !sample_map.contains_key(token) {
                    let msg = format!("the token {} does not refer to any sample", token);
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
            }
        }

        // check sample annotation integrity
        for (_, sample_annotation) in sample_annotation_map.iter() {
            if !sample_map.contains_key(&sample_annotation.sample_token) {
                let msg = format!(
                    "the token {} does not refer to any sample",
                    sample_annotation.sample_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            if !instance_map.contains_key(&sample_annotation.instance_token) {
                let msg = format!(
                    "the token {} does not refer to any instance",
                    sample_annotation.instance_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            for token in sample_annotation.attribute_tokens.iter() {
                if !attribute_map.contains_key(token) {
                    let msg = format!("the token {} does not refer to any attribute", token);
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
            }

            if let Some(token) = &sample_annotation.visibility_token {
                if !visibility_map.contains_key(token) {
                    let msg = format!("the token {} does not refer to any visibility", token);
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
            }

            if let Some(token) = &sample_annotation.prev {
                if !sample_annotation_map.contains_key(token) {
                    let msg = format!(
                        "the token {} does not refer to any sample annotation",
                        token
                    );
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
            }

            if let Some(token) = &sample_annotation.next {
                if !sample_annotation_map.contains_key(token) {
                    let msg = format!(
                        "the token {} does not refer to any sample annotation",
                        token
                    );
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
            }
        }

        // check sample data integrity
        for (_, sample_data) in sample_data_map.iter() {
            if !sample_map.contains_key(&sample_data.sample_token) {
                let msg = format!(
                    "the token {} does not refer to any sample",
                    sample_data.sample_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            if !ego_pose_map.contains_key(&sample_data.ego_pose_token) {
                let msg = format!(
                    "the token {} does not refer to any ego pose",
                    sample_data.ego_pose_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            if !calibrated_sensor_map.contains_key(&sample_data.calibrated_sensor_token) {
                let msg = format!(
                    "the token {} does not refer to any calibrated sensor",
                    sample_data.calibrated_sensor_token
                );
                return Err(NuScenesDataError::CorruptedDataset(msg));
            }

            if let Some(token) = &sample_data.prev {
                if !sample_data_map.contains_key(token) {
                    let msg = format!("the token {} does not refer to any sample data", token);
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
            }

            if let Some(token) = &sample_data.next {
                if !sample_data_map.contains_key(token) {
                    let msg = format!("the token {} does not refer to any sample data", token);
                    return Err(NuScenesDataError::CorruptedDataset(msg));
                }
            }
        }

        // keep track of relations from samples to sample annotations
        let mut sample_to_annotation_groups = sample_annotation_map
            .iter()
            .map(|(sample_annotation_token, sample_annotation)| {
                (sample_annotation.sample_token, *sample_annotation_token)
            })
            .into_group_map();

        // keep track of relations from samples to sample data
        let mut sample_to_sample_data_groups = sample_data_map
            .iter()
            .map(|(sample_data_token, sample_data)| (sample_data.sample_token, *sample_data_token))
            .into_group_map();

        // convert some types for ease of usage
        let instance_internal_map = instance_map
            .into_iter()
            .map(|(instance_token, instance)| {
                let ret = InstanceInternal::from(instance, &sample_annotation_map)?;
                Ok((instance_token, ret))
            })
            .collect::<NuScenesDataResult<HashMap<_, _>>>()?;

        let scene_internal_map = scene_map
            .into_iter()
            .map(|(scene_token, scene)| {
                let internal = SceneInternal::from(scene, &sample_map)?;
                Ok((scene_token, internal))
            })
            .collect::<NuScenesDataResult<HashMap<_, _>>>()?;

        let sample_internal_map = sample_map
            .into_iter()
            .map(|(sample_token, sample)| {
                let sample_data_tokens = sample_to_sample_data_groups
                    .remove(&sample_token)
                    .ok_or(NuScenesDataError::InternalBug)?;
                let annotation_tokens = sample_to_annotation_groups
                    .remove(&sample_token)
                    .ok_or(NuScenesDataError::InternalBug)?;
                let internal = SampleInternal::from(sample, annotation_tokens, sample_data_tokens);
                Ok((sample_token, internal))
            })
            .collect::<NuScenesDataResult<HashMap<_, _>>>()?;

        // sort ego_pose by timestamp
        let sorted_ego_pose_tokens: Vec<_> = {
            let mut sorted_pairs: Vec<(&Token, NaiveDateTime)> = ego_pose_map
                .iter()
                .map(|(sample_token, sample)| (sample_token, sample.timestamp))
                .collect();
            sorted_pairs.sort_by_cached_key(|(_, timestamp)| *timestamp);

            sorted_pairs.into_iter().map(|(token, _)| *token).collect()
        };

        // sort samples by timestamp
        let sorted_sample_tokens: Vec<_> = {
            let mut sorted_pairs: Vec<(&Token, NaiveDateTime)> = sample_internal_map
                .iter()
                .map(|(sample_token, sample)| (sample_token, sample.timestamp))
                .collect();
            sorted_pairs.sort_by_cached_key(|(_, timestamp)| *timestamp);

            sorted_pairs.into_iter().map(|(token, _)| *token).collect()
        };

        // sort sample data by timestamp
        let sorted_sample_data_tokens: Vec<_> = {
            let mut sorted_pairs: Vec<(&Token, NaiveDateTime)> = sample_data_map
                .iter()
                .map(|(sample_token, sample)| (sample_token, sample.timestamp))
                .collect();
            sorted_pairs.sort_by_cached_key(|(_, timestamp)| *timestamp);

            sorted_pairs.into_iter().map(|(token, _)| *token).collect()
        };

        // sort scenes by timestamp
        let sorted_scene_tokens: Vec<_> = {
            let mut sorted_pairs = scene_internal_map
                .iter()
                .map(|(scene_token, scene)| {
                    let timestamp = scene
                        .sample_tokens
                        .iter()
                        .map(|sample_token| {
                            let sample = sample_internal_map
                                .get(sample_token)
                                .ok_or(NuScenesDataError::InternalBug)?;
                            Ok(sample.timestamp)
                        })
                        .collect::<NuScenesDataResult<Vec<_>>>()?
                        .into_iter()
                        .min()
                        .ok_or(NuScenesDataError::InternalBug)?;

                    Ok((scene_token, timestamp))
                })
                .collect::<NuScenesDataResult<Vec<_>>>()?;
            sorted_pairs.sort_by_cached_key(|(_, timestamp)| *timestamp);

            sorted_pairs.into_iter().map(|(token, _)| *token).collect()
        };

        // construct result
        let ret = Self {
            version: version.to_string(),
            dataset_dir: dataset_dir.to_owned(),
            attribute: attribute_map,
            calibrated_sensor: calibrated_sensor_map,
            category: category_map,
            ego_pose: ego_pose_map,
            instance: instance_internal_map,
            log: log_map,
            map: map_map,
            sample: sample_internal_map,
            sample_annotation: sample_annotation_map,
            sample_data: sample_data_map,
            scene: scene_internal_map,
            sensor: sensor_map,
            visibility: visibility_map,
            sorted_ego_pose_tokens,
            sorted_scene_tokens,
            sorted_sample_tokens,
            sorted_sample_data_tokens,
        };

        Ok(ret)
    }
}

#[derive(Clone)]
pub enum LoadedSampleData {
    PointCloud(PointCloudMatrix),
    Image(DynamicImage),
}

fn load_json<T, P>(path: P) -> NuScenesDataResult<T>
where
    P: AsRef<Path>,
    T: for<'a> Deserialize<'a>,
{
    let reader = BufReader::new(File::open(path.as_ref())?);
    let value = serde_json::from_reader(reader).map_err(|err| {
        let msg = format!("failed to load file {}: {:?}", path.as_ref().display(), err);
        NuScenesDataError::CorruptedDataset(msg)
    })?;
    Ok(value)
}