use crate::{
    error::{Error, Result},
    parsed::{InstanceInternal, SampleInternal, SceneInternal},
    serializable::{
        Attribute, CalibratedSensor, Category, EgoPose, Instance, Log, Map, Sample,
        SampleAnnotation, SampleData, Scene, Sensor, Token, Visibility, VisibilityToken, WithToken,
    },
};
use chrono::NaiveDateTime;
use image::DynamicImage;
use itertools::Itertools;
use nalgebra::MatrixXx5;
use serde::Deserialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

pub type PointCloudMatrix = MatrixXx5<f32>;

#[derive(Debug, Clone)]
pub struct DatasetLoader {
    pub check: bool,
}

impl DatasetLoader {
    /// Load the dataset directory.
    ///
    /// ```rust
    /// use nuscenes_data::{DatasetLoader, Result};
    ///
    /// fn main() -> Result<()> {
    ///     let loader = DatasetLoader { check: true };
    ///     let dataset = loader.load("1.02", "/path/to/your/dataset")?;
    ///     OK(())
    /// }
    /// ```
    pub fn load<P>(&self, version: &str, dir: P) -> Result<Dataset>
    where
        P: AsRef<Path>,
    {
        let Self { check } = *self;
        let dataset_dir = dir.as_ref();
        let meta_dir = dataset_dir.join(version);

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
        let visibility_map: HashMap<VisibilityToken, Visibility> = visibility_list
            .into_iter()
            .map(|visibility| (visibility.token, visibility))
            .collect();

        if check {
            // check calibrated sensor integrity
            for calibrated_sensor in calibrated_sensor_map.values() {
                if !sensor_map.contains_key(&calibrated_sensor.sensor_token) {
                    let msg = format!(
                        "the token {} does not refer to any sensor",
                        calibrated_sensor.sensor_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }
            }

            // check instance integrity
            for (instance_token, instance) in &instance_map {
                if !sample_annotation_map.contains_key(&instance.first_annotation_token) {
                    let msg = format!(
                        "the token {} does not refer to any sample annotation",
                        instance.first_annotation_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                if !sample_annotation_map.contains_key(&instance.last_annotation_token) {
                    let msg = format!(
                        "the token {} does not refer to any sample annotation",
                        instance.last_annotation_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                if !category_map.contains_key(&instance.category_token) {
                    let msg = format!(
                        "the token {} does not refer to any sample category",
                        instance.category_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                let mut annotation_token = &instance.first_annotation_token;
                let mut prev_annotation_token = None;
                let mut count = 0;

                loop {
                    let annotation = match sample_annotation_map.get(annotation_token) {
                    Some(annotation) => annotation,
                    None => {
                        match prev_annotation_token {
                            Some(prev) => return Err(Error::CorruptedDataset(format!("the sample_annotation with token {} points to next token {} that does not exist", prev, annotation_token))),
                            None => return Err(Error::CorruptedDataset(format!("the instance with token {} points to first_annotation_token {} that does not exist", instance_token, annotation_token))),
                        }
                    }
                };

                    if prev_annotation_token != annotation.prev.as_ref() {
                        let msg = format!(
                            "the prev field is not correct in sample annotation with token {}",
                            annotation_token
                        );
                        return Err(Error::CorruptedDataset(msg));
                    }
                    count += 1;

                    prev_annotation_token = Some(annotation_token);
                    annotation_token = match &annotation.next {
                        Some(next) => next,
                        None => {
                            if &instance.last_annotation_token != annotation_token {
                                let msg = format!("the last_annotation_token is not correct in instance with token {}",
                                                  instance_token);
                                return Err(Error::CorruptedDataset(msg));
                            }

                            if count != instance.nbr_annotations {
                                let msg = format!(
                                    "the nbr_annotations is not correct in instance with token {}",
                                    instance_token
                                );
                                return Err(Error::CorruptedDataset(msg));
                            }
                            break;
                        }
                    };
                }
            }

            // check map integrity
            for map in map_map.values() {
                for token in &map.log_tokens {
                    if !log_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any log", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
                }
            }

            // check scene integrity
            for (scene_token, scene) in &scene_map {
                if !log_map.contains_key(&scene.log_token) {
                    let msg = format!("the token {} does not refer to any log", scene.log_token);
                    return Err(Error::CorruptedDataset(msg));
                }

                if !sample_map.contains_key(&scene.first_sample_token) {
                    let msg = format!(
                        "the token {} does not refer to any sample",
                        scene.first_sample_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                if !sample_map.contains_key(&scene.last_sample_token) {
                    let msg = format!(
                        "the token {} does not refer to any sample",
                        scene.last_sample_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                let mut prev_sample_token = None;
                let mut sample_token = &scene.first_sample_token;
                let mut count = 0;

                loop {
                    let sample = match sample_map.get(sample_token) {
                    Some(sample) => sample,
                    None => {
                        match prev_sample_token {
                            Some(prev) => return Err(Error::CorruptedDataset(format!("the sample with token {} points to a next token {} that does not exist", prev, sample_token))),
                            None => return Err(Error::CorruptedDataset(format!("the scene with token {} points to first_sample_token {} that does not exist", scene_token, sample_token))),
                        }
                    }
                };
                    if prev_sample_token != sample.prev.as_ref() {
                        let msg = format!(
                            "the prev field in sample with token {} is not correct",
                            sample_token
                        );
                        return Err(Error::CorruptedDataset(msg));
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
                                return Err(Error::CorruptedDataset(msg));
                            }
                            if count != scene.nbr_samples {
                                let msg = format!(
                                    "the nbr_samples in scene with token {} is not correct",
                                    scene_token
                                );
                                return Err(Error::CorruptedDataset(msg));
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
                    return Err(Error::CorruptedDataset(msg));
                }

                if let Some(token) = &sample.prev {
                    if !sample_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any sample", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
                }

                if let Some(token) = &sample.next {
                    if !sample_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any sample", token);
                        return Err(Error::CorruptedDataset(msg));
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
                    return Err(Error::CorruptedDataset(msg));
                }

                if !instance_map.contains_key(&sample_annotation.instance_token) {
                    let msg = format!(
                        "the token {} does not refer to any instance",
                        sample_annotation.instance_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                for token in sample_annotation.attribute_tokens.iter() {
                    if !attribute_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any attribute", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
                }

                if let Some(token) = &sample_annotation.visibility_token {
                    if !visibility_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any visibility", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
                }

                if let Some(token) = &sample_annotation.prev {
                    if !sample_annotation_map.contains_key(token) {
                        let msg = format!(
                            "the token {} does not refer to any sample annotation",
                            token
                        );
                        return Err(Error::CorruptedDataset(msg));
                    }
                }

                if let Some(token) = &sample_annotation.next {
                    if !sample_annotation_map.contains_key(token) {
                        let msg = format!(
                            "the token {} does not refer to any sample annotation",
                            token
                        );
                        return Err(Error::CorruptedDataset(msg));
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
                    return Err(Error::CorruptedDataset(msg));
                }

                if !ego_pose_map.contains_key(&sample_data.ego_pose_token) {
                    let msg = format!(
                        "the token {} does not refer to any ego pose",
                        sample_data.ego_pose_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                if !calibrated_sensor_map.contains_key(&sample_data.calibrated_sensor_token) {
                    let msg = format!(
                        "the token {} does not refer to any calibrated sensor",
                        sample_data.calibrated_sensor_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                if let Some(token) = &sample_data.prev {
                    if !sample_data_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any sample data", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
                }

                if let Some(token) = &sample_data.next {
                    if !sample_data_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any sample data", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
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
        let instance_internal_map: HashMap<Token, InstanceInternal> = instance_map
            .into_iter()
            .map(|(instance_token, instance)| -> Result<_> {
                let ret = InstanceInternal::from(instance, &sample_annotation_map)?;
                Ok((instance_token, ret))
            })
            .try_collect()?;

        let scene_internal_map: HashMap<_, _> = scene_map
            .into_iter()
            .map(|(scene_token, scene)| -> Result<_> {
                let internal = SceneInternal::from(scene, &sample_map)?;
                Ok((scene_token, internal))
            })
            .try_collect()?;

        let sample_internal_map: HashMap<_, _> = sample_map
            .into_iter()
            .map(|(sample_token, sample)| -> Result<_> {
                let sample_data_tokens = sample_to_sample_data_groups
                    .remove(&sample_token)
                    .ok_or(Error::InternalBug)?;
                let annotation_tokens = sample_to_annotation_groups
                    .remove(&sample_token)
                    .ok_or(Error::InternalBug)?;
                let internal = SampleInternal::from(sample, annotation_tokens, sample_data_tokens);
                Ok((sample_token, internal))
            })
            .try_collect()?;

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
            let mut sorted_pairs: Vec<_> = scene_internal_map
                .iter()
                .map(|(scene_token, scene)| -> Result<_> {
                    let timestamp = scene
                        .sample_tokens
                        .iter()
                        .map(|sample_token| {
                            let sample = sample_internal_map
                                .get(sample_token)
                                .ok_or(Error::InternalBug)?;
                            Ok(sample.timestamp)
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .min()
                        .ok_or(Error::InternalBug)?;

                    Ok((scene_token, timestamp))
                })
                .try_collect()?;
            sorted_pairs.sort_by_cached_key(|(_, timestamp)| *timestamp);

            sorted_pairs.into_iter().map(|(token, _)| *token).collect()
        };

        // construct result
        let ret = Dataset {
            version: version.to_string(),
            dataset_dir: dataset_dir.to_owned(),
            attribute_map,
            calibrated_sensor_map,
            category_map,
            ego_pose_map,
            instance_map: instance_internal_map,
            log_map,
            map_map,
            sample_map: sample_internal_map,
            sample_annotation_map,
            sample_data_map,
            scene_map: scene_internal_map,
            sensor_map,
            visibility_map,
            sorted_ego_pose_tokens,
            sorted_scene_tokens,
            sorted_sample_tokens,
            sorted_sample_data_tokens,
        };

        Ok(ret)
    }

    pub async fn load_async<P>(&self, version: &str, dir: P) -> Result<Dataset>
    where
        P: AsRef<Path>,
    {
        use futures::prelude::*;
        use tokio::task::{spawn, spawn_blocking};

        let Self { check } = *self;
        let dataset_dir = dir.as_ref();
        let meta_dir = dataset_dir.join(version);

        let (
            attribute_list,
            calibrated_sensor_list,
            category_list,
            ego_pose_list,
            instance_list,
            log_list,
            map_list,
            sample_list,
            sample_annotation_list,
            sample_data_list,
            scene_list,
            sensor_list,
            visibility_list,
        ): (
            Vec<Attribute>,
            Vec<CalibratedSensor>,
            Vec<Category>,
            Vec<EgoPose>,
            Vec<Instance>,
            Vec<Log>,
            Vec<Map>,
            Vec<Sample>,
            Vec<SampleAnnotation>,
            Vec<SampleData>,
            Vec<Scene>,
            Vec<Sensor>,
            Vec<Visibility>,
        ) = futures::try_join!(
            spawn(load_json_async(meta_dir.join("attribute.json"))).map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("calibrated_sensor.json")))
                .map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("category.json"))).map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("ego_pose.json"))).map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("instance.json"))).map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("log.json"))).map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("map.json"))).map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("sample.json"))).map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("sample_annotation.json")))
                .map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("sample_data.json"))).map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("scene.json"))).map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("sensor.json"))).map(|result| result.unwrap()),
            spawn(load_json_async(meta_dir.join("visibility.json"))).map(|result| result.unwrap()),
        )?;

        // index items by tokens
        let (
            attribute_map,
            calibrated_sensor_map,
            category_map,
            ego_pose_map,
            instance_map,
            log_map,
            map_map,
            sample_annotation_map,
            sample_data_map,
            sample_map,
            scene_map,
            sensor_map,
            visibility_map,
        ) = futures::try_join!(
            spawn_blocking(move || vec_to_hashmap(attribute_list)),
            spawn_blocking(move || vec_to_hashmap(calibrated_sensor_list)),
            spawn_blocking(move || vec_to_hashmap(category_list)),
            spawn_blocking(move || vec_to_hashmap(ego_pose_list)),
            spawn_blocking(move || vec_to_hashmap(instance_list)),
            spawn_blocking(move || vec_to_hashmap(log_list)),
            spawn_blocking(move || vec_to_hashmap(map_list)),
            spawn_blocking(move || vec_to_hashmap(sample_annotation_list)),
            spawn_blocking(move || vec_to_hashmap(sample_data_list)),
            spawn_blocking(move || vec_to_hashmap(sample_list)),
            spawn_blocking(move || vec_to_hashmap(scene_list)),
            spawn_blocking(move || vec_to_hashmap(sensor_list)),
            spawn_blocking(move || visibility_list
                .into_iter()
                .map(|item| (item.token, item))
                .collect::<HashMap<_, _>>()),
        )
        .unwrap();

        if check {
            // check calibrated sensor integrity
            for calibrated_sensor in calibrated_sensor_map.values() {
                if !sensor_map.contains_key(&calibrated_sensor.sensor_token) {
                    let msg = format!(
                        "the token {} does not refer to any sensor",
                        calibrated_sensor.sensor_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }
            }

            // check instance integrity
            for (instance_token, instance) in &instance_map {
                if !sample_annotation_map.contains_key(&instance.first_annotation_token) {
                    let msg = format!(
                        "the token {} does not refer to any sample annotation",
                        instance.first_annotation_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                if !sample_annotation_map.contains_key(&instance.last_annotation_token) {
                    let msg = format!(
                        "the token {} does not refer to any sample annotation",
                        instance.last_annotation_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                if !category_map.contains_key(&instance.category_token) {
                    let msg = format!(
                        "the token {} does not refer to any sample category",
                        instance.category_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                let mut annotation_token = &instance.first_annotation_token;
                let mut prev_annotation_token = None;
                let mut count = 0;

                loop {
                    let annotation = match sample_annotation_map.get(annotation_token) {
                    Some(annotation) => annotation,
                    None => {
                        match prev_annotation_token {
                            Some(prev) => return Err(Error::CorruptedDataset(format!("the sample_annotation with token {} points to next token {} that does not exist", prev, annotation_token))),
                            None => return Err(Error::CorruptedDataset(format!("the instance with token {} points to first_annotation_token {} that does not exist", instance_token, annotation_token))),
                        }
                    }
                };

                    if prev_annotation_token != annotation.prev.as_ref() {
                        let msg = format!(
                            "the prev field is not correct in sample annotation with token {}",
                            annotation_token
                        );
                        return Err(Error::CorruptedDataset(msg));
                    }
                    count += 1;

                    prev_annotation_token = Some(annotation_token);
                    annotation_token = match &annotation.next {
                        Some(next) => next,
                        None => {
                            if &instance.last_annotation_token != annotation_token {
                                let msg = format!("the last_annotation_token is not correct in instance with token {}",
                                                  instance_token);
                                return Err(Error::CorruptedDataset(msg));
                            }

                            if count != instance.nbr_annotations {
                                let msg = format!(
                                    "the nbr_annotations is not correct in instance with token {}",
                                    instance_token
                                );
                                return Err(Error::CorruptedDataset(msg));
                            }
                            break;
                        }
                    };
                }
            }

            // check map integrity
            for map in map_map.values() {
                for token in &map.log_tokens {
                    if !log_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any log", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
                }
            }

            // check scene integrity
            for (scene_token, scene) in &scene_map {
                if !log_map.contains_key(&scene.log_token) {
                    let msg = format!("the token {} does not refer to any log", scene.log_token);
                    return Err(Error::CorruptedDataset(msg));
                }

                if !sample_map.contains_key(&scene.first_sample_token) {
                    let msg = format!(
                        "the token {} does not refer to any sample",
                        scene.first_sample_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                if !sample_map.contains_key(&scene.last_sample_token) {
                    let msg = format!(
                        "the token {} does not refer to any sample",
                        scene.last_sample_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                let mut prev_sample_token = None;
                let mut sample_token = &scene.first_sample_token;
                let mut count = 0;

                loop {
                    let sample = match sample_map.get(sample_token) {
                    Some(sample) => sample,
                    None => {
                        match prev_sample_token {
                            Some(prev) => return Err(Error::CorruptedDataset(format!("the sample with token {} points to a next token {} that does not exist", prev, sample_token))),
                            None => return Err(Error::CorruptedDataset(format!("the scene with token {} points to first_sample_token {} that does not exist", scene_token, sample_token))),
                        }
                    }
                };
                    if prev_sample_token != sample.prev.as_ref() {
                        let msg = format!(
                            "the prev field in sample with token {} is not correct",
                            sample_token
                        );
                        return Err(Error::CorruptedDataset(msg));
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
                                return Err(Error::CorruptedDataset(msg));
                            }
                            if count != scene.nbr_samples {
                                let msg = format!(
                                    "the nbr_samples in scene with token {} is not correct",
                                    scene_token
                                );
                                return Err(Error::CorruptedDataset(msg));
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
                    return Err(Error::CorruptedDataset(msg));
                }

                if let Some(token) = &sample.prev {
                    if !sample_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any sample", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
                }

                if let Some(token) = &sample.next {
                    if !sample_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any sample", token);
                        return Err(Error::CorruptedDataset(msg));
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
                    return Err(Error::CorruptedDataset(msg));
                }

                if !instance_map.contains_key(&sample_annotation.instance_token) {
                    let msg = format!(
                        "the token {} does not refer to any instance",
                        sample_annotation.instance_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                for token in sample_annotation.attribute_tokens.iter() {
                    if !attribute_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any attribute", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
                }

                if let Some(token) = &sample_annotation.visibility_token {
                    if !visibility_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any visibility", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
                }

                if let Some(token) = &sample_annotation.prev {
                    if !sample_annotation_map.contains_key(token) {
                        let msg = format!(
                            "the token {} does not refer to any sample annotation",
                            token
                        );
                        return Err(Error::CorruptedDataset(msg));
                    }
                }

                if let Some(token) = &sample_annotation.next {
                    if !sample_annotation_map.contains_key(token) {
                        let msg = format!(
                            "the token {} does not refer to any sample annotation",
                            token
                        );
                        return Err(Error::CorruptedDataset(msg));
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
                    return Err(Error::CorruptedDataset(msg));
                }

                if !ego_pose_map.contains_key(&sample_data.ego_pose_token) {
                    let msg = format!(
                        "the token {} does not refer to any ego pose",
                        sample_data.ego_pose_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                if !calibrated_sensor_map.contains_key(&sample_data.calibrated_sensor_token) {
                    let msg = format!(
                        "the token {} does not refer to any calibrated sensor",
                        sample_data.calibrated_sensor_token
                    );
                    return Err(Error::CorruptedDataset(msg));
                }

                if let Some(token) = &sample_data.prev {
                    if !sample_data_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any sample data", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
                }

                if let Some(token) = &sample_data.next {
                    if !sample_data_map.contains_key(token) {
                        let msg = format!("the token {} does not refer to any sample data", token);
                        return Err(Error::CorruptedDataset(msg));
                    }
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
        let instance_internal_map: HashMap<Token, InstanceInternal> = instance_map
            .into_iter()
            .map(|(instance_token, instance)| -> Result<_> {
                let ret = InstanceInternal::from(instance, &sample_annotation_map)?;
                Ok((instance_token, ret))
            })
            .try_collect()?;

        let scene_internal_map: HashMap<_, _> = scene_map
            .into_iter()
            .map(|(scene_token, scene)| -> Result<_> {
                let internal = SceneInternal::from(scene, &sample_map)?;
                Ok((scene_token, internal))
            })
            .try_collect()?;

        let sample_internal_map: HashMap<_, _> = sample_map
            .into_iter()
            .map(|(sample_token, sample)| -> Result<_> {
                let sample_data_tokens = sample_to_sample_data_groups
                    .remove(&sample_token)
                    .ok_or(Error::InternalBug)?;
                let annotation_tokens = sample_to_annotation_groups
                    .remove(&sample_token)
                    .ok_or(Error::InternalBug)?;
                let internal = SampleInternal::from(sample, annotation_tokens, sample_data_tokens);
                Ok((sample_token, internal))
            })
            .try_collect()?;

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
            let mut sorted_pairs: Vec<_> = scene_internal_map
                .iter()
                .map(|(scene_token, scene)| -> Result<_> {
                    let timestamp = scene
                        .sample_tokens
                        .iter()
                        .map(|sample_token| {
                            let sample = sample_internal_map
                                .get(sample_token)
                                .ok_or(Error::InternalBug)?;
                            Ok(sample.timestamp)
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .min()
                        .ok_or(Error::InternalBug)?;

                    Ok((scene_token, timestamp))
                })
                .try_collect()?;
            sorted_pairs.sort_by_cached_key(|(_, timestamp)| *timestamp);

            sorted_pairs.into_iter().map(|(token, _)| *token).collect()
        };

        // construct result
        let ret = Dataset {
            version: version.to_string(),
            dataset_dir: dataset_dir.to_owned(),
            attribute_map,
            calibrated_sensor_map,
            category_map,
            ego_pose_map,
            instance_map: instance_internal_map,
            log_map,
            map_map,
            sample_map: sample_internal_map,
            sample_annotation_map,
            sample_data_map,
            scene_map: scene_internal_map,
            sensor_map,
            visibility_map,
            sorted_ego_pose_tokens,
            sorted_scene_tokens,
            sorted_sample_tokens,
            sorted_sample_data_tokens,
        };

        Ok(ret)
    }
}

impl Default for DatasetLoader {
    fn default() -> Self {
        Self { check: false }
    }
}

#[derive(Debug, Clone)]
pub struct Dataset {
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

impl Dataset {
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
    /// use nuscenes_data::{Dataset, Result};
    ///
    /// fn main() -> Result<()> {
    ///     let dataset = Dataset::load("1.02", "/path/to/your/dataset")?;
    ///     OK(())
    /// }
    /// ```
    pub fn load<P>(version: &str, dir: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        DatasetLoader::default().load(version, dir)
    }

    pub async fn load_async<P>(version: &str, dir: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        DatasetLoader::default().load_async(version, dir).await
    }
}

#[derive(Clone)]
pub enum LoadedSampleData {
    PointCloud(PointCloudMatrix),
    Image(DynamicImage),
}

fn load_json<T, P>(path: P) -> Result<T>
where
    P: AsRef<Path>,
    T: for<'a> Deserialize<'a>,
{
    use std::{fs::File, io::BufReader};

    let reader = BufReader::new(File::open(path.as_ref())?);
    let value = serde_json::from_reader(reader).map_err(|err| {
        let msg = format!("failed to load file {}: {:?}", path.as_ref().display(), err);
        Error::CorruptedDataset(msg)
    })?;
    Ok(value)
}

async fn load_json_async<T, P>(path: P) -> Result<T>
where
    P: AsRef<Path>,
    T: for<'a> Deserialize<'a>,
{
    let path = path.as_ref();
    let text = tokio::fs::read_to_string(path).await?;
    let value = serde_json::from_str(&text).map_err(|err| {
        let msg = format!("failed to load file {}: {:?}", path.display(), err);
        Error::CorruptedDataset(msg)
    })?;
    Ok(value)
}

fn vec_to_hashmap<T>(vec: Vec<T>) -> HashMap<Token, T>
where
    T: WithToken,
{
    vec.into_iter().map(|item| (item.token(), item)).collect()
}
