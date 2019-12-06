#![feature(backtrace)]

pub mod error;
mod internal;
pub mod iter;
pub mod meta;

use crate::{
    error::NuSceneDataError,
    internal::{InstanceInternal, SampleInternal, SceneInternal},
    iter::Iter,
    meta::{
        Attribute, CalibratedSensor, Category, EgoPose, Instance, Log, LongToken, Map, Sample,
        SampleAnnotation, SampleData, Scene, Sensor, ShortToken, Visibility,
    },
};

use failure::{bail, ensure, Fallible};
use itertools::Itertools;
use serde::de::DeserializeOwned;
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    marker::PhantomData,
    path::{Path, PathBuf},
};

pub struct NuSceneDataset {
    name: String,
    dataset_dir: PathBuf,
    attribute_map: HashMap<LongToken, Attribute>,
    calibrated_sensor_map: HashMap<LongToken, CalibratedSensor>,
    category_map: HashMap<LongToken, Category>,
    ego_pose_map: HashMap<LongToken, EgoPose>,
    instance_map: HashMap<LongToken, InstanceInternal>,
    log_map: HashMap<LongToken, Log>,
    map_map: HashMap<ShortToken, Map>,
    scene_map: HashMap<LongToken, SceneInternal>,
    sample_map: HashMap<LongToken, SampleInternal>,
    sample_annotation_map: HashMap<LongToken, SampleAnnotation>,
    sample_data_map: HashMap<LongToken, SampleData>,
    sensor_map: HashMap<LongToken, Sensor>,
    visibility_map: HashMap<String, Visibility>,
    sorted_scene_tokens: Vec<LongToken>,
    sorted_sample_tokens: Vec<LongToken>,
}

impl NuSceneDataset {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dir(&self) -> &Path {
        &self.dataset_dir
    }

    pub fn load<S, P>(name: S, dir: P) -> Fallible<Self>
    where
        S: AsRef<str>,
        P: AsRef<Path>,
    {
        let dataset_dir = dir.as_ref();
        let meta_dir = dataset_dir.join(name.as_ref());

        // load JSON files
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
        let attribute_map = attribute_list
            .into_iter()
            .map(|attribute| (attribute.token.clone(), attribute))
            .collect::<HashMap<_, _>>();
        let calibrated_sensor_map = calibrated_sensor_list
            .into_iter()
            .map(|calibrated_sensor| (calibrated_sensor.token.clone(), calibrated_sensor))
            .collect::<HashMap<_, _>>();
        let category_map = category_list
            .into_iter()
            .map(|category| (category.token.clone(), category))
            .collect::<HashMap<_, _>>();
        let ego_pose_map = ego_pose_list
            .into_iter()
            .map(|ego_pos| (ego_pos.token.clone(), ego_pos))
            .collect::<HashMap<_, _>>();
        let instance_map = instance_list
            .into_iter()
            .map(|instance| (instance.token.clone(), instance))
            .collect::<HashMap<_, _>>();
        let log_map = log_list
            .into_iter()
            .map(|log| (log.token.clone(), log))
            .collect::<HashMap<_, _>>();
        let map_map = map_list
            .into_iter()
            .map(|map| (map.token.clone(), map))
            .collect::<HashMap<_, _>>();
        let sample_annotation_map = sample_annotation_list
            .into_iter()
            .map(|sample| (sample.token.clone(), sample))
            .collect::<HashMap<_, _>>();
        let sample_data_map = sample_data_list
            .into_iter()
            .map(|sample| (sample.token.clone(), sample))
            .collect::<HashMap<_, _>>();
        let sample_map = sample_list
            .into_iter()
            .map(|sample| (sample.token.clone(), sample))
            .collect::<HashMap<_, _>>();
        let scene_map = scene_list
            .into_iter()
            .map(|scene| (scene.token.clone(), scene))
            .collect::<HashMap<_, _>>();
        let sensor_map = sensor_list
            .into_iter()
            .map(|sensor| (sensor.token.clone(), sensor))
            .collect::<HashMap<_, _>>();
        let visibility_map = visibility_list
            .into_iter()
            .map(|visibility| (visibility.token.clone(), visibility))
            .collect::<HashMap<_, _>>();

        // check calibrated sensor integrity
        for (_, calibrated_sensor) in calibrated_sensor_map.iter() {
            ensure!(
                sensor_map.contains_key(&calibrated_sensor.sensor_token),
                "the token {} does not refer to any sensor",
                calibrated_sensor.sensor_token
            );
        }

        // check instance integrity
        for (instance_token, instance) in instance_map.iter() {
            ensure!(
                sample_annotation_map.contains_key(&instance.first_annotation_token),
                "the token {} does not refer to any sample annotation",
                instance.first_annotation_token
            );

            ensure!(
                sample_annotation_map.contains_key(&instance.last_annotation_token),
                "the token {} does not refer to any sample annotation",
                instance.last_annotation_token
            );

            ensure!(
                category_map.contains_key(&instance.category_token),
                "the token {} does not refer to any sample category",
                instance.category_token
            );

            let mut annotation_token = &instance.first_annotation_token;
            let mut prev_annotation_token = None;
            let mut count = 0;

            loop {
                let annotation = match sample_annotation_map.get(annotation_token) {
                    Some(annotation) => annotation,
                    None => {
                        match prev_annotation_token {
                            Some(prev) => bail!("the sample_annotation with token {} points to next token {} that does not exist", prev, annotation_token),
                            None => bail!("the instance with token {} points to first_annotation_token {} that does not exist", instance_token, annotation_token),
                        }
                    }
                };

                ensure!(
                    prev_annotation_token == annotation.prev.as_ref(),
                    "the prev field is not correct in sample annotation with token {}",
                    annotation_token
                );
                count += 1;

                prev_annotation_token = Some(annotation_token);
                annotation_token = match &annotation.next {
                    Some(next) => next,
                    None => {
                        ensure!(
                            &instance.last_annotation_token == annotation_token,
                            "the last_annotation_token is not correct in instance with token {}",
                            instance_token
                        );
                        ensure!(
                            count == instance.nbr_annotations,
                            "the nbr_annotations is not correct in instance with token {}",
                            instance_token
                        );
                        break;
                    }
                };
            }
        }

        // check map integrity
        for (_, map) in map_map.iter() {
            for token in map.log_tokens.iter() {
                ensure!(
                    log_map.contains_key(token),
                    "the token {} does not refer to any log",
                    token
                );
            }
        }

        // check scene integrity
        for (scene_token, scene) in scene_map.iter() {
            ensure!(
                log_map.contains_key(&scene.log_token),
                "the token {} does not refer to any log",
                scene.log_token
            );

            ensure!(
                sample_map.contains_key(&scene.first_sample_token),
                "the token {} does not refer to any sample",
                scene.first_sample_token
            );

            ensure!(
                sample_map.contains_key(&scene.last_sample_token),
                "the token {} does not refer to any sample",
                scene.last_sample_token
            );

            let mut prev_sample_token = None;
            let mut sample_token = &scene.first_sample_token;
            let mut count = 0;

            loop {
                let sample = match sample_map.get(sample_token) {
                    Some(sample) => sample,
                    None => {
                        match prev_sample_token {
                            Some(prev) => bail!("the sample with token {} points to a next token {} that does not exist", prev, sample_token),
                            None => bail!("the scene with token {} points to first_sample_token {} that does not exist", scene_token, sample_token),
                        }
                    }
                };
                ensure!(
                    prev_sample_token == sample.prev.as_ref(),
                    "the prev field in sample with token {} is not correct",
                    sample_token
                );
                prev_sample_token = Some(sample_token);
                count += 1;

                sample_token = match &sample.next {
                    Some(next) => next,
                    None => {
                        ensure!(
                            sample_token == &scene.last_sample_token,
                            "the last_sample_token is not correct in scene with token {}",
                            scene_token
                        );
                        ensure!(
                            count == scene.nbr_samples,
                            "the nbr_samples in scene with token {} is not correct",
                            scene_token
                        );
                        break;
                    }
                };
            }
        }

        // check sample integrity
        for (_, sample) in sample_map.iter() {
            ensure!(
                scene_map.contains_key(&sample.scene_token),
                "the token {} does not refer to any scene",
                sample.scene_token
            );

            if let Some(token) = &sample.prev {
                ensure!(
                    sample_map.contains_key(token),
                    "the token {} does not refer to any sample",
                    token
                );
            }

            if let Some(token) = &sample.next {
                ensure!(
                    sample_map.contains_key(token),
                    "the token {} does not refer to any sample",
                    token
                );
            }
        }

        // check sample annotation integrity
        for (_, sample_annotation) in sample_annotation_map.iter() {
            ensure!(
                sample_map.contains_key(&sample_annotation.sample_token),
                "the token {} does not refer to any sample",
                sample_annotation.sample_token
            );

            ensure!(
                instance_map.contains_key(&sample_annotation.instance_token),
                "the token {} does not refer to any instance",
                sample_annotation.instance_token
            );

            for token in sample_annotation.attribute_tokens.iter() {
                ensure!(
                    attribute_map.contains_key(token),
                    "the token {} does not refer to any attribute",
                    token
                );
            }

            if let Some(token) = &sample_annotation.visibility_token {
                ensure!(
                    visibility_map.contains_key(token),
                    "the token {} does not refer to any visibility",
                    token
                );
            }

            if let Some(token) = &sample_annotation.prev {
                ensure!(
                    sample_annotation_map.contains_key(token),
                    "the token {} does not refer to any sample annotation",
                    token
                );
            }

            if let Some(token) = &sample_annotation.next {
                ensure!(
                    sample_annotation_map.contains_key(token),
                    "the token {} does not refer to any sample annotation",
                    token
                );
            }
        }

        // check sample data integrity
        for (_, sample_data) in sample_data_map.iter() {
            ensure!(
                sample_map.contains_key(&sample_data.sample_token),
                "the token {} does not refer to any sample",
                sample_data.sample_token
            );

            ensure!(
                ego_pose_map.contains_key(&sample_data.ego_pose_token),
                "the token {} does not refer to any ego pose",
                sample_data.ego_pose_token
            );

            ensure!(
                calibrated_sensor_map.contains_key(&sample_data.calibrated_sensor_token),
                "the token {} does not refer to any calibrated sensor",
                sample_data.calibrated_sensor_token
            );

            if let Some(token) = &sample_data.prev {
                ensure!(
                    sample_data_map.contains_key(token),
                    "the token {} does not refer to any sample data",
                    token
                );
            }

            if let Some(token) = &sample_data.next {
                ensure!(
                    sample_data_map.contains_key(token),
                    "the token {} does not refer to any sample data",
                    token
                );
            }
        }

        // keep track of relations from samples to sample annotations
        let mut sample_to_annotation_groups = sample_annotation_map
            .iter()
            .map(|(sample_annotation_token, sample_annotation)| {
                (
                    sample_annotation.sample_token.clone(),
                    sample_annotation_token.clone(),
                )
            })
            .into_group_map();

        // keep track of relations from samples to sample data
        let mut sample_to_sample_data_groups = sample_data_map
            .iter()
            .map(|(sample_data_token, sample_data)| {
                (sample_data.sample_token.clone(), sample_data_token.clone())
            })
            .into_group_map();

        // convert some types for ease of usage
        let instance_internal_map = instance_map
            .into_iter()
            .map(|(instance_token, instance)| {
                let ret = InstanceInternal::from(instance, &sample_annotation_map)?;
                Ok((instance_token, ret))
            })
            .collect::<Fallible<HashMap<_, _>>>()?;

        let scene_internal_map = scene_map
            .into_iter()
            .map(|(scene_token, scene)| {
                let internal = SceneInternal::from(scene, &sample_map)?;
                Ok((scene_token, internal))
            })
            .collect::<Fallible<HashMap<_, _>>>()?;

        let sample_internal_map = sample_map
            .into_iter()
            .map(|(sample_token, sample)| {
                let sample_data_tokens = sample_to_sample_data_groups
                    .remove(&sample_token)
                    .ok_or(NuSceneDataError::internal_bug())?;
                let annotation_tokens = sample_to_annotation_groups
                    .remove(&sample_token)
                    .ok_or(NuSceneDataError::internal_bug())?;
                let internal = SampleInternal::from(sample, annotation_tokens, sample_data_tokens);
                Ok((sample_token, internal))
            })
            .collect::<Fallible<HashMap<_, _>>>()?;

        // sort scenes by timestamp
        let sorted_scene_tokens = {
            let mut sorted_pairs = scene_internal_map
                .iter()
                .map(|(scene_token, scene)| {
                    let timestamp = scene
                        .sample_tokens
                        .iter()
                        .map(|sample_token| {
                            let sample = sample_internal_map
                                .get(&sample_token)
                                .ok_or(NuSceneDataError::internal_bug())?;
                            Ok(sample.timestamp)
                        })
                        .collect::<Fallible<Vec<_>>>()?
                        .into_iter()
                        .min()
                        .ok_or(NuSceneDataError::internal_bug())?;

                    Ok((scene_token, timestamp))
                })
                .collect::<Fallible<Vec<_>>>()?;
            sorted_pairs.sort_by_cached_key(|(_, timestamp)| timestamp.clone());

            sorted_pairs
                .into_iter()
                .map(|(token, _)| token.clone())
                .collect::<Vec<_>>()
        };

        // sort samples by timestamp
        let sorted_sample_tokens = {
            let mut sorted_pairs = sample_internal_map
                .iter()
                .map(|(sample_token, sample)| (sample_token, sample.timestamp))
                .collect::<Vec<_>>();
            sorted_pairs.sort_by_cached_key(|(_, timestamp)| timestamp.clone());

            sorted_pairs
                .into_iter()
                .map(|(token, _)| token.clone())
                .collect::<Vec<_>>()
        };

        // construct result
        let ret = Self {
            name: name.as_ref().to_owned(),
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
            sorted_scene_tokens,
            sorted_sample_tokens,
        };

        Ok(ret)
    }

    pub fn scene_iter<'a>(&'a self) -> Iter<'a, LongToken, SceneInternal> {
        Iter {
            dataset: self,
            tokens_iter: self.sorted_scene_tokens.iter(),
            _phantom: PhantomData,
        }
    }

    pub fn sample_iter<'a>(&'a self) -> Iter<'a, LongToken, SampleInternal> {
        Iter {
            dataset: self,
            tokens_iter: self.sorted_sample_tokens.iter(),
            _phantom: PhantomData,
        }
    }
}

fn load_json<'de, T, P>(path: P) -> Fallible<T>
where
    P: AsRef<Path>,
    T: DeserializeOwned,
{
    let reader = BufReader::new(File::open(path.as_ref())?);
    let value = serde_json::from_reader(reader)?;
    Ok(value)
}
