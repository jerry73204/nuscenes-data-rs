use crate::{
    dataset::{Dataset, DatasetInner},
    error::{Error, Result},
    parsed::{InstanceInternal, SampleInternal, SceneInternal},
    serializable::{
        Attribute, CalibratedSensor, Category, EgoPose, Instance, Log, Map, Sample,
        SampleAnnotation, SampleData, Scene, Sensor, Token, Visibility, VisibilityToken,
    },
    utils::WithToken,
};
use chrono::NaiveDateTime;
use itertools::Itertools;
use serde::Deserialize;
use std::{collections::HashMap, fs::File, io::BufReader, path::Path};

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

        // Load .json files in parallel
        let mut attribute_map: Result<HashMap<Token, Attribute>> = Ok(Default::default());
        let mut calibrated_sensor_map: Result<HashMap<Token, CalibratedSensor>> =
            Ok(Default::default());
        let mut category_map: Result<HashMap<Token, Category>> = Ok(Default::default());
        let mut ego_pose_map: Result<HashMap<Token, EgoPose>> = Ok(Default::default());
        let mut instance_map: Result<HashMap<Token, Instance>> = Ok(Default::default());
        let mut log_map: Result<HashMap<Token, Log>> = Ok(Default::default());
        let mut map_map: Result<HashMap<Token, Map>> = Ok(Default::default());
        let mut sample_annotation_map: Result<HashMap<Token, SampleAnnotation>> =
            Ok(Default::default());
        let mut sample_data_map: Result<HashMap<Token, SampleData>> = Ok(Default::default());
        let mut sample_map: Result<HashMap<Token, Sample>> = Ok(Default::default());
        let mut scene_map: Result<HashMap<Token, Scene>> = Ok(Default::default());
        let mut sensor_map: Result<HashMap<Token, Sensor>> = Ok(Default::default());
        let mut visibility_map: Result<HashMap<VisibilityToken, Visibility>> =
            Ok(Default::default());

        rayon::scope(|scope| {
            scope.spawn(|_| {
                attribute_map = load_map(meta_dir.join("attribute.json"));
            });
            scope.spawn(|_| {
                calibrated_sensor_map = load_map(meta_dir.join("sensor.json"));
            });
            scope.spawn(|_| {
                category_map = load_map(meta_dir.join("category.json"));
            });
            scope.spawn(|_| {
                ego_pose_map = load_map(meta_dir.join("ego_pose.json"));
            });
            scope.spawn(|_| {
                instance_map = load_map(meta_dir.join("instance.json"));
            });
            scope.spawn(|_| {
                log_map = load_map(meta_dir.join("log.json"));
            });
            scope.spawn(|_| {
                map_map = load_map(meta_dir.join("map.json"));
            });
            scope.spawn(|_| {
                sample_annotation_map = load_map(meta_dir.join("annotation.json"));
            });
            scope.spawn(|_| {
                sample_data_map = load_map(meta_dir.join("sample_data.json"));
            });
            scope.spawn(|_| {
                sample_map = load_map(meta_dir.join("sample.json"));
            });
            scope.spawn(|_| {
                scene_map = load_map(meta_dir.join("scene.json"));
            });
            scope.spawn(|_| {
                sensor_map = load_map(meta_dir.join("sensor.json"));
            });
            scope.spawn(|_| {
                visibility_map = (|| {
                    let vec: Vec<Visibility> = load_json(meta_dir.join("visibility.json"))?;
                    let map: HashMap<VisibilityToken, Visibility> =
                        vec.into_iter().map(|item| (item.token, item)).collect();
                    Ok(map)
                })();
            });
        });

        let attribute_map = attribute_map?;
        let calibrated_sensor_map = calibrated_sensor_map?;
        let category_map = category_map?;
        let ego_pose_map = ego_pose_map?;
        let instance_map = instance_map?;
        let log_map = log_map?;
        let map_map = map_map?;
        let sample_annotation_map = sample_annotation_map?;
        let sample_data_map = sample_data_map?;
        let sample_map = sample_map?;
        let scene_map = scene_map?;
        let sensor_map = sensor_map?;
        let visibility_map = visibility_map?;

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
        let inner = DatasetInner {
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

        Ok(Dataset::from_inner(inner))
    }
}

impl Default for DatasetLoader {
    fn default() -> Self {
        Self { check: false }
    }
}

fn load_map<T, P>(path: P) -> Result<HashMap<Token, T>>
where
    P: AsRef<Path>,
    T: for<'a> Deserialize<'a> + WithToken,
{
    let vec: Vec<T> = load_json(path)?;
    let map = vec.into_iter().map(|item| (item.token(), item)).collect();
    Ok(map)
}

fn load_json<T, P>(path: P) -> Result<T>
where
    P: AsRef<Path>,
    T: for<'a> Deserialize<'a>,
{
    let reader = BufReader::new(File::open(path.as_ref())?);
    let value = serde_json::from_reader(reader).map_err(|err| {
        let msg = format!("failed to load file {}: {:?}", path.as_ref().display(), err);
        Error::CorruptedDataset(msg)
    })?;
    Ok(value)
}
