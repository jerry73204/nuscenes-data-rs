use crate::{
    dataset::{Dataset, DatasetInner},
    error::{Error, Result},
    parsed::{InstanceInternal, SampleInternal, SceneInternal},
    serializable::{
        Attribute, CalibratedSensor, Category, EgoPose, Instance, Log, Map, Sample,
        SampleAnnotation, SampleData, Scene, Sensor, Token, Visibility, VisibilityToken,
    },
    utils::{ParallelIteratorExt, WithToken},
};
use chrono::NaiveDateTime;
use itertools::Itertools;
use rayon::prelude::*;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};

macro_rules! bail_corrupted {
    ($($arg:expr),*) => {
        {
            let msg = format!($($arg),*);
            return Err(Error::CorruptedDataset(msg));
        }
    };
}

macro_rules! ensure_corrupted {
    ($cond:expr, $($arg:expr),*) => {
        {
            if !$cond {
                bail_corrupted!($($arg),*);
            }
        }
    };
}

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

        // Load .json files
        let load_json = load_json_files(&meta_dir)?;

        // Check the data integrity if requested
        if check {
            check_loaded_json(&load_json)?;
        }

        // Index internal associated records
        let inner = index_records(version.to_string(), dataset_dir.to_owned(), load_json)?;

        Ok(Dataset::from_inner(inner))
    }
}

impl Default for DatasetLoader {
    fn default() -> Self {
        Self { check: true }
    }
}

struct LoadJson {
    pub attribute_map: HashMap<Token, Attribute>,
    pub calibrated_sensor_map: HashMap<Token, CalibratedSensor>,
    pub category_map: HashMap<Token, Category>,
    pub ego_pose_map: HashMap<Token, EgoPose>,
    pub instance_map: HashMap<Token, Instance>,
    pub log_map: HashMap<Token, Log>,
    pub map_map: HashMap<Token, Map>,
    pub scene_map: HashMap<Token, Scene>,
    pub sample_map: HashMap<Token, Sample>,
    pub sample_annotation_map: HashMap<Token, SampleAnnotation>,
    pub sample_data_map: HashMap<Token, SampleData>,
    pub sensor_map: HashMap<Token, Sensor>,
    pub visibility_map: HashMap<VisibilityToken, Visibility>,
}

fn load_json_files(dir: &Path) -> Result<LoadJson> {
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
    let mut visibility_map: Result<HashMap<VisibilityToken, Visibility>> = Ok(Default::default());

    rayon::scope(|scope| {
        scope.spawn(|_| {
            attribute_map = load_map(dir.join("attribute.json"));
        });
        scope.spawn(|_| {
            calibrated_sensor_map = load_map(dir.join("calibrated_sensor.json"));
        });
        scope.spawn(|_| {
            category_map = load_map(dir.join("category.json"));
        });
        scope.spawn(|_| {
            ego_pose_map = load_map(dir.join("ego_pose.json"));
        });
        scope.spawn(|_| {
            instance_map = load_map(dir.join("instance.json"));
        });
        scope.spawn(|_| {
            log_map = load_map(dir.join("log.json"));
        });
        scope.spawn(|_| {
            map_map = load_map(dir.join("map.json"));
        });
        scope.spawn(|_| {
            sample_annotation_map = load_map(dir.join("sample_annotation.json"));
        });
        scope.spawn(|_| {
            sample_data_map = load_map(dir.join("sample_data.json"));
        });
        scope.spawn(|_| {
            sample_map = load_map(dir.join("sample.json"));
        });
        scope.spawn(|_| {
            scene_map = load_map(dir.join("scene.json"));
        });
        scope.spawn(|_| {
            sensor_map = load_map(dir.join("sensor.json"));
        });
        scope.spawn(|_| {
            visibility_map = (|| {
                let vec: Vec<Visibility> = load_json(dir.join("visibility.json"))?;
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

    Ok(LoadJson {
        attribute_map,
        calibrated_sensor_map,
        category_map,
        ego_pose_map,
        instance_map,
        log_map,
        map_map,
        scene_map,
        sample_map,
        sample_annotation_map,
        sample_data_map,
        sensor_map,
        visibility_map,
    })
}

fn check_loaded_json(load_json: &LoadJson) -> Result<()> {
    let LoadJson {
        attribute_map,
        calibrated_sensor_map,
        category_map,
        ego_pose_map,
        instance_map,
        log_map,
        map_map,
        scene_map,
        sample_map,
        sample_annotation_map,
        sample_data_map,
        sensor_map,
        visibility_map,
    } = load_json;

    // check calibrated sensor integrity
    calibrated_sensor_map
        .par_iter()
        .try_for_each(|(_, calibrated_sensor)| {
            ensure_corrupted!(
                sensor_map.contains_key(&calibrated_sensor.sensor_token),
                "the token {} does not refer to any sensor",
                calibrated_sensor.sensor_token
            );
            Ok(())
        })?;

    // check sample annotation integrity
    sample_annotation_map
        .par_iter()
        .try_for_each(|(_, sample_annotation)| {
            ensure_corrupted!(
                sample_map.contains_key(&sample_annotation.sample_token),
                "the token {} does not refer to any sample",
                sample_annotation.sample_token
            );

            ensure_corrupted!(
                instance_map.contains_key(&sample_annotation.instance_token),
                "the token {} does not refer to any instance",
                sample_annotation.instance_token
            );

            sample_annotation
                .attribute_tokens
                .par_iter()
                .try_for_each(|token| {
                    ensure_corrupted!(
                        attribute_map.contains_key(token),
                        "the token {} does not refer to any attribute",
                        token
                    );
                    Ok(())
                })?;

            if let Some(token) = &sample_annotation.visibility_token {
                ensure_corrupted!(
                    visibility_map.contains_key(token),
                    "the token {} does not refer to any visibility",
                    token
                );
            }

            if let Some(token) = &sample_annotation.prev {
                ensure_corrupted!(
                    sample_annotation_map.contains_key(token),
                    "the token {} does not refer to any sample annotation",
                    token
                );
            }

            if let Some(token) = &sample_annotation.next {
                ensure_corrupted!(
                    sample_annotation_map.contains_key(token),
                    "the token {} does not refer to any sample annotation",
                    token
                );
            }

            Ok(())
        })?;

    // Check sample_annotation.{next,prev} fields integrity
    {
        let mut prev_edges: Vec<_> = sample_annotation_map
            .par_iter()
            .filter_map(|(&curr_token, annotation)| Some((annotation.prev?, curr_token)))
            .collect();
        prev_edges.par_sort_unstable();

        let mut next_edges: Vec<_> = sample_annotation_map
            .par_iter()
            .filter_map(|(&curr_token, annotation)| Some((curr_token, annotation.next?)))
            .collect();
        next_edges.par_sort_unstable();

        ensure_corrupted!(
            prev_edges.len() == next_edges.len(),
            "The number of non-null sample_annotation.next does not match the number of sample_annotation.prev"
        );

        prev_edges
            .par_iter()
            .zip(next_edges.par_iter())
            .try_for_each(|(e1, e2)| {
                ensure_corrupted!(
                    e1 == e2,
                    "The prev and next fields of sample_annotatoin.json are corrupted"
                );
                Ok(())
            })?;
    }

    // check instance integrity
    instance_map.par_iter().try_for_each(|(_, instance)| {
        ensure_corrupted!(
            sample_annotation_map.contains_key(&instance.first_annotation_token),
            "the token {} does not refer to any sample annotation",
            instance.first_annotation_token
        );

        ensure_corrupted!(
            sample_annotation_map.contains_key(&instance.last_annotation_token),
            "the token {} does not refer to any sample annotation",
            instance.last_annotation_token
        );

        ensure_corrupted!(
            category_map.contains_key(&instance.category_token),
            "the token {} does not refer to any sample category",
            instance.category_token
        );

        Ok(())
    })?;

    // Check instance.first_annotation_token
    {
        let mut lhs: Vec<_> = sample_annotation_map
            .par_iter()
            .filter_map(|(&token, annotation)| annotation.prev.is_none().then_some(token))
            .collect();
        let mut rhs: Vec<_> = instance_map
            .par_iter()
            .map(|(_, instance)| instance.first_annotation_token)
            .collect();

        lhs.par_sort_unstable();
        rhs.par_sort_unstable();
        lhs.par_iter()
            .zip(rhs.par_iter())
            .try_for_each(|(lhs, rhs)| {
                ensure_corrupted!(lhs == rhs, "instance.first_annotation_token is corrupted");
                Ok(())
            })?;
    }

    // Check instance.last_annotation_token
    {
        let mut lhs: Vec<_> = sample_annotation_map
            .par_iter()
            .filter_map(|(&token, annotation)| annotation.next.is_none().then_some(token))
            .collect();
        let mut rhs: Vec<_> = instance_map
            .par_iter()
            .map(|(_, instance)| instance.last_annotation_token)
            .collect();

        lhs.par_sort_unstable();
        rhs.par_sort_unstable();

        lhs.par_iter()
            .zip(rhs.par_iter())
            .try_for_each(|(lhs, rhs)| {
                ensure_corrupted!(lhs == rhs, "instance.first_annotation_token is corrupted");
                Ok(())
            })?;
    }

    // Check instance.nbr_annotations
    // TODO: implement the parallel algorithm to count the length of chained annotations
    // {
    //     for (instance_token, instance) in instance_map {
    //         let mut annotation_token = &instance.first_annotation_token;
    //         let mut prev_annotation_token = None;
    //         let mut count = 0;

    //         loop {
    //             let annotation = match sample_annotation_map.get(annotation_token) {
    //                 Some(annotation) => annotation,
    //                 None => {
    //                     match prev_annotation_token {
    //                         Some(prev) => bail_corrupted!("the sample_annotation with token {prev} points to next token {annotation_token} that does not exist"),
    //                         None => bail_corrupted!("the instance with token {instance_token} points to first_annotation_token {annotation_token} that does not exist"),
    //                     }
    //                 }
    //             };

    //             ensure_corrupted!(
    //                 prev_annotation_token == annotation.prev.as_ref(),
    //                 "the prev field is not correct in sample annotation with token {}",
    //                 annotation_token
    //             );

    //             count += 1;

    //             prev_annotation_token = Some(annotation_token);
    //             annotation_token = match &annotation.next {
    //                 Some(next) => next,
    //                 None => {
    //                     ensure_corrupted!(
    //                         &instance.last_annotation_token == annotation_token,
    //                         "the last_annotation_token is not correct in instance with token {}",
    //                         instance_token
    //                     );
    //                     ensure_corrupted!(
    //                         count == instance.nbr_annotations,
    //                         "the nbr_annotations is not correct in instance with token {}",
    //                         instance_token
    //                     );
    //                     break;
    //                 }
    //             };
    //         }
    //     }
    // }

    // check map integrity
    map_map
        .par_iter()
        .flat_map(|(map_token, map)| {
            map.log_tokens
                .par_iter()
                .map(move |log_token| (map_token, log_token))
        })
        .try_for_each(|(map_token, log_token)| {
            ensure_corrupted!(
                log_map.contains_key(log_token),
                "in the map {map_token}, the log_token {log_token} does not refer to any valid log"
            );
            Ok(())
        })?;

    // check sample integrity
    sample_map.par_iter().try_for_each(|(_, sample)| {
        ensure_corrupted!(
            scene_map.contains_key(&sample.scene_token),
            "the token {} does not refer to any scene",
            sample.scene_token
        );

        if let Some(token) = &sample.prev {
            ensure_corrupted!(
                sample_map.contains_key(token),
                "the token {} does not refer to any sample",
                token
            );
        }

        if let Some(token) = &sample.next {
            ensure_corrupted!(
                sample_map.contains_key(token),
                "the token {} does not refer to any sample",
                token
            );
        }

        Ok(())
    })?;

    // Check sample.{next,prev} fields integrity
    {
        let mut prev_edges: Vec<_> = sample_map
            .par_iter()
            .filter_map(|(&curr_token, sample)| Some((sample.prev?, curr_token)))
            .collect();
        prev_edges.par_sort_unstable();

        let mut next_edges: Vec<_> = sample_map
            .par_iter()
            .filter_map(|(&curr_token, sample)| Some((curr_token, sample.next?)))
            .collect();
        next_edges.par_sort_unstable();

        ensure_corrupted!(
            prev_edges.len() == next_edges.len(),
            "The number of non-null sample.next does not match the number of sample.prev"
        );

        prev_edges
            .par_iter()
            .zip(next_edges.par_iter())
            .try_for_each(|(e1, e2)| {
                ensure_corrupted!(
                    e1 == e2,
                    "The prev and next fields of sample.json are corrupted"
                );
                Ok(())
            })?;
    }

    // check scene integrity
    scene_map.par_iter().try_for_each(|(_, scene)| {
        ensure_corrupted!(
            log_map.contains_key(&scene.log_token),
            "the token {} does not refer to any log",
            scene.log_token
        );

        ensure_corrupted!(
            sample_map.contains_key(&scene.first_sample_token),
            "the token {} does not refer to any sample",
            scene.first_sample_token
        );

        ensure_corrupted!(
            sample_map.contains_key(&scene.last_sample_token),
            "the token {} does not refer to any sample",
            scene.last_sample_token
        );

        Ok(())
    })?;

    // Check scene.first_sample_token
    {
        let mut lhs: Vec<_> = sample_map
            .par_iter()
            .filter_map(|(&token, sample)| sample.prev.is_none().then_some(token))
            .collect();
        let mut rhs: Vec<_> = scene_map
            .par_iter()
            .map(|(_, scene)| scene.first_sample_token)
            .collect();

        lhs.par_sort_unstable();
        rhs.par_sort_unstable();
        lhs.par_iter()
            .zip(rhs.par_iter())
            .try_for_each(|(lhs, rhs)| {
                ensure_corrupted!(lhs == rhs, "scene.first_sample_token is corrupted");
                Ok(())
            })?;
    }

    // Check scene.last_sample_token
    {
        let mut lhs: Vec<_> = sample_map
            .par_iter()
            .filter_map(|(&token, sample)| sample.next.is_none().then_some(token))
            .collect();
        let mut rhs: Vec<_> = scene_map
            .par_iter()
            .map(|(_, scene)| scene.last_sample_token)
            .collect();

        lhs.par_sort_unstable();
        rhs.par_sort_unstable();

        lhs.par_iter()
            .zip(rhs.par_iter())
            .try_for_each(|(lhs, rhs)| {
                ensure_corrupted!(lhs == rhs, "scene.first_sample_token is corrupted");
                Ok(())
            })?;
    }

    // Check scene.nbr_samples
    // TODO: implement a parallel algorithm to check scene.nbr_samples
    // for (scene_token, scene) in scene_map {
    //     let mut prev_sample_token = None;
    //     let mut sample_token = &scene.first_sample_token;
    //     let mut count = 0;

    //     loop {
    //         let sample = match sample_map.get(sample_token) {
    //                 Some(sample) => sample,
    //                 None => {
    //                     match prev_sample_token {
    //                         Some(prev) => bail_corrupted!("the sample with token {} points to a next token {} that does not exist", prev, sample_token),
    //                         None => bail_corrupted!("the scene with token {} points to first_sample_token {} that does not exist", scene_token, sample_token),
    //                     }
    //                 }
    //         };

    //         ensure_corrupted!(
    //             prev_sample_token == sample.prev.as_ref(),
    //             "the prev field in sample with token {} is not correct",
    //             sample_token
    //         );

    //         prev_sample_token = Some(sample_token);
    //         count += 1;

    //         sample_token = match &sample.next {
    //             Some(next) => next,
    //             None => {
    //                 ensure_corrupted!(
    //                     sample_token == &scene.last_sample_token,
    //                     "the last_sample_token is not correct in scene with token {}",
    //                     scene_token
    //                 );
    //                 ensure_corrupted!(
    //                     count == scene.nbr_samples,
    //                     "the nbr_samples in scene with token {} is not correct",
    //                     scene_token
    //                 );
    //                 break;
    //             }
    //         };
    //     }
    // }

    // check sample data integrity
    sample_data_map
        .par_iter()
        .try_for_each(|(_, sample_data)| {
            ensure_corrupted!(
                sample_map.contains_key(&sample_data.sample_token),
                "the token {} does not refer to any sample",
                sample_data.sample_token
            );

            ensure_corrupted!(
                ego_pose_map.contains_key(&sample_data.ego_pose_token),
                "the token {} does not refer to any ego pose",
                sample_data.ego_pose_token
            );

            ensure_corrupted!(
                calibrated_sensor_map.contains_key(&sample_data.calibrated_sensor_token),
                "the token {} does not refer to any calibrated sensor",
                sample_data.calibrated_sensor_token
            );

            if let Some(token) = &sample_data.prev {
                ensure_corrupted!(
                    sample_data_map.contains_key(token),
                    "the token {} does not refer to any sample data",
                    token
                );
            }

            if let Some(token) = &sample_data.next {
                ensure_corrupted!(
                    sample_data_map.contains_key(token),
                    "the token {} does not refer to any sample data",
                    token
                );
            }

            Ok(())
        })?;

    // Check sample_annotation.{next,prev} fields integrity
    {
        let mut prev_edges: Vec<_> = sample_data_map
            .par_iter()
            .filter_map(|(&curr_token, data)| Some((data.prev?, curr_token)))
            .collect();
        prev_edges.par_sort_unstable();

        let mut next_edges: Vec<_> = sample_data_map
            .par_iter()
            .filter_map(|(&curr_token, data)| Some((curr_token, data.next?)))
            .collect();
        next_edges.par_sort_unstable();

        ensure_corrupted!(
            prev_edges.len() == next_edges.len(),
            "The number of non-null sample_data.next does not match the number of sample_data.prev"
        );

        prev_edges
            .par_iter()
            .zip(next_edges.par_iter())
            .try_for_each(|(e1, e2)| {
                ensure_corrupted!(
                    e1 == e2,
                    "The prev and next fields of sample_annotatoin.json are corrupted"
                );
                Ok(())
            })?;
    }

    Ok(())
}

fn index_records(
    version: String,
    dataset_dir: PathBuf,
    load_json: LoadJson,
) -> Result<DatasetInner> {
    let LoadJson {
        attribute_map,
        calibrated_sensor_map,
        category_map,
        ego_pose_map,
        instance_map,
        log_map,
        map_map,
        scene_map,
        sample_map,
        sample_annotation_map,
        sample_data_map,
        sensor_map,
        visibility_map,
    } = load_json;

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
        .into_par_iter()
        .map(|(instance_token, instance)| -> Result<_> {
            let ret = InstanceInternal::from(instance, &sample_annotation_map)?;
            Ok((instance_token, ret))
        })
        .par_try_collect()?;

    let scene_internal_map: HashMap<_, _> = scene_map
        .into_par_iter()
        .map(|(scene_token, scene)| -> Result<_> {
            let internal = SceneInternal::from(scene, &sample_map)?;
            Ok((scene_token, internal))
        })
        .par_try_collect()?;

    let sample_internal_map: HashMap<_, _> = sample_map
        .into_iter()
        .map(|(sample_token, sample)| -> Result<_> {
            let sample_data_tokens = sample_to_sample_data_groups
                .remove(&sample_token)
                .unwrap_or_default();
            let annotation_tokens = sample_to_annotation_groups
                .remove(&sample_token)
                .unwrap_or_default();
            let internal = SampleInternal::from(sample, annotation_tokens, sample_data_tokens);
            Ok((sample_token, internal))
        })
        .try_collect()?;

    // sort ego_pose by timestamp
    let sorted_ego_pose_tokens: Vec<_> = {
        let mut sorted_pairs: Vec<(&Token, NaiveDateTime)> = ego_pose_map
            .par_iter()
            .map(|(sample_token, sample)| (sample_token, sample.timestamp))
            .collect();
        sorted_pairs.par_sort_unstable_by_key(|(_, timestamp)| *timestamp);
        sorted_pairs
            .into_par_iter()
            .map(|(token, _)| *token)
            .collect()
    };

    // sort samples by timestamp
    let sorted_sample_tokens: Vec<_> = {
        let mut sorted_pairs: Vec<(&Token, NaiveDateTime)> = sample_internal_map
            .par_iter()
            .map(|(sample_token, sample)| (sample_token, sample.timestamp))
            .collect();
        sorted_pairs.par_sort_unstable_by_key(|(_, timestamp)| *timestamp);
        sorted_pairs
            .into_par_iter()
            .map(|(token, _)| *token)
            .collect()
    };

    // sort sample data by timestamp
    let sorted_sample_data_tokens: Vec<_> = {
        let mut sorted_pairs: Vec<(&Token, NaiveDateTime)> = sample_data_map
            .par_iter()
            .map(|(sample_token, sample)| (sample_token, sample.timestamp))
            .collect();
        sorted_pairs.par_sort_unstable_by_key(|(_, timestamp)| *timestamp);
        sorted_pairs
            .into_par_iter()
            .map(|(token, _)| *token)
            .collect()
    };

    // sort scenes by timestamp
    let sorted_scene_tokens: Vec<_> = {
        let mut sorted_pairs: Vec<_> = scene_internal_map
            .par_iter()
            .map(|(scene_token, scene)| {
                let timestamps: Vec<NaiveDateTime> = scene
                    .sample_tokens
                    .par_iter()
                    .map(|sample_token| {
                        let sample = sample_internal_map
                            .get(sample_token)
                            .expect("internal error: invalid sample_token");
                        sample.timestamp
                    })
                    .collect();

                let timestamp = timestamps
                    .into_par_iter()
                    .min()
                    .expect("scene.sample_tokens must not be empty");

                (scene_token, timestamp)
            })
            .collect();
        sorted_pairs.par_sort_unstable_by_key(|(_, timestamp)| *timestamp);

        sorted_pairs
            .into_par_iter()
            .map(|(token, _)| *token)
            .collect()
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

    Ok(inner)
}

fn load_map<T, P>(path: P) -> Result<HashMap<Token, T>>
where
    P: AsRef<Path>,
    T: for<'a> Deserialize<'a> + WithToken + Send,
    Vec<T>: rayon::iter::IntoParallelIterator<Item = T>,
{
    let vec: Vec<T> = load_json(path)?;
    let map = vec
        .into_par_iter()
        .map(|item| (item.token(), item))
        .collect();
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
