#![feature(backtrace)]

pub mod meta;

use crate::meta::{
    Attribute, CalibratedSensor, Category, EgoPose, Instance, Log, Map, Sample, SampleAnnotation,
    SampleData, Scene, Sensor, Visibility,
};
use failure::{ensure, Fallible};
use serde::de::DeserializeOwned;
use std::{
    collections::HashSet,
    fs::File,
    io::{prelude::*, BufReader},
    path::{Path, PathBuf},
};

pub fn load<S, P>(name: S, dir: P) -> Fallible<()>
where
    S: AsRef<str>,
    P: AsRef<Path>,
{
    let meta_dir = dir.as_ref().join(name.as_ref());
    load_meta(meta_dir)?;
    Ok(())
}

fn load_meta<P>(dir: P) -> Fallible<()>
where
    P: AsRef<Path>,
{
    let dir_ref = dir.as_ref();
    let attribute_list: Vec<Attribute> = {
        let attribute_path = dir_ref.join("attribute.json");
        load_json(attribute_path)?
    };
    let calibrated_sensor_list: Vec<CalibratedSensor> = {
        let calibrated_sensor_path = dir_ref.join("calibrated_sensor.json");
        load_json(calibrated_sensor_path)?
    };
    let category_list: Vec<Category> = {
        let category_path = dir_ref.join("category.json");
        load_json(category_path)?
    };
    let ego_pose_list: Vec<EgoPose> = {
        let ego_pose_path = dir_ref.join("ego_pose.json");
        load_json(ego_pose_path)?
    };
    let instance_list: Vec<Instance> = {
        let instance_path = dir_ref.join("instance.json");
        load_json(instance_path)?
    };
    let log_list: Vec<Log> = {
        let log_path = dir_ref.join("log.json");
        load_json(log_path)?
    };
    let map_list: Vec<Map> = {
        let map_path = dir_ref.join("map.json");
        load_json(map_path)?
    };
    let sample_annotation_list: Vec<SampleAnnotation> = {
        let sample_annotation_path = dir_ref.join("sample_annotation.json");
        load_json(sample_annotation_path)?
    };
    let sample_data_list: Vec<SampleData> = {
        let sample_data_path = dir_ref.join("sample_data.json");
        load_json(sample_data_path)?
    };
    let sample_list: Vec<Sample> = {
        let sample_path = dir_ref.join("sample.json");
        load_json(sample_path)?
    };
    let scene_list: Vec<Scene> = {
        let scene_path = dir_ref.join("scene.json");
        load_json(scene_path)?
    };
    let sensor_list: Vec<Sensor> = {
        let sensor_path = dir_ref.join("sensor.json");
        load_json(sensor_path)?
    };
    let visibility_list: Vec<Visibility> = {
        let visibility_path = dir_ref.join("visibility.json");
        load_json(visibility_path)?
    };

    {
        let sample_tokens = sample_list
            .iter()
            .map(|sample| &sample.token)
            .collect::<HashSet<_>>();

        for sample in sample_list.iter() {
            if let Some(token) = &sample.prev {
                ensure!(
                    sample_tokens.contains(token),
                    "the token {} does not refer to any sample",
                    token
                );
            }

            if let Some(token) = &sample.next {
                ensure!(
                    sample_tokens.contains(token),
                    "the token {} does not refer to any sample",
                    token
                );
            }
        }
    }

    {
        let sample_data_tokens = sample_data_list
            .iter()
            .map(|sample| &sample.token)
            .collect::<HashSet<_>>();

        for sample_data in sample_data_list.iter() {
            if let Some(token) = &sample_data.prev {
                ensure!(
                    sample_data_tokens.contains(token),
                    "the token {} does not refer to any sample data",
                    token
                );
            }

            if let Some(token) = &sample_data.next {
                ensure!(
                    sample_data_tokens.contains(token),
                    "the token {} does not refer to any sample data",
                    token
                );
            }
        }
    }

    {
        let sample_annotation_tokens = sample_annotation_list
            .iter()
            .map(|sample| &sample.token)
            .collect::<HashSet<_>>();

        for sample_annotation in sample_annotation_list.iter() {
            if let Some(token) = &sample_annotation.prev {
                ensure!(
                    sample_annotation_tokens.contains(token),
                    "the token {} does not refer to any sample annotation",
                    token
                );
            }

            if let Some(token) = &sample_annotation.next {
                ensure!(
                    sample_annotation_tokens.contains(token),
                    "the token {} does not refer to any sample annotation",
                    token
                );
            }
        }
    }

    Ok(())
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
