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

    let log_tokens = log_list
        .iter()
        .map(|log| &log.token)
        .collect::<HashSet<_>>();

    let instance_tokens = instance_list
        .iter()
        .map(|instance| &instance.token)
        .collect::<HashSet<_>>();

    let attribute_tokens = attribute_list
        .iter()
        .map(|attribute| &attribute.token)
        .collect::<HashSet<_>>();

    let visibility_tokens = visibility_list
        .iter()
        .map(|visibility| &visibility.token)
        .collect::<HashSet<_>>();

    let scene_tokens = scene_list
        .iter()
        .map(|scene| &scene.token)
        .collect::<HashSet<_>>();

    let category_tokens = category_list
        .iter()
        .map(|category| &category.token)
        .collect::<HashSet<_>>();

    let ego_pose_tokens = ego_pose_list
        .iter()
        .map(|ego_pos| &ego_pos.token)
        .collect::<HashSet<_>>();

    let calibrated_sensor_tokens = calibrated_sensor_list
        .iter()
        .map(|calibrated_sensor| &calibrated_sensor.token)
        .collect::<HashSet<_>>();

    let sensor_tokens = sensor_list
        .iter()
        .map(|sensor| &sensor.token)
        .collect::<HashSet<_>>();

    let sample_tokens = sample_list
        .iter()
        .map(|sample| &sample.token)
        .collect::<HashSet<_>>();

    let sample_data_tokens = sample_data_list
        .iter()
        .map(|sample| &sample.token)
        .collect::<HashSet<_>>();

    let sample_annotation_tokens = sample_annotation_list
        .iter()
        .map(|sample| &sample.token)
        .collect::<HashSet<_>>();

    // check calibrated sensor integrity
    for calibrated_sensor in calibrated_sensor_list.iter() {
        ensure!(
            sensor_tokens.contains(&calibrated_sensor.sensor_token),
            "the token {} does not refer to any sensor",
            calibrated_sensor.sensor_token
        );
    }

    // check scene integrity
    for scene in scene_list.iter() {
        ensure!(
            log_tokens.contains(&scene.log_token),
            "the token {} does not refer to any log",
            scene.log_token
        );

        ensure!(
            sample_tokens.contains(&scene.first_sample_token),
            "the token {} does not refer to any sample",
            scene.first_sample_token
        );

        ensure!(
            sample_tokens.contains(&scene.last_sample_token),
            "the token {} does not refer to any sample",
            scene.last_sample_token
        );
    }

    // check map integrity
    for map in map_list.iter() {
        for token in map.log_tokens.iter() {
            ensure!(
                log_tokens.contains(token),
                "the token {} does not refer to any log",
                token
            );
        }
    }

    // check instance integrity
    for instance in instance_list.iter() {
        ensure!(
            sample_annotation_tokens.contains(&instance.first_annotation_token),
            "the token {} does not refer to any sample annotation",
            instance.first_annotation_token
        );

        ensure!(
            sample_annotation_tokens.contains(&instance.last_annotation_token),
            "the token {} does not refer to any sample annotation",
            instance.last_annotation_token
        );

        ensure!(
            category_tokens.contains(&instance.category_token),
            "the token {} does not refer to any sample category",
            instance.category_token
        );
    }

    // check sample integrity
    for sample in sample_list.iter() {
        ensure!(
            scene_tokens.contains(&sample.scene_token),
            "the token {} does not refer to any scene",
            sample.scene_token
        );

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

    // check sample data integrity
    for sample_data in sample_data_list.iter() {
        ensure!(
            sample_tokens.contains(&sample_data.sample_token),
            "the token {} does not refer to any sample",
            sample_data.sample_token
        );

        ensure!(
            ego_pose_tokens.contains(&sample_data.ego_pose_token),
            "the token {} does not refer to any ego pose",
            sample_data.ego_pose_token
        );

        ensure!(
            calibrated_sensor_tokens.contains(&sample_data.calibrated_sensor_token),
            "the token {} does not refer to any calibrated sensor",
            sample_data.calibrated_sensor_token
        );

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

    // check sample annotation integrity
    for sample_annotation in sample_annotation_list.iter() {
        ensure!(
            sample_tokens.contains(&sample_annotation.sample_token),
            "the token {} does not refer to any sample",
            sample_annotation.sample_token
        );

        ensure!(
            instance_tokens.contains(&sample_annotation.instance_token),
            "the token {} does not refer to any instance",
            sample_annotation.instance_token
        );

        for token in sample_annotation.attribute_tokens.iter() {
            ensure!(
                attribute_tokens.contains(token),
                "the token {} does not refer to any attribute",
                token
            );
        }

        if let Some(token) = &sample_annotation.visibility_token {
            ensure!(
                visibility_tokens.contains(token),
                "the token {} does not refer to any visibility",
                token
            );
        }

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
