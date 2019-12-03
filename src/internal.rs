use crate::meta::{
    Attribute, CalibratedSensor, CameraIntrinsic, Category, EgoPose, FileFormat, Instance, Log,
    LongToken, Map, Sample, SampleAnnotation, SampleData, Scene, Sensor, ShortToken, Visibility,
};
use failure::{bail, ensure, Fallible};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct CalibratedSensorInternal<'a> {
    pub token: LongToken,
    pub sensor_ref: &'a Sensor,
    pub rotation: [f64; 4],
    pub camera_intrinsic: CameraIntrinsic,
    pub translation: [f64; 3],
}

impl<'a> CalibratedSensorInternal<'a> {
    pub fn from(calibrated_sensor: CalibratedSensor, sensor_ref: &'a Sensor) -> Fallible<Self> {
        let CalibratedSensor {
            token,
            sensor_token,
            rotation,
            camera_intrinsic,
            translation,
        } = calibrated_sensor;

        ensure!(sensor_token == sensor_ref.token);

        let ret = Self {
            token,
            sensor_ref,
            rotation,
            camera_intrinsic,
            translation,
        };

        Ok(ret)
    }
}

#[derive(Debug, Clone)]
pub struct InstanceInternal<'a> {
    pub token: LongToken,
    pub category_ref: &'a Category,
    pub annotation_refs: Vec<&'a SampleAnnotationInternal<'a>>,
}

impl<'a> InstanceInternal<'a> {
    pub fn from(
        instance: Instance,
        category_ref: &'a Category,
        annotation_refs: Vec<&'a SampleAnnotationInternal<'a>>,
    ) -> Fallible<Self> {
        let Instance {
            token,
            nbr_annotations,
            category_token,
            first_annotation_token,
            last_annotation_token,
        } = instance;

        ensure!(category_token == category_ref.token);
        ensure!(annotation_refs.len() == nbr_annotations && !annotation_refs.is_empty());
        ensure!(annotation_refs.first().unwrap().token == first_annotation_token);
        ensure!(annotation_refs.last().unwrap().token == last_annotation_token);

        let ret = Self {
            token,
            category_ref,
            annotation_refs,
        };

        Ok(ret)
    }
}

#[derive(Debug, Clone)]
pub struct MapInternal<'a> {
    pub token: ShortToken,
    pub log_refs: Vec<&'a Log>,
    pub filename: PathBuf,
    pub category: String,
}

impl<'a> MapInternal<'a> {
    pub fn from(map: Map, log_refs: Vec<&'a Log>) -> Fallible<Self> {
        let Map {
            token,
            log_tokens,
            filename,
            category,
        } = map;

        ensure!(log_tokens.len() == log_refs.len());
        for (log_token, log_ref) in log_tokens.iter().zip(log_refs.iter()) {
            ensure!(log_token == &log_ref.token);
        }

        let ret = Self {
            token,
            log_refs,
            filename,
            category,
        };

        Ok(ret)
    }
}

#[derive(Debug, Clone)]
pub struct SampleAnnotationInternal<'a> {
    pub token: LongToken,
    pub num_lidar_pts: isize,
    pub num_radar_pts: isize,
    pub size: [f64; 3],
    pub rotation: [f64; 4],
    pub translation: [f64; 3],
    pub attribute_refs: Vec<&'a Attribute>,
    pub visibility_ref_opt: Option<&'a Visibility>,
}

impl<'a> SampleAnnotationInternal<'a> {
    pub fn from(
        annotation: SampleAnnotation,
        attribute_refs: Vec<&'a Attribute>,
        visibility_ref_opt: Option<&'a Visibility>,
    ) -> Fallible<Self> {
        let SampleAnnotation {
            token,
            num_lidar_pts,
            num_radar_pts,
            size,
            rotation,
            translation,
            sample_token: _,
            instance_token: _,
            attribute_tokens,
            visibility_token,
            prev: _,
            next: _,
        } = annotation;

        ensure!(attribute_tokens.len() == attribute_refs.len());

        for (token, attribute_ref) in attribute_tokens.iter().zip(attribute_refs.iter()) {
            ensure!(token == &attribute_ref.token);
        }

        match (visibility_token, visibility_ref_opt) {
            (None, None) => (),
            (Some(token), Some(visibility_ref)) => {
                ensure!(token == visibility_ref.token);
            }
            _ => bail!("please report bug"),
        }

        let ret = Self {
            token,
            num_lidar_pts,
            num_radar_pts,
            size,
            rotation,
            translation,
            attribute_refs,
            visibility_ref_opt,
        };

        Ok(ret)
    }
}

#[derive(Debug, Clone)]
pub struct SampleInternal<'a> {
    pub token: LongToken,
    pub timestamp: f64,
    pub sample_data_refs: Vec<&'a SampleDataInternal<'a>>,
    pub annotation_refs: Vec<&'a SampleAnnotationInternal<'a>>,
}

impl<'a> SampleInternal<'a> {
    pub fn from(
        sample: Sample,
        sample_data_refs: Vec<&'a SampleDataInternal<'a>>,
        annotation_refs: Vec<&'a SampleAnnotationInternal<'a>>,
    ) -> Fallible<Self> {
        let Sample {
            token,
            next: _,
            prev: _,
            scene_token: _,
            timestamp,
        } = sample;

        let ret = Self {
            token,
            timestamp,
            sample_data_refs,
            annotation_refs,
        };

        Ok(ret)
    }
}

#[derive(Debug, Clone)]
pub struct SampleDataInternal<'a> {
    pub token: LongToken,
    pub fileformat: FileFormat,
    pub is_key_frame: bool,
    pub timestamp: f64,
    pub filename: String,
    pub ego_pose_ref: &'a EgoPose,
    pub calibrated_sensor_ref: &'a CalibratedSensorInternal<'a>,
}

impl<'a> SampleDataInternal<'a> {
    pub fn from(
        sample_data: SampleData,
        ego_pose_ref: &'a EgoPose,
        calibrated_sensor_ref: &'a CalibratedSensorInternal<'a>,
    ) -> Fallible<Self> {
        let SampleData {
            token,
            fileformat,
            is_key_frame,
            filename,
            timestamp,
            sample_token: _,
            ego_pose_token,
            calibrated_sensor_token,
            prev: _,
            next: _,
        } = sample_data;

        ensure!(ego_pose_token == ego_pose_ref.token);
        ensure!(calibrated_sensor_token == calibrated_sensor_ref.token);

        let ret = Self {
            token,
            fileformat,
            is_key_frame,
            ego_pose_ref,
            calibrated_sensor_ref,
            timestamp,
            filename,
        };

        Ok(ret)
    }
}

#[derive(Debug, Clone)]
pub struct SceneInternal<'a> {
    pub token: LongToken,
    pub name: String,
    pub description: String,
    pub log_ref: &'a Log,
    pub sample_refs: Vec<&'a SampleInternal<'a>>,
}

impl<'a> SceneInternal<'a> {
    pub fn from(
        scene: Scene,
        log_ref: &'a Log,
        sample_refs: Vec<&'a SampleInternal<'a>>,
    ) -> Fallible<Self> {
        let Scene {
            token,
            name,
            description,
            nbr_samples,
            log_token,
            first_sample_token,
            last_sample_token,
        } = scene;

        ensure!(log_token == log_ref.token);
        ensure!(nbr_samples == sample_refs.len() && !sample_refs.is_empty());
        ensure!(sample_refs.first().unwrap().token == first_sample_token);
        ensure!(sample_refs.last().unwrap().token == last_sample_token);

        let ret = Self {
            token,
            name,
            description,
            log_ref,
            sample_refs,
        };

        Ok(ret)
    }
}
