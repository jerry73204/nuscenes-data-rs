use failure::Fallible;
use nuscenes_data::NuSceneDataset;
// use nuscenes_data::meta::{
//     Attribute, CalibratedSensor, Category, EgoPose, Instance, Log, Map, Sample, SampleAnnotation,
//     SampleData, Scene, Sensor, Visibility,
// };
// use std::{
//     fs::File,
//     io::{prelude::*, BufReader},
// };

#[test]
fn load() -> Fallible<()> {
    NuSceneDataset::load("v1.02-train", "/mnt/wd/home/aeon/dataset/v1.02-train")?;
    Ok(())
}
