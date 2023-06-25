//! nuScenes dataset loader.
//!
//! # Tutorial
//!
//! This section gives a brief overview of this crate. It assumes the
//! dataset is located at "/path/to/dataset" directory. It contains a
//! "v1.0-trainval" directory, which name is also the version number.
//!
//! ## Load nuScenes Dataset
//!
//! Import [Dataset] type and use it to load the data directory. The
//! dataset version is "v1.0-trainval" in this example. You should
//! able to find the "/path/to/dataset/v1.0-trainval" directory.
//!
//! ```ignore
//! use nuscenes_data::Dataset;
//!
//! let dataset = Dataset::load("v1.0-trainval", "/path/to/dataset")?;
//! ```
//!
//! ## Traverse Scenes and Samples in the Dataset
//!
//! The dataset contains many scenes. Use `dataset.scene_iter()` to
//! iterate over scenes in the dataset. Scenes contain samples. Use
//! `scene.sample_iter()` to iterate them.
//!
//! ```ignore
//! for scene dataset.scene_iter() {
//!     for sample in scene.sample_iter() {
//!         for annotation in sample.annotatoin_iter() { /* omit */ }
//!         for data in sample.sample_data_iter() { /* omit */ }
//!     }
//! }
//! ```
//!
//! ## Look-up Samples using Tokens
//!
//! It supports data query using tokens. The usage is straightforward.
//!
//! ```ignore
//! let sample_token = Token::from_str("24fa5b014824491f9bd2f9e4a1a1b5b8").unwrap();
//! let sample = dataset.sample(sample_token).unwrap();
//!
//! let sensor_token = Token::from_str("1f69f87a4e175e5ba1d03e2e6d9bcd27").unwrap();
//! let sensor = dataset.sensor(sensor_token).unwrap();
//! ```
//!
//! ## Associated Data Query
//!
//! It's easy to search to associated data in the dataset.
//!
//! ```ignore
//! let scene = dataset.scene_iter().first().unwrap();
//! let sample = scene.sample_iter().first().unwrap();
//! let data = sample.sample_data_iter().first().unwrap();
//! let ego_pose = data.ego_pose();
//! let calibrated_sensor = data.calibrated_sensor();
//! ```
//!
//! ## Integration with [nalgebra](https://docs.rs/nalgebra)
//!
//! Add this extension crate to enable [nalgebra](https://docs.rs/nalgebra) support.
//!
//! ```sh
//! cargo add nuscenes-data-nalgebra
//! ```
//!
//! It add extra methods to existing types. This example obtains an
//! nalgebra transformation from an ego\_pose object.
//!
//! ```ignore
//! use nuscenes_data_nalgebra::prelude::*;
//! use nalgebra as na;
//!
//! let ego_pose = dataset.ego_pose(token).unwrap();
//! let transform: na::Isometry3<f64> = ego_pose.na_transform();
//!
//! let old_point = na::Point3::new(0.0, 0.0, 0.0);
//! let new_point = &transform * &old_point;
//! ```
//!
//! ## Load Data Files
//!
//! This crate supports integration with
//! [opencv](https://docs.rs/opencv), [image](https://docs.rs/image)
//! and [pcd-rs](https://docs.rs/pcd-rs) crates. Add these extension
//! crates to enable this.
//!
//! ```sh
//! cargo add nuscenes-data-opencv
//! cargo add nuscenes-data-image
//! cargo add nuscenes-data-pcd
//! ```
//!
//! It adds data loading methods on sample data objects.
//!
//! ```ignore
//! // image
//! use nuscenes_data_image::prelude::*;
//! let image_sample = dataset.sample_data(token).unwrap();
//! let image: image::DynamicImage = image_sample.load_dynamic_image()?.unwrap();
//!
//! // opencv
//! use nuscenes_data_opencv::prelude::*;
//! let image_sample = dataset.sample_data(token).unwrap();
//! let image: opencv::core::Mat = image_sample.load_opencv_mat()?.unwrap();
//!
//! // pcd-rs
//! use nuscenes_data_pcd::{prelude::*, PointCloud};
//! let pcd_sample = dataset.sample_data(token).unwrap();
//! let pcd: PointCloud = pcd_sample.load_pcd()?.unwrap();
//! match pcd {
//!     PointCloud::Pcd(points) => { /* Loaded from a .pcd file */ }
//!     PointCloud::Bin(points) => { /* Loaded from a .bin file */  }
//!     PointCloud::NotSupported => {}
//! }
//! ```

pub mod dataset;
pub mod error;
pub mod loader;
pub mod serializable;
pub mod utils;

pub use crate::{dataset::Dataset, loader::DatasetLoader, serializable::Token};
