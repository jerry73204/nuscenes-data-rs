use nalgebra as na;

pub use nalgebra;
use nuscenes_data::serializable::{CalibratedSensor, EgoPose, SampleAnnotation};

pub mod prelude {
    pub use super::CalibratedSensorNalgebraExt;
}

pub trait CalibratedSensorNalgebraExt {
    fn na_camera_intrinsic_matrix(&self) -> Option<na::Matrix3<f64>>;
    fn na_translation(&self) -> na::Translation3<f64>;
}

impl CalibratedSensorNalgebraExt for CalibratedSensor {
    fn na_camera_intrinsic_matrix(&self) -> Option<na::Matrix3<f64>> {
        let iter = self.camera_intrinsic.as_ref()?.iter().flatten().cloned();
        Some(na::Matrix3::from_iterator(iter))
    }

    fn na_translation(&self) -> na::Translation3<f64> {
        self.translation.into()
    }
}

trait EgoPoseNalgebraExt {
    fn na_rotation(&self) -> na::UnitQuaternion<f64>;
    fn na_translation(&self) -> na::Translation3<f64>;
    fn na_transofrm(&self) -> na::Isometry3<f64>;
}

impl EgoPoseNalgebraExt for EgoPose {
    fn na_rotation(&self) -> na::UnitQuaternion<f64> {
        let quat: na::Quaternion<f64> = self.rotation.into();
        na::Unit::new_normalize(quat)
    }

    fn na_translation(&self) -> na::Translation3<f64> {
        self.translation.into()
    }

    fn na_transofrm(&self) -> na::Isometry3<f64> {
        na::Isometry3::from_parts(self.na_translation(), self.na_rotation())
    }
}

trait SampleAnnotationNalgebraExt {
    fn na_size(&self) -> na::Vector3<f64>;
    fn na_rotation(&self) -> na::UnitQuaternion<f64>;
    fn na_translation(&self) -> na::Translation3<f64>;
    fn na_transofrm(&self) -> na::Isometry3<f64>;
}

impl SampleAnnotationNaExt for SampleAnnotation {
    fn na_rotation(&self) -> na::UnitQuaternion<f64> {
        let quat: na::Quaternion<f64> = self.rotation.into();
        na::Unit::new_normalize(quat)
    }

    fn na_translation(&self) -> na::Translation3<f64> {
        self.translation.into()
    }

    fn na_transofrm(&self) -> na::Isometry3<f64> {
        na::Isometry3::from_parts(self.na_translation(), self.na_rotation())
    }

    fn na_size(&self) -> na::Vector3<f64> {
        self.size.into()
    }
}
