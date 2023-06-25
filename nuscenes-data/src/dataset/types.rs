use super::dataset_inner::DatasetInner;
use crate::{
    error::Result,
    parsed::{InstanceInternal, SampleInternal, SceneInternal},
    serializable::{
        Attribute, CalibratedSensor, Category, EgoPose, Log, Map, SampleAnnotation, SampleData,
        Sensor, Visibility, VisibilityToken,
    },
    DatasetLoader, Token,
};
use ownref::ArcRefC;
use std::{
    ops::Deref,
    path::{Path, PathBuf},
};

type ARef<T> = ArcRefC<'static, DatasetInner, T>;

macro_rules! make_ref {
    ($name:ident, $ty:ty) => {
        pub struct $name {
            #[allow(dead_code)]
            owner: ARef<DatasetInner>,
            ref_: ARef<$ty>,
        }

        impl $name {
            #[allow(dead_code)]
            fn new(owner: ARef<DatasetInner>, ref_: ARef<$ty>) -> Self {
                Self { owner, ref_ }
            }

            pub fn dataset(&self) -> Dataset {
                Dataset::new(self.owner.clone(), self.owner.clone())
            }
        }

        impl Deref for $name {
            type Target = $ty;

            fn deref(&self) -> &Self::Target {
                self.ref_.deref()
            }
        }
    };
}

make_ref!(Dataset, DatasetInner);
make_ref!(AttributeRef, Attribute);
make_ref!(CalibratedSensorRef, CalibratedSensor);
make_ref!(CategoryRef, Category);
make_ref!(EgoPoseRef, EgoPose);
make_ref!(InstanceRef, InstanceInternal);
make_ref!(LogRef, Log);
make_ref!(MapRef, Map);
make_ref!(SceneRef, SceneInternal);
make_ref!(SampleRef, SampleInternal);
make_ref!(SampleAnnotationRef, SampleAnnotation);
make_ref!(SampleDataRef, SampleData);
make_ref!(SensorRef, Sensor);
make_ref!(VisibilityRef, Visibility);

impl Dataset {
    pub(crate) fn from_inner(inner: DatasetInner) -> Self {
        let owner = ARef::new(inner);
        Self {
            owner: owner.clone(),
            ref_: owner.clone(),
        }
    }

    pub fn load<P>(version: &str, dataset_dir: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        DatasetLoader::default().load(version, dataset_dir)
    }

    pub fn attribute(&self, token: Token) -> Option<AttributeRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.attribute_map.get(&token))?;
        Some(AttributeRef::new(self.owner.clone(), ref_))
    }

    pub fn calibrated_sensor(&self, token: Token) -> Option<CalibratedSensorRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.calibrated_sensor_map.get(&token))?;
        Some(CalibratedSensorRef::new(self.owner.clone(), ref_))
    }

    pub fn category(&self, token: Token) -> Option<CategoryRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.category_map.get(&token))?;
        Some(CategoryRef::new(self.owner.clone(), ref_))
    }

    pub fn ego_pose(&self, token: Token) -> Option<EgoPoseRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.ego_pose_map.get(&token))?;
        Some(EgoPoseRef::new(self.owner.clone(), ref_))
    }

    pub fn instance(&self, token: Token) -> Option<InstanceRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.instance_map.get(&token))?;
        Some(InstanceRef::new(self.owner.clone(), ref_))
    }

    pub fn log(&self, token: Token) -> Option<LogRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.log_map.get(&token))?;
        Some(LogRef::new(self.owner.clone(), ref_))
    }

    pub fn map(&self, token: Token) -> Option<MapRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.map_map.get(&token))?;
        Some(MapRef::new(self.owner.clone(), ref_))
    }

    pub fn scene(&self, token: Token) -> Option<SceneRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.scene_map.get(&token))?;
        Some(SceneRef::new(self.owner.clone(), ref_))
    }

    pub fn sample(&self, token: Token) -> Option<SampleRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.sample_map.get(&token))?;
        Some(SampleRef::new(self.owner.clone(), ref_))
    }

    pub fn sample_annotation(&self, token: Token) -> Option<SampleAnnotationRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.sample_annotation_map.get(&token))?;
        Some(SampleAnnotationRef::new(self.owner.clone(), ref_))
    }

    pub fn sample_data(&self, token: Token) -> Option<SampleDataRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.sample_data_map.get(&token))?;
        Some(SampleDataRef::new(self.owner.clone(), ref_))
    }

    pub fn sensor(&self, token: Token) -> Option<SensorRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.sensor_map.get(&token))?;
        Some(SensorRef::new(self.owner.clone(), ref_))
    }

    pub fn visibility(&self, token: VisibilityToken) -> Option<VisibilityRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.visibility_map.get(&token))?;
        Some(VisibilityRef::new(self.owner.clone(), ref_))
    }
}

macro_rules! impl_field_iter {
    ($method_name:ident, $field_name:ident, $item_ty:ident) => {
        impl Dataset {
            pub fn $method_name(&self) -> impl Iterator<Item = $item_ty> + Send + Sync + '_ {
                self.owner
                    .clone()
                    .flat_map(|owner| owner.$field_name.values())
                    .map(|item| $item_ty::new(self.owner.clone(), item))
            }
        }
    };
}

impl_field_iter!(attribute_iter, attribute_map, AttributeRef);
impl_field_iter!(
    calibrated_sensor_iter,
    calibrated_sensor_map,
    CalibratedSensorRef
);
impl_field_iter!(category_iter, category_map, CategoryRef);
impl_field_iter!(ego_pose_iter, ego_pose_map, EgoPoseRef);
impl_field_iter!(instance_iter, instance_map, InstanceRef);
impl_field_iter!(log_iter, log_map, LogRef);
impl_field_iter!(map_iter, map_map, MapRef);
impl_field_iter!(scene_iter, scene_map, SceneRef);
impl_field_iter!(sample_iter, sample_map, SampleRef);
impl_field_iter!(
    sample_annotation_iter,
    sample_annotation_map,
    SampleAnnotationRef
);
impl_field_iter!(sample_data_iter, sample_data_map, SampleDataRef);
impl_field_iter!(sensor_iter, sensor_map, SensorRef);
impl_field_iter!(visibility_iter, visibility_map, VisibilityRef);

impl CalibratedSensorRef {
    pub fn sensor(&self) -> SensorRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.sensor_map[&self.ref_.sensor_token]);
        SensorRef::new(self.owner.clone(), ref_)
    }
}

impl InstanceRef {
    pub fn category(&self) -> CategoryRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.category_map[&self.ref_.category_token]);
        CategoryRef::new(self.owner.clone(), ref_)
    }

    pub fn annotation_iter(
        &self,
    ) -> impl Iterator<Item = SampleAnnotationRef> + Send + Sync + Clone + '_ {
        self.ref_
            .annotation_tokens
            .iter()
            .map(|token| {
                self.owner
                    .clone()
                    .map(|owner| &owner.sample_annotation_map[token])
            })
            .map(|ref_| SampleAnnotationRef::new(self.owner.clone(), ref_))
    }
}

impl LogRef {
    // pub fn logfile(&self) -> Option<PathBuf> {
    //     Some(self.owner.dataset_dir.join(self.ref_.logfile.as_ref()?))
    // }
}

impl MapRef {
    pub fn log_iter(&self) -> impl Iterator<Item = LogRef> + Send + Sync + Clone + '_ {
        self.ref_
            .log_tokens
            .iter()
            .map(|token| self.owner.clone().map(|owner| &owner.log_map[token]))
            .map(|ref_| LogRef::new(self.owner.clone(), ref_))
    }

    pub fn path(&self) -> PathBuf {
        self.owner.dataset_dir.join(&self.ref_.filename)
    }
}

impl SceneRef {
    pub fn log(&self) -> LogRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.log_map[&self.ref_.log_token]);
        LogRef::new(self.owner.clone(), ref_)
    }

    pub fn sample_iter(&self) -> impl Iterator<Item = SampleRef> + Send + Sync + Clone + '_ {
        self.ref_
            .sample_tokens
            .iter()
            .map(|token| self.owner.clone().map(|owner| &owner.sample_map[token]))
            .map(|ref_| SampleRef::new(self.owner.clone(), ref_))
    }
}

impl SampleRef {
    pub fn next(&self) -> Option<SampleRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample_map[&self.ref_.next?]))?;
        Some(SampleRef::new(self.owner.clone(), ref_))
    }

    pub fn prev(&self) -> Option<SampleRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample_map[&self.ref_.prev?]))?;
        Some(SampleRef::new(self.owner.clone(), ref_))
    }

    pub fn scene(&self) -> SceneRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.scene_map[&self.ref_.scene_token]);
        SceneRef::new(self.owner.clone(), ref_)
    }

    pub fn annotation_iter(
        &self,
    ) -> impl Iterator<Item = SampleAnnotationRef> + Send + Sync + Clone + '_ {
        self.ref_
            .annotation_tokens
            .iter()
            .map(|token| {
                self.owner
                    .clone()
                    .map(|owner| &owner.sample_annotation_map[token])
            })
            .map(|ref_| SampleAnnotationRef::new(self.owner.clone(), ref_))
    }

    pub fn sample_data_iter(
        &self,
    ) -> impl Iterator<Item = SampleDataRef> + Send + Sync + Clone + '_ {
        self.ref_
            .sample_data_tokens
            .iter()
            .map(|token| {
                self.owner
                    .clone()
                    .map(|owner| &owner.sample_data_map[token])
            })
            .map(|ref_| SampleDataRef::new(self.owner.clone(), ref_))
    }
}

impl SampleAnnotationRef {
    pub fn sample(&self) -> SampleRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.sample_map[&self.ref_.sample_token]);
        SampleRef::new(self.owner.clone(), ref_)
    }

    pub fn instance(&self) -> InstanceRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.instance_map[&self.ref_.instance_token]);
        InstanceRef::new(self.owner.clone(), ref_)
    }

    pub fn attribute_iter(&self) -> impl Iterator<Item = AttributeRef> + Send + Sync + Clone + '_ {
        self.ref_
            .attribute_tokens
            .iter()
            .map(|token| self.owner.clone().map(|owner| &owner.attribute_map[token]))
            .map(|ref_| AttributeRef::new(self.owner.clone(), ref_))
    }

    pub fn visibility(&self) -> Option<VisibilityRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.visibility_map[&self.ref_.visibility_token?]))?;
        Some(VisibilityRef::new(self.owner.clone(), ref_))
    }

    pub fn next(&self) -> Option<SampleAnnotationRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample_annotation_map[&self.ref_.next?]))?;
        Some(SampleAnnotationRef::new(self.owner.clone(), ref_))
    }

    pub fn prev(&self) -> Option<SampleAnnotationRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample_annotation_map[&self.ref_.prev?]))?;
        Some(SampleAnnotationRef::new(self.owner.clone(), ref_))
    }
}

impl SampleDataRef {
    pub fn sample(&self) -> SampleRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.sample_map[&self.ref_.sample_token]);
        SampleRef::new(self.owner.clone(), ref_)
    }

    pub fn ego_pose(&self) -> EgoPoseRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.ego_pose_map[&self.ref_.ego_pose_token]);
        EgoPoseRef::new(self.owner.clone(), ref_)
    }

    pub fn calibrated_sensor(&self) -> CalibratedSensorRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.calibrated_sensor_map[&self.ref_.calibrated_sensor_token]);
        CalibratedSensorRef::new(self.owner.clone(), ref_)
    }

    pub fn next(&self) -> Option<SampleDataRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample_data_map[&self.ref_.next?]))?;
        Some(SampleDataRef::new(self.owner.clone(), ref_))
    }

    pub fn prev(&self) -> Option<SampleDataRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample_data_map[&self.ref_.prev?]))?;
        Some(SampleDataRef::new(self.owner.clone(), ref_))
    }

    pub fn path(&self) -> PathBuf {
        self.owner.dataset_dir.join(&self.ref_.filename)
    }
}
