use crate::{
    dataset::Dataset,
    parsed::{InstanceInternal, SampleInternal, SceneInternal},
    serializable::{
        Attribute, CalibratedSensor, Category, EgoPose, Log, Map, SampleAnnotation, SampleData,
        Sensor, Visibility, VisibilityToken,
    },
    Token,
};
use ownref::ArcRefC;
use std::ops::Deref;

type ARef<T> = ArcRefC<'static, Dataset, T>;

macro_rules! make_ref {
    ($name:ident, $ty:ty) => {
        pub struct $name {
            #[allow(dead_code)]
            owner: ARef<Dataset>,
            ref_: ARef<$ty>,
        }

        impl $name {
            #[allow(dead_code)]
            fn new(owner: ARef<Dataset>, ref_: ARef<$ty>) -> Self {
                Self { owner, ref_ }
            }

            pub fn dataset(&self) -> DatasetRef {
                DatasetRef::new(self.owner.clone(), self.owner.clone())
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

make_ref!(DatasetRef, Dataset);
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

impl DatasetRef {
    pub fn attribute(&self, token: Token) -> Option<AttributeRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.attribute.get(&token))?;
        Some(AttributeRef::new(self.owner.clone(), ref_))
    }

    pub fn calibrated_sensor(&self, token: Token) -> Option<CalibratedSensorRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.calibrated_sensor.get(&token))?;
        Some(CalibratedSensorRef::new(self.owner.clone(), ref_))
    }

    pub fn category(&self, token: Token) -> Option<CategoryRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.category.get(&token))?;
        Some(CategoryRef::new(self.owner.clone(), ref_))
    }

    pub fn ego_pose(&self, token: Token) -> Option<EgoPoseRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.ego_pose.get(&token))?;
        Some(EgoPoseRef::new(self.owner.clone(), ref_))
    }

    pub fn instance(&self, token: Token) -> Option<InstanceRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.instance.get(&token))?;
        Some(InstanceRef::new(self.owner.clone(), ref_))
    }

    pub fn log(&self, token: Token) -> Option<LogRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.log.get(&token))?;
        Some(LogRef::new(self.owner.clone(), ref_))
    }

    pub fn map(&self, token: Token) -> Option<MapRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.map.get(&token))?;
        Some(MapRef::new(self.owner.clone(), ref_))
    }

    pub fn scene(&self, token: Token) -> Option<SceneRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.scene.get(&token))?;
        Some(SceneRef::new(self.owner.clone(), ref_))
    }

    pub fn sample(&self, token: Token) -> Option<SampleRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.sample.get(&token))?;
        Some(SampleRef::new(self.owner.clone(), ref_))
    }

    pub fn sample_annotation(&self, token: Token) -> Option<SampleAnnotationRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.sample_annotation.get(&token))?;
        Some(SampleAnnotationRef::new(self.owner.clone(), ref_))
    }

    pub fn sample_data(&self, token: Token) -> Option<SampleDataRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.sample_data.get(&token))?;
        Some(SampleDataRef::new(self.owner.clone(), ref_))
    }

    pub fn sensor(&self, token: Token) -> Option<SensorRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.sensor.get(&token))?;
        Some(SensorRef::new(self.owner.clone(), ref_))
    }

    pub fn visibility(&self, token: VisibilityToken) -> Option<VisibilityRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| owner.visibility.get(&token))?;
        Some(VisibilityRef::new(self.owner.clone(), ref_))
    }
}

impl CalibratedSensorRef {
    pub fn sensor(&self) -> SensorRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.sensor[&self.ref_.sensor_token]);
        SensorRef::new(self.owner.clone(), ref_)
    }
}

impl InstanceRef {
    pub fn category(&self) -> CategoryRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.category[&self.ref_.category_token]);
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
                    .map(|owner| &owner.sample_annotation[token])
            })
            .map(|ref_| SampleAnnotationRef::new(self.owner.clone(), ref_))
    }
}

impl MapRef {
    pub fn log_iter(&self) -> impl Iterator<Item = LogRef> + Send + Sync + Clone + '_ {
        self.ref_
            .log_tokens
            .iter()
            .map(|token| self.owner.clone().map(|owner| &owner.log[token]))
            .map(|ref_| LogRef::new(self.owner.clone(), ref_))
    }
}

impl SceneRef {
    pub fn log(&self) -> LogRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.log[&self.ref_.log_token]);
        LogRef::new(self.owner.clone(), ref_)
    }

    pub fn sample_iter(&self) -> impl Iterator<Item = SampleRef> + Send + Sync + Clone + '_ {
        self.ref_
            .sample_tokens
            .iter()
            .map(|token| self.owner.clone().map(|owner| &owner.sample[token]))
            .map(|ref_| SampleRef::new(self.owner.clone(), ref_))
    }
}

impl SampleRef {
    pub fn next(&self) -> Option<SampleRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample[&self.ref_.next?]))?;
        Some(SampleRef::new(self.owner.clone(), ref_))
    }

    pub fn prev(&self) -> Option<SampleRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample[&self.ref_.prev?]))?;
        Some(SampleRef::new(self.owner.clone(), ref_))
    }

    pub fn scene(&self) -> SceneRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.scene[&self.ref_.scene_token]);
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
                    .map(|owner| &owner.sample_annotation[token])
            })
            .map(|ref_| SampleAnnotationRef::new(self.owner.clone(), ref_))
    }

    pub fn sample_data_iter(
        &self,
    ) -> impl Iterator<Item = SampleDataRef> + Send + Sync + Clone + '_ {
        self.ref_
            .sample_data_tokens
            .iter()
            .map(|token| self.owner.clone().map(|owner| &owner.sample_data[token]))
            .map(|ref_| SampleDataRef::new(self.owner.clone(), ref_))
    }
}

impl SampleAnnotationRef {
    pub fn sample(&self) -> SampleRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.sample[&self.ref_.sample_token]);
        SampleRef::new(self.owner.clone(), ref_)
    }

    pub fn instance(&self) -> InstanceRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.instance[&self.ref_.instance_token]);
        InstanceRef::new(self.owner.clone(), ref_)
    }

    pub fn attribute_iter(&self) -> impl Iterator<Item = AttributeRef> + Send + Sync + Clone + '_ {
        self.ref_
            .attribute_tokens
            .iter()
            .map(|token| self.owner.clone().map(|owner| &owner.attribute[token]))
            .map(|ref_| AttributeRef::new(self.owner.clone(), ref_))
    }

    pub fn visibility(&self) -> Option<VisibilityRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.visibility[&self.ref_.visibility_token?]))?;
        Some(VisibilityRef::new(self.owner.clone(), ref_))
    }

    pub fn next(&self) -> Option<SampleAnnotationRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample_annotation[&self.ref_.next?]))?;
        Some(SampleAnnotationRef::new(self.owner.clone(), ref_))
    }

    pub fn prev(&self) -> Option<SampleAnnotationRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample_annotation[&self.ref_.prev?]))?;
        Some(SampleAnnotationRef::new(self.owner.clone(), ref_))
    }
}

impl SampleDataRef {
    pub fn sample(&self) -> SampleRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.sample[&self.ref_.sample_token]);
        SampleRef::new(self.owner.clone(), ref_)
    }

    pub fn ego_pose(&self) -> EgoPoseRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.ego_pose[&self.ref_.ego_pose_token]);
        EgoPoseRef::new(self.owner.clone(), ref_)
    }

    pub fn calibrated_sensor(&self) -> CalibratedSensorRef {
        let ref_ = self
            .owner
            .clone()
            .map(|owner| &owner.calibrated_sensor[&self.ref_.calibrated_sensor_token]);
        CalibratedSensorRef::new(self.owner.clone(), ref_)
    }

    pub fn next(&self) -> Option<SampleDataRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample_data[&self.ref_.next?]))?;
        Some(SampleDataRef::new(self.owner.clone(), ref_))
    }

    pub fn prev(&self) -> Option<SampleDataRef> {
        let ref_ = self
            .owner
            .clone()
            .filter_map(|owner| Some(&owner.sample_data[&self.ref_.prev?]))?;
        Some(SampleDataRef::new(self.owner.clone(), ref_))
    }
}
