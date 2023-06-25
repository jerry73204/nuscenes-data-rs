# nuscenes-data: Loading nuScenes Dataset in Rust

\[ [crates.io](latest/nuscenes_data/) | [docs.rs](https://docs.rs/nuscenes-data/) \]

This project provides Rust implementation of nuScenes dataset loader,
which format is defined on the [nuScenes
website](https://www.nuscenes.org/data-format).

## Usage

Add the crate to your Rust project.

```sh
cargo add nuscenes-data
```

Import `Dataset` type and use it to load the data directory. The
dataset version is "v1.0-trainval" in this example. You should able to
find the "/path/to/dataset/v1.0-trainval" directory.

```rust
use nuscenes_data::Dataset;

let dataset = Dataset::load("v1.0-trainval", "/path/to/dataset")?;
```

The dataset contains many scenes. Use `dataset.scene_iter()` to
iterate over scenes in the dataset. Scenes contain samples. Use
`scene.sample_iter()` to iterate them.

```rust
for scene dataset.scene_iter() {
    for sample in scene.sample_iter() {
        for annotation in sample.annotatoin_iter() { /* omit */ }
        for data in sample.sample_data_iter() { /* omit */ }
    }
}
```

The complete tutorial can be found at the crate-level doc on
[docs.rs](https://docs.rs/nuscenes-data/).

## License

MIT license. See [license file](LICENSE).
