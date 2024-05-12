/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::{env, io::Write, path::PathBuf};

use serde_yaml::Value;
use snafu::{OptionExt, ResultExt, Snafu};

use crate::{
    component::dataset::acceleration::Acceleration,
    reader::{ReadableYaml, StdFileSystem},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to convert object to yaml: {source}"))]
    UnableToSerialize { source: serde_yaml::Error },
    #[snafu(display("Unable to overwrite {}: {source}", path.display()))]
    UnableToOverwriteYaml {
        source: std::io::Error,
        path: PathBuf,
    },
    #[snafu(display("spicepod.yaml not found in {}", path.display()))]
    SpicepodNotFound { path: PathBuf },
    #[snafu(display("Unable to parse spicepod.yaml: {source}"))]
    UnableToParseSpicepod { source: serde_yaml::Error },
    #[snafu(display("Yaml definition {} does not have dataset '{dataset_name}'", path.display()))]
    DatasetNotFound { dataset_name: String, path: PathBuf },
    #[snafu(display("Unable to update yaml"))]
    UnableToUpdateYaml {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct SpicepodUpdater {
    spicepod_definition: Value,
    path: PathBuf,
}

impl SpicepodUpdater {
    pub fn from_main_spicepod() -> Result<Self> {
        let current_dir = env::current_dir().unwrap_or(PathBuf::from("."));
        let yaml_path = current_dir.join("spicepod.yaml");
        let rdr =
            StdFileSystem
                .open_yaml(&current_dir, "spicepod")
                .context(SpicepodNotFoundSnafu {
                    path: current_dir.clone(),
                })?;

        Self::from_yaml_reader(rdr, yaml_path)
    }

    pub fn from_yaml_reader<R>(rdr: R, path: PathBuf) -> Result<Self>
    where
        R: std::io::Read,
    {
        let spicepod_definition: Value =
            serde_yaml::from_reader(rdr).context(UnableToParseSpicepodSnafu)?;

        Ok(Self {
            spicepod_definition,
            path,
        })
    }

    pub fn patch_acceleration(
        mut self,
        dataset_name: &str,
        acceleration: &Acceleration,
    ) -> Result<Self> {
        let updated_accel_value =
            serde_yaml::to_value(acceleration).context(UnableToSerializeSnafu)?;

        let path = self.path.clone();

        let dataset = self
            .get_mut_dataset(dataset_name)
            .context(DatasetNotFoundSnafu { dataset_name, path })?;

        if let Some(accel_map) = dataset
            .get_mut("acceleration")
            .and_then(|a| a.as_mapping_mut())
        {
            let accel_value = updated_accel_value;
            *accel_map = accel_value
                .as_mapping()
                .context(UnableToUpdateYamlSnafu)?
                .clone();
        } else {
            // acceleration section does not exist
            dataset
                .as_mapping_mut()
                .context(UnableToUpdateYamlSnafu)?
                .insert(Value::from("acceleration"), updated_accel_value);
        }

        Ok(self)
    }

    pub fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self.spicepod_definition).context(UnableToSerializeSnafu)
    }

    pub fn save(&self) -> Result<()> {
        let mut file = std::fs::File::create(&self.path)
            .context(UnableToOverwriteYamlSnafu { path: &self.path })?;
        let updated_yaml = self.to_yaml()?;
        file.write_all(updated_yaml.as_bytes())
            .context(UnableToOverwriteYamlSnafu { path: &self.path })?;

        Ok(())
    }

    fn get_mut_dataset(&mut self, dataset_name: &str) -> Option<&mut Value> {
        self.spicepod_definition
            .get_mut("datasets")
            .and_then(|d| d.as_sequence_mut())
            .and_then(|seq| {
                seq.iter_mut()
                    .find(|d| d.get("name").and_then(|n| n.as_str()) == Some(dataset_name))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn setup_from_yaml_string(yaml: &str) -> SpicepodUpdater {
        let cursor = Cursor::new(yaml.as_bytes());
        let path = PathBuf::from("/fake/path/for/testing");
        SpicepodUpdater::from_yaml_reader(cursor, path).expect("Should be able to parse yaml")
    }

    #[test]
    fn test_patch_existin_acceleration() {
        let initial_yaml = r"version: v1beta1
kind: Spicepod
name: test
datasets:
- from: s3://spiceai-demo-datasets/taxi_trips/2024/
  description: taxi trips in s3
  name: taxi_trips
  acceleration:
    enabled: false
    refresh_sql: SELECT * FROM taxi_trips limit 1;
";

        let expected_yaml = r"version: v1beta1
kind: Spicepod
name: test
datasets:
- from: s3://spiceai-demo-datasets/taxi_trips/2024/
  description: taxi trips in s3
  name: taxi_trips
  acceleration:
    enabled: false
    refresh_sql: SELECT * FROM taxi_trips limit 10;
";

        let mut updater = setup_from_yaml_string(initial_yaml);
        let acceleration = Acceleration {
            enabled: false,
            refresh_sql: Some("SELECT * FROM taxi_trips limit 10;".to_string()),
            ..Default::default()
        };

        updater = updater
            .patch_acceleration("taxi_trips", &acceleration)
            .expect("Should be able to patch acceleration");
        let output_yaml = updater.to_yaml().expect("Should be able to serialize yaml");

        assert_eq!(output_yaml, expected_yaml);
    }

    #[test]
    fn test_patch_non_existing_acceleration() {
        let initial_yaml = r"version: v1beta1
kind: Spicepod
name: test
datasets:
- from: s3://spiceai-demo-datasets/taxi_trips/2024/
  description: taxi trips in s3
  name: taxi_trips
";

        let expected_yaml = r"version: v1beta1
kind: Spicepod
name: test
datasets:
- from: s3://spiceai-demo-datasets/taxi_trips/2024/
  description: taxi trips in s3
  name: taxi_trips
  acceleration:
    enabled: true
    refresh_sql: SELECT * FROM taxi_trips limit 10;
";

        let mut updater = setup_from_yaml_string(initial_yaml);
        let acceleration = Acceleration {
            enabled: true,
            refresh_sql: Some("SELECT * FROM taxi_trips limit 10;".to_string()),
            ..Default::default()
        };

        updater = updater
            .patch_acceleration("taxi_trips", &acceleration)
            .expect("Should be able to patch acceleration");

        let output_yaml = updater.to_yaml().expect("Should be able to serialize yaml");

        assert_eq!(output_yaml, expected_yaml);
    }
}
