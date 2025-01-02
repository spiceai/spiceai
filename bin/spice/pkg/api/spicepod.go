/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

package api

type Spicepod struct {
	Version      string    `json:"version,omitempty" csv:"version" yaml:"version,omitempty"`
	Name         string    `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Datasets     []Dataset `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`
	Models       []Model   `json:"models,omitempty" csv:"models" yaml:"models,omitempty"`
	Dependencies []string  `json:"dependencies,omitempty" csv:"depdencies" yaml:"dependencies,omitempty"`
}

type SpicepodStatus struct {
	Version           string `json:"version,omitempty" csv:"version" yaml:"version,omitempty"`
	Name              string `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	DatasetsCount     int    `json:"datasets_count,omitempty" csv:"datasets_count" yaml:"datasets_count,omitempty"`
	ModelsCount       int    `json:"models_count,omitempty" csv:"models_count" yaml:"models_count,omitempty"`
	DependenciesCount int    `json:"dependencies_count,omitempty" csv:"dependencies_count" yaml:"dependencies_count,omitempty"`
}
