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

package spec

type SpicepodSpec struct {
	Version      string                   `json:"version,omitempty" csv:"version" yaml:"version,omitempty"`
	Kind         string                   `json:"kind,omitempty" csv:"kind" yaml:"kind,omitempty"`
	Name         string                   `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Params       map[string]string        `json:"params,omitempty" yaml:"params,omitempty" mapstructure:"params,omitempty"`
	Metadata     map[string]string        `json:"metadata,omitempty" csv:"metadata" yaml:"metadata,omitempty"`
	Datasets     []map[string]interface{} `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`
	Functions    []*Reference             `json:"functions,omitempty" csv:"functions" yaml:"functions,omitempty"`
	Models       []*Reference             `json:"models,omitempty" csv:"models" yaml:"models,omitempty"`
	Dependencies []string                 `json:"dependencies,omitempty" csv:"dependencies" yaml:"dependencies,omitempty"`
	Secrets      Secrets                  `json:"secrets,omitempty" csv:"secrets" yaml:"secrets,omitempty"`
}

type Reference struct {
	Ref       string `json:"ref,omitempty" csv:"ref" yaml:"ref,omitempty"`
	DependsOn string `json:"depends_on,omitempty" csv:"depends_on" yaml:"dependsOn,omitempty"`
}

type Secrets struct {
	Store string `json:"store,omitempty" csv:"store" yaml:"store,omitempty"`
}
