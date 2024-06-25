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

import (
	"gopkg.in/yaml.v3"
)

// Helper function to find a node by key in a mapping node
func findNodeByKey(node *yaml.Node, key string) *yaml.Node {
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func (s *SpicepodSpec) UnmarshalYAML(value *yaml.Node) error {
	// Initialize a new struct to hold temporary values
	temp := struct {
		Version      string            `yaml:"version,omitempty"`
		Kind         string            `yaml:"kind,omitempty"`
		Name         string            `yaml:"name,omitempty"`
		Params       map[string]string `yaml:"params,omitempty"`
		Metadata     map[string]string `yaml:"metadata,omitempty"`
		Functions    []*Reference      `yaml:"functions,omitempty"`
		Models       []*Reference      `yaml:"models,omitempty"`
		Dependencies []string          `yaml:"dependencies,omitempty"`
		Secrets      Secrets           `yaml:"secrets,omitempty"`
	}{}

	// Unmarshal all fields except Datasets
	if err := value.Decode(&temp); err != nil {
		return err
	}

	// Set the values of the inner temp to the struct
	// This way we can retain the default unmarshal behaviour for values we don't care to change
	s.Version = temp.Version
	s.Kind = temp.Kind
	s.Name = temp.Name
	s.Params = temp.Params
	s.Metadata = temp.Metadata
	s.Functions = temp.Functions
	s.Models = temp.Models
	s.Dependencies = temp.Dependencies
	s.Secrets = temp.Secrets

	// Find and process datasets into their respective type (Reference or DatasetSpec)
	datasetsNode := findNodeByKey(value, "datasets")                   // this relies on the parent YAML being the correct key-pair structure
	if datasetsNode != nil && datasetsNode.Kind == yaml.SequenceNode { // we expect Datasets to be a list
		for _, node := range datasetsNode.Content {
			if node.Kind != yaml.MappingNode { // confirm the dataset entry is an key-pair object
				continue
			}

			// Determine if it's a Reference or Dataset based on presence of 'ref' key
			if refNode := findNodeByKey(node, "ref"); refNode != nil {
				var ref Reference
				if err := node.Decode(&ref); err != nil {
					return err
				}
				s.Datasets = append(s.Datasets, &ref) // we can append either Reference or DatasetSpec because they both implement DatasetOrReference
			} else {
				var dataset DatasetSpec
				if err := node.Decode(&dataset); err != nil {
					return err
				}
				s.Datasets = append(s.Datasets, &dataset) // we can append either Reference or DatasetSpec because they both implement DatasetOrReference
			}
		}
	}

	return nil
}

type SpicepodSpec struct {
	Version      string               `json:"version,omitempty" csv:"version" yaml:"version,omitempty"`
	Kind         string               `json:"kind,omitempty" csv:"kind" yaml:"kind,omitempty"`
	Name         string               `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Params       map[string]string    `json:"params,omitempty" yaml:"params,omitempty" mapstructure:"params,omitempty"`
	Metadata     map[string]string    `json:"metadata,omitempty" csv:"metadata" yaml:"metadata,omitempty"`
	Datasets     []DatasetOrReference `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`
	Functions    []*Reference         `json:"functions,omitempty" csv:"functions" yaml:"functions,omitempty"`
	Models       []*Reference         `json:"models,omitempty" csv:"models" yaml:"models,omitempty"`
	Dependencies []string             `json:"dependencies,omitempty" csv:"dependencies" yaml:"dependencies,omitempty"`
	Secrets      Secrets              `json:"secrets,omitempty" csv:"secrets" yaml:"secrets,omitempty"`
}

type DatasetOrReference interface {
	IsReference() bool
	IsDataset() bool
	Dataset() DatasetSpec
	Reference() Reference
}

type Reference struct {
	Ref       string `json:"ref,omitempty" csv:"ref" yaml:"ref,omitempty"`
	DependsOn string `json:"depends_on,omitempty" csv:"depends_on" yaml:"dependsOn,omitempty"`
}

func (Reference) IsReference() bool {
	return true
}

func (Reference) IsDataset() bool {
	return false
}

func (Reference) Dataset() DatasetSpec {
	panic("Value is a Reference, not a DatasetSpec!")
}

func (r Reference) Reference() Reference {
	return r
}

type Secrets struct {
	Store string `json:"store,omitempty" csv:"store" yaml:"store,omitempty"`
}
