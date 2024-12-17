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

type SpicepodSpec struct {
	Version      string                   `json:"version,omitempty" csv:"version" yaml:"version,omitempty"`
	Kind         string                   `json:"kind,omitempty" csv:"kind" yaml:"kind,omitempty"`
	Name         string                   `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Params       map[string]string        `json:"params,omitempty" yaml:"params,omitempty" mapstructure:"params,omitempty"`
	Metadata     map[string]string        `json:"metadata,omitempty" csv:"metadata" yaml:"metadata,omitempty"`
	Dependencies []string                 `json:"dependencies,omitempty" csv:"dependencies" yaml:"dependencies,omitempty"`
	Datasets     []map[string]interface{} `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`

	// Embed yaml.Node to preserve unknown fields
	Node *yaml.Node `yaml:",inline"`
}

// Custom UnmarshalYAML to handle both known and unknown fields
func (s *SpicepodSpec) UnmarshalYAML(value *yaml.Node) error {
	// Create a temporary type without the yaml.Node to avoid recursive unmarshaling
	type TempSpec struct {
		Version      string                   `yaml:"version,omitempty"`
		Kind         string                   `yaml:"kind,omitempty"`
		Name         string                   `yaml:"name,omitempty"`
		Params       map[string]string        `yaml:"params,omitempty"`
		Metadata     map[string]string        `yaml:"metadata,omitempty"`
		Dependencies []string                 `yaml:"dependencies,omitempty"`
		Datasets     []map[string]interface{} `yaml:"datasets,omitempty"`
	}

	// Decode known fields into temporary struct
	var tmp TempSpec
	if err := value.Decode(&tmp); err != nil {
		return err
	}

	// Copy known fields to the actual struct
	s.Version = tmp.Version
	s.Kind = tmp.Kind
	s.Name = tmp.Name
	s.Params = tmp.Params
	s.Metadata = tmp.Metadata
	s.Dependencies = tmp.Dependencies
	s.Datasets = tmp.Datasets
	// Get the content node
	contentNode := value
	if value.Kind == yaml.DocumentNode && len(value.Content) > 0 {
		contentNode = value.Content[0]
	}

	// Create a new node for unknown fields
	s.Node = &yaml.Node{
		Kind: yaml.MappingNode,
		Tag:  "!!map",
	}

	// Create a map of known fields for quick lookup
	knownFields := map[string]bool{
		"version":      true,
		"kind":         true,
		"name":         true,
		"params":       true,
		"metadata":     true,
		"dependencies": true,
		"datasets":     true,
	}

	// Preserve unknown fields
	for i := 0; i < len(contentNode.Content); i += 2 {
		key := contentNode.Content[i].Value
		if !knownFields[key] {
			// Deep copy the nodes to avoid reference issues
			keyNode := &yaml.Node{
				Kind:  contentNode.Content[i].Kind,
				Style: contentNode.Content[i].Style,
				Tag:   contentNode.Content[i].Tag,
				Value: contentNode.Content[i].Value,
			}
			valueNode := &yaml.Node{
				Kind:    contentNode.Content[i+1].Kind,
				Style:   contentNode.Content[i+1].Style,
				Tag:     contentNode.Content[i+1].Tag,
				Value:   contentNode.Content[i+1].Value,
				Content: make([]*yaml.Node, len(contentNode.Content[i+1].Content)),
			}
			if len(contentNode.Content[i+1].Content) > 0 {
				copy(valueNode.Content, contentNode.Content[i+1].Content)
			}
			s.Node.Content = append(s.Node.Content, keyNode, valueNode)
		}
	}

	return nil
}

// Custom MarshalYAML to output both known and unknown fields
func (s SpicepodSpec) MarshalYAML() (interface{}, error) {
	// Create a temporary type without the Node field to avoid recursive marshaling
	type TempSpec struct {
		Version      string                   `yaml:"version,omitempty"`
		Kind         string                   `yaml:"kind,omitempty"`
		Name         string                   `yaml:"name,omitempty"`
		Params       map[string]string        `yaml:"params,omitempty"`
		Metadata     map[string]string        `yaml:"metadata,omitempty"`
		Dependencies []string                 `yaml:"dependencies,omitempty"`
		Datasets     []map[string]interface{} `yaml:"datasets,omitempty"`
	}

	// Create a new node for the result
	result := &yaml.Node{
		Kind: yaml.MappingNode,
		Tag:  "!!map",
	}

	// Let yaml handle encoding of all known fields
	var knownNode yaml.Node
	if err := knownNode.Encode(TempSpec{
		Version:      s.Version,
		Kind:         s.Kind,
		Name:         s.Name,
		Params:       s.Params,
		Metadata:     s.Metadata,
		Dependencies: s.Dependencies,
		Datasets:     s.Datasets,
	}); err != nil {
		return nil, err
	}

	// Add known fields to result
	if knownNode.Kind == yaml.DocumentNode && len(knownNode.Content) > 0 {
		result.Content = append(result.Content, knownNode.Content[0].Content...)
	} else {
		result.Content = append(result.Content, knownNode.Content...)
	}

	// Add unknown fields from Node if they exist
	if s.Node != nil && len(s.Node.Content) > 0 {
		result.Content = append(result.Content, s.Node.Content...)
	}

	return result, nil
}
