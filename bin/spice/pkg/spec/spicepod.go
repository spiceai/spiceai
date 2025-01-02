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

package spec

import (
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// SpicepodSpecFields contains all the known fields of a Spicepod
type SpicepodSpecFields struct {
	Version      string                   `json:"version,omitempty" csv:"version" yaml:"version,omitempty"`
	Kind         string                   `json:"kind,omitempty" csv:"kind" yaml:"kind,omitempty"`
	Name         string                   `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Params       map[string]string        `json:"params,omitempty" yaml:"params,omitempty" mapstructure:"params,omitempty"`
	Metadata     map[string]string        `json:"metadata,omitempty" csv:"metadata" yaml:"metadata,omitempty"`
	Dependencies []string                 `json:"dependencies,omitempty" csv:"dependencies" yaml:"dependencies,omitempty"`
	Datasets     []map[string]interface{} `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`
}

// SpicepodSpec represents a Spicepod specification
type SpicepodSpec struct {
	SpicepodSpecFields
	// Embed yaml.Node to preserve unknown fields
	Node *yaml.Node `yaml:",inline"`
}

var knownFields map[string]bool

func init() {
	knownFields = getKnownFields()
}

// Custom UnmarshalYAML to handle both known and unknown fields
func (s *SpicepodSpec) UnmarshalYAML(value *yaml.Node) error {
	// Decode known fields
	if err := value.Decode(&s.SpicepodSpecFields); err != nil {
		return err
	}

	// Create a new node for unknown fields
	s.Node = &yaml.Node{
		Kind: yaml.MappingNode,
		Tag:  "!!map",
	}

	// Get the mapping node
	contentNode := value
	if value.Kind == yaml.DocumentNode && len(value.Content) > 0 {
		contentNode = value.Content[0]
	}

	// Preserve unknown fields by encoding them directly
	for i := 0; i < len(contentNode.Content); i += 2 {
		key := contentNode.Content[i].Value
		if !knownFields[key] {
			s.Node.Content = append(s.Node.Content,
				contentNode.Content[i],
				contentNode.Content[i+1])
		}
	}

	return nil
}

// Custom MarshalYAML to output both known and unknown fields
func (s SpicepodSpec) MarshalYAML() (interface{}, error) {
	// Create a new node for the result
	result := &yaml.Node{
		Kind: yaml.MappingNode,
		Tag:  "!!map",
	}

	// Let yaml handle encoding of all known fields
	var knownNode yaml.Node
	if err := knownNode.Encode(s.SpicepodSpecFields); err != nil {
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

// getKnownFields returns a map of field names from the yaml tags of SpicepodSpecFields
func getKnownFields() map[string]bool {
	knownFields := make(map[string]bool)
	t := reflect.TypeOf(SpicepodSpecFields{})

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		// Get the yaml tag
		if tag, ok := field.Tag.Lookup("yaml"); ok {
			// Split the tag on ',' and take the first part (the field name)
			name := strings.Split(tag, ",")[0]
			if name != "" {
				knownFields[name] = true
			}
		}
	}
	return knownFields
}
