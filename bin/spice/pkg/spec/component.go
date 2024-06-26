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
	"reflect"

	"gopkg.in/yaml.v3"
)

type Component struct {
	Dataset   DatasetSpec
	Reference Reference
}

func (c Component) IsReference() bool {
	return c.Reference != (Reference{})
}

func (c Component) IsDataset() bool {
	return !reflect.DeepEqual(c.Dataset, DatasetSpec{})
}

// Helper function to find a node by key in a mapping node
func findNodeByKey(node *yaml.Node, key string) *yaml.Node {
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func hasNodeKey(node *yaml.Node, key string) bool {
	return findNodeByKey(node, key) != nil
}

func (c *Component) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode { // confirm the dataset entry is an key-pair object
		return nil
	}

	// Determine if it's a Reference or Dataset based on presence of 'ref' key
	if hasNodeKey(node, "ref") {
		var ref Reference
		if err := node.Decode(&ref); err != nil {
			return err
		}
		c.Reference = ref
	} else {
		var dataset DatasetSpec
		if err := node.Decode(&dataset); err != nil {
			return err
		}
		c.Dataset = dataset
	}
	return nil
}

func (c Component) MarshalYAML() (interface{}, error) {
	if c.IsReference() {
		return c.Reference, nil
	}

	if c.IsDataset() {
		return c.Dataset, nil
	}

	return nil, nil // should we error here?
}
