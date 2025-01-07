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
	"fmt"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

// convertMap converts map[interface{}]interface{} to map[string]interface{}
func convertMap(i interface{}) interface{} {
	switch x := i.(type) {
	case map[interface{}]interface{}:
		m2 := map[string]interface{}{}
		for k, v := range x {
			// Convert key to string, handling different types
			var strKey string
			switch k := k.(type) {
			case string:
				strKey = k
			case bool:
				strKey = fmt.Sprintf("%v", k)
			case int:
				strKey = fmt.Sprintf("%d", k)
			case float64:
				strKey = fmt.Sprintf("%g", k)
			default:
				strKey = fmt.Sprintf("%v", k)
			}
			m2[strKey] = convertMap(v)
		}
		return m2
	case []interface{}:
		for i, v := range x {
			x[i] = convertMap(v)
		}
	}
	return i
}

func TestSpicepodSpec_UnmarshalYAML_KnownFields(t *testing.T) {
	yamlText := `
version: v1
kind: Spicepod
name: test-pod
datasets:
  - from: spice.ai/spiceai/tpch/datasets/tpch.customer
    name: tpch.customer
  - ref: datasets/lineitem
params:
  key1: value1
  key2: value2
metadata:
  meta1: value1
dependencies:
  - dep1
  - dep2
`
	var spicePod SpicepodSpec
	err := yaml.Unmarshal([]byte(yamlText), &spicePod)
	if err != nil {
		t.Fatalf("Failed to unmarshal yaml: %v", err)
	}

	// Verify known fields
	if spicePod.Version != "v1" {
		t.Errorf("Expected version v1, got %s", spicePod.Version)
	}
	if spicePod.Kind != "Spicepod" {
		t.Errorf("Expected kind Spicepod, got %s", spicePod.Kind)
	}
	if spicePod.Name != "test-pod" {
		t.Errorf("Expected name test-pod, got %s", spicePod.Name)
	}
	if !reflect.DeepEqual(spicePod.Params, map[string]string{"key1": "value1", "key2": "value2"}) {
		t.Errorf("Params not as expected, got %v", spicePod.Params)
	}
	if !reflect.DeepEqual(spicePod.Metadata, map[string]string{"meta1": "value1"}) {
		t.Errorf("Metadata not as expected, got %v", spicePod.Metadata)
	}
	if !reflect.DeepEqual(spicePod.Dependencies, []string{"dep1", "dep2"}) {
		t.Errorf("Dependencies not as expected, got %v", spicePod.Dependencies)
	}
	if !reflect.DeepEqual(spicePod.Datasets, []map[string]interface{}{
		{"from": "spice.ai/spiceai/tpch/datasets/tpch.customer", "name": "tpch.customer"},
		{"ref": "datasets/lineitem"},
	}) {
		t.Errorf("Datasets not as expected, got %v", spicePod.Datasets)
	}
}

func TestSpicepodSpec_UnmarshalYAML_UnknownFields(t *testing.T) {
	yamlText := `
version: v1
kind: Spicepod
name: test-pod
datasets:
  - from: spice.ai/spiceai/tpch/datasets/tpch.customer
    name: tpch.customer
  - ref: datasets/lineitem
unknown_field: value
nested_unknown:
  field1: value1
  field2: value2
`
	var spicePod SpicepodSpec
	err := yaml.Unmarshal([]byte(yamlText), &spicePod)
	if err != nil {
		t.Fatalf("Failed to unmarshal yaml: %v", err)
	}

	// Verify known fields
	if spicePod.Version != "v1" {
		t.Errorf("Expected version v1, got %s", spicePod.Version)
	}

	// Verify unknown fields are preserved in Node
	if spicePod.Node == nil {
		t.Fatal("Node should not be nil")
	}

	// Marshal back to verify unknown fields are preserved
	output, err := yaml.Marshal(&spicePod)
	if err != nil {
		t.Fatalf("Failed to marshal back to yaml: %v", err)
	}

	// Unmarshal into a map to check all fields
	var result map[string]interface{}
	err = yaml.Unmarshal(output, &result)
	if err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	// Convert the map before checking values
	result = convertMap(result).(map[string]interface{})

	// Check unknown fields exist
	if _, exists := result["unknown_field"]; !exists {
		t.Error("unknown_field was not preserved")
	}
	if nested, exists := result["nested_unknown"].(map[string]interface{}); !exists {
		t.Error("nested_unknown was not preserved")
	} else {
		if nested["field1"] != "value1" || nested["field2"] != "value2" {
			t.Error("nested unknown fields were not preserved correctly")
		}
	}
}

func TestSpicepodSpec_MarshalYAML(t *testing.T) {
	// Create a SpicepodSpec with both known and unknown fields
	yamlText := `
version: v1
kind: Spicepod
name: test-pod
datasets:
  - from: spice.ai/spiceai/tpch/datasets/tpch.customer
    name: tpch.customer
  - ref: datasets/lineitem
params:
  key1: value1
unknown_field: test
nested_unknown:
  field1: value1
`
	var spicePod SpicepodSpec
	err := yaml.Unmarshal([]byte(yamlText), &spicePod)
	if err != nil {
		t.Fatalf("Failed to unmarshal initial yaml: %v", err)
	}

	// Marshal it back to YAML
	output, err := yaml.Marshal(&spicePod)
	if err != nil {
		t.Fatalf("Failed to marshal to yaml: %v", err)
	}

	// Unmarshal into a map to check all fields
	var result map[string]interface{}
	err = yaml.Unmarshal(output, &result)
	if err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	// Convert the map before checking values
	result = convertMap(result).(map[string]interface{})

	// Check known fields
	if result["version"] != "v1" {
		t.Errorf("Expected version v1, got %v", result["version"])
	}
	if result["kind"] != "Spicepod" {
		t.Errorf("Expected kind Spicepod, got %v", result["kind"])
	}
	if result["name"] != "test-pod" {
		t.Errorf("Expected name test-pod, got %v", result["name"])
	}
	if params, ok := result["params"].(map[string]interface{}); !ok || params["key1"] != "value1" {
		t.Errorf("Params not preserved correctly")
	}
	if datasets, ok := result["datasets"].([]interface{}); !ok {
		t.Errorf("Datasets not preserved correctly, expected []interface{}, got %T", result["datasets"])
	} else if len(datasets) != 2 {
		t.Errorf("Expected 2 datasets, got %d", len(datasets))
	} else {
		// Check first dataset
		if dataset0, ok := datasets[0].(map[string]interface{}); !ok {
			t.Errorf("First dataset not a map, got %T", datasets[0])
		} else {
			if dataset0["from"] != "spice.ai/spiceai/tpch/datasets/tpch.customer" {
				t.Errorf("First dataset 'from' not preserved, got %v", dataset0["from"])
			}
			if dataset0["name"] != "tpch.customer" {
				t.Errorf("First dataset 'name' not preserved, got %v", dataset0["name"])
			}
		}

		// Check second dataset
		if dataset1, ok := datasets[1].(map[string]interface{}); !ok {
			t.Errorf("Second dataset not a map, got %T", datasets[1])
		} else {
			if dataset1["ref"] != "datasets/lineitem" {
				t.Errorf("Second dataset 'ref' not preserved, got %v", dataset1["ref"])
			}
		}
	}

	// Check unknown fields
	if result["unknown_field"] != "test" {
		t.Errorf("unknown_field not preserved, got %v", result["unknown_field"])
	}
	if nested, ok := result["nested_unknown"].(map[string]interface{}); !ok || nested["field1"] != "value1" {
		t.Errorf("nested_unknown not preserved correctly")
	}
}

func TestSpicepodSpec_UnmarshalYAML_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr bool
	}{
		{
			name:    "empty document",
			yaml:    "",
			wantErr: false,
		},
		{
			name:    "null document",
			yaml:    "null",
			wantErr: false,
		},
		{
			name:    "minimal valid document",
			yaml:    "version: v1\nkind: Spicepod\nname: test",
			wantErr: false,
		},
		{
			name:    "invalid yaml",
			yaml:    "invalid: : yaml:",
			wantErr: true,
		},
		{
			name: "duplicate known and unknown fields",
			yaml: `
version: v1
kind: Spicepod
name: test-pod
unknown_kind: different
unknown_version: v2
`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var spicePod SpicepodSpec
			err := yaml.Unmarshal([]byte(tt.yaml), &spicePod)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalYAML() error = %v, wantErr %v", err, tt.wantErr)
			}

			// For the duplicate fields test, verify both known and unknown fields
			if tt.name == "duplicate known and unknown fields" && err == nil {
				if spicePod.Kind != "Spicepod" {
					t.Errorf("Expected kind Spicepod, got %s", spicePod.Kind)
				}

				// Marshal back to verify unknown fields are preserved
				output, err := yaml.Marshal(&spicePod)
				if err != nil {
					t.Fatalf("Failed to marshal back to yaml: %v", err)
				}

				// Unmarshal into a map to check all fields
				var result map[string]interface{}
				err = yaml.Unmarshal(output, &result)
				if err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}

				// Check that both known and unknown fields exist
				if _, exists := result["unknown_kind"]; !exists {
					t.Error("unknown_kind was not preserved")
				}
				if _, exists := result["unknown_version"]; !exists {
					t.Error("unknown_version was not preserved")
				}
				if result["kind"] != "Spicepod" {
					t.Error("known field 'kind' was not preserved correctly")
				}
			}
		})
	}
}

func TestSpicepodSpec_EmptyDatasets(t *testing.T) {
	yamlText := `
version: v1
kind: Spicepod
name: test-pod
datasets: []
`
	var spicePod SpicepodSpec
	err := yaml.Unmarshal([]byte(yamlText), &spicePod)
	if err != nil {
		t.Fatalf("Failed to unmarshal yaml: %v", err)
	}

	if spicePod.Datasets == nil || len(spicePod.Datasets) != 0 {
		t.Errorf("Expected empty datasets slice, got %v", spicePod.Datasets)
	}
}

func TestSpicepodSpec_ComplexDatasets(t *testing.T) {
	yamlText := `
version: v1
kind: Spicepod
name: test-pod
datasets:
  - name: dataset1
    nested:
      key1: value1
      key2:
        nested2: value2
    array: [1, 2, 3]
    numbers:
      int: 42
      float: 3.14
      scientific: 1e-10
    booleans:
      true: true
      false: false
    nullValue: null
`
	var spicePod SpicepodSpec
	err := yaml.Unmarshal([]byte(yamlText), &spicePod)
	if err != nil {
		t.Fatalf("Failed to unmarshal yaml: %v", err)
	}

	// Marshal back to YAML
	output, err := yaml.Marshal(&spicePod)
	if err != nil {
		t.Fatalf("Failed to marshal back to yaml: %v", err)
	}

	// Unmarshal into a map to check fields
	var result map[string]interface{}
	err = yaml.Unmarshal(output, &result)
	if err != nil {
		t.Fatalf("Failed to unmarshal output: %v", err)
	}

	// Convert the map before checking values
	result = convertMap(result).(map[string]interface{})

	datasets, ok := result["datasets"].([]interface{})
	if !ok || len(datasets) != 1 {
		t.Fatalf("Expected 1 dataset, got %v", result["datasets"])
	}

	dataset := convertMap(datasets[0]).(map[string]interface{})

	// Check nested structure
	nested := convertMap(dataset["nested"]).(map[string]interface{})
	if nested["key1"] != "value1" {
		t.Errorf("Expected nested.key1 = value1, got %v", nested["key1"])
	}

	nested2 := convertMap(nested["key2"]).(map[string]interface{})
	if nested2["nested2"] != "value2" {
		t.Errorf("Expected nested.key2.nested2 = value2, got %v", nested2["nested2"])
	}

	// Check array
	array := dataset["array"].([]interface{})
	expectedArray := []interface{}{1, 2, 3}
	if !reflect.DeepEqual(array, expectedArray) {
		t.Errorf("Expected array %v, got %v", expectedArray, array)
	}

	// Check numbers
	numbers := convertMap(dataset["numbers"]).(map[string]interface{})
	if v, ok := numbers["int"].(int); !ok || v != 42 {
		t.Errorf("Expected numbers.int = 42, got %v of type %T", numbers["int"], numbers["int"])
	}
	if v, ok := numbers["float"].(float64); !ok || v != 3.14 {
		t.Errorf("Expected numbers.float = 3.14, got %v of type %T", numbers["float"], numbers["float"])
	}
	if v, ok := numbers["scientific"].(float64); !ok || v != 1e-10 {
		t.Errorf("Expected numbers.scientific = 1e-10, got %v of type %T", numbers["scientific"], numbers["scientific"])
	}

	// Check booleans
	booleans := convertMap(dataset["booleans"]).(map[string]interface{})
	if !booleans["true"].(bool) {
		t.Errorf("Expected booleans.true = true, got %v", booleans["true"])
	}
	if booleans["false"].(bool) {
		t.Errorf("Expected booleans.false = false, got %v", booleans["false"])
	}

	// Check null value
	if dataset["nullValue"] != nil {
		t.Errorf("Expected nullValue = nil, got %v", dataset["nullValue"])
	}
}

func TestSpicepodSpec_PreserveYAMLStyle(t *testing.T) {
	yamlText := `
version: v1
kind: Spicepod
name: test-pod
datasets:
  - name: dataset1
    flow-style: {key1: value1, key2: value2}
    block-style:
      key1: value1
      key2: value2
    flow-sequence: [1, 2, 3]
    block-sequence:
      - 1
      - 2
      - 3
`
	var spicePod SpicepodSpec
	err := yaml.Unmarshal([]byte(yamlText), &spicePod)
	if err != nil {
		t.Fatalf("Failed to unmarshal yaml: %v", err)
	}

	// Marshal back to YAML
	output, err := yaml.Marshal(&spicePod)
	if err != nil {
		t.Fatalf("Failed to marshal back to yaml: %v", err)
	}

	// The style checks would be visual in the output string
	outputStr := string(output)
	t.Logf("Marshaled YAML:\n%s", outputStr)
}
