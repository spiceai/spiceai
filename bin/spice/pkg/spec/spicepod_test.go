package spec

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSpicepodSpec_UnmarshalYAML_KnownFields(t *testing.T) {
	yamlText := `
version: v1beta1
kind: Spicepod
name: test-pod
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
	if spicePod.Version != "v1beta1" {
		t.Errorf("Expected version v1beta1, got %s", spicePod.Version)
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
}

func TestSpicepodSpec_UnmarshalYAML_UnknownFields(t *testing.T) {
	yamlText := `
version: v1beta1
kind: Spicepod
name: test-pod
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
	if spicePod.Version != "v1beta1" {
		t.Errorf("Expected version v1beta1, got %s", spicePod.Version)
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
version: v1beta1
kind: Spicepod
name: test-pod
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

	// Check known fields
	if result["version"] != "v1beta1" {
		t.Errorf("Expected version v1beta1, got %v", result["version"])
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
			yaml:    "version: v1beta1\nkind: Spicepod\nname: test",
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
version: v1beta1
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
