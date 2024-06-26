package spec

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSpicepodDataset_LoadsDataset(t *testing.T) {
	yaml_text := `version: v1beta1
kind: Spicepod
name: spice_app
datasets:
    - from: datasets/uniswap_v2_eth_usdc
`

	var spicePod SpicepodSpec
	err := yaml.Unmarshal([]byte(yaml_text), &spicePod)
	if err != nil {
		t.Fatalf("Failed to unmarshal yaml: %v", err)
	}

	// confirm the only dataset is a Dataset and not a Reference
	if len(spicePod.Datasets) != 1 {
		t.Fatalf("Expected 1 dataset, got %d", len(spicePod.Datasets))
	}

	value := spicePod.Datasets[0]
	if value.Reference != (Reference{}) {
		t.Fatalf("Expected Dataset, got Reference")
	}

	if value.Dataset.From != "datasets/uniswap_v2_eth_usdc" {
		t.Fatalf("Expected datasets/uniswap_v2_eth_usdc, got %s", value.Dataset.From)
	}
}

func TestSpicepodDataset_LoadsReference(t *testing.T) {
	yaml_text := `version: v1beta1
kind: Spicepod
name: spice_app
datasets:
    - ref: eth_recent_blocks
`

	var spicePod SpicepodSpec
	err := yaml.Unmarshal([]byte(yaml_text), &spicePod)
	if err != nil {
		t.Fatalf("Failed to unmarshal yaml: %v", err)
	}

	// confirm the only dataset is a Reference and not a Dataset
	if len(spicePod.Datasets) != 1 {
		t.Fatalf("Expected 1 dataset, got %d", len(spicePod.Datasets))
	}

	value := spicePod.Datasets[0]
	if !reflect.DeepEqual(value.Dataset, DatasetSpec{}) {
		t.Fatalf("Expected Reference, got Dataset")
	}

	if value.Reference.Ref != "eth_recent_blocks" {
		t.Fatalf("Expected eth_recent_blocks, got %s", value.Reference.Ref)
	}
}

func TestSpicepodDataset_LoadsReferenceAndDataset(t *testing.T) {
	yaml_text := `version: v1beta1
kind: Spicepod
name: spice_app
datasets:
    - ref: eth_recent_blocks
    - from: datasets/uniswap_v2_eth_usdc
`

	var spicePod SpicepodSpec
	err := yaml.Unmarshal([]byte(yaml_text), &spicePod)
	if err != nil {
		t.Fatalf("Failed to unmarshal yaml: %v", err)
	}

	// confirm the only dataset is a Reference and not a Dataset
	if len(spicePod.Datasets) != 2 {
		t.Fatalf("Expected 2 datasets, got %d", len(spicePod.Datasets))
	}

	reference := spicePod.Datasets[0]
	if !reflect.DeepEqual(reference.Dataset, DatasetSpec{}) {
		t.Fatalf("Expected Reference, got Dataset")
	}

	if reference.Reference.Ref != "eth_recent_blocks" {
		t.Fatalf("Expected eth_recent_blocks, got %s", reference.Reference.Ref)
	}

	dataset := spicePod.Datasets[1]
	if dataset.Reference != (Reference{}) {
		t.Fatalf("Expected Dataset, got Reference")
	}

	if dataset.Dataset.From != "datasets/uniswap_v2_eth_usdc" {
		t.Fatalf("Expected datasets/uniswap_v2_eth_usdc, got %s", dataset.Dataset.From)
	}
}

func TestSpicepod_MarshalDataset(t *testing.T) {
	yaml_text := `version: v1beta1
kind: Spicepod
name: spice_app
datasets:
    - from: datasets/uniswap_v2_eth_usdc
`

	spicePod := SpicepodSpec{
		Version: "v1beta1",
		Kind:    "Spicepod",
		Name:    "spice_app",
		Datasets: []Component{
			{
				Dataset: DatasetSpec{
					From: "datasets/uniswap_v2_eth_usdc",
				},
			},
		},
	}

	bytes, err := yaml.Marshal(spicePod)
	if err != nil {
		t.Fatalf("Failed to marshal yaml: %v", err)
	}

	if string(bytes) != yaml_text {
		t.Fatalf("Expected %s, got %s", yaml_text, string(bytes))
	}
}

func TestSpicepod_MarshalReference(t *testing.T) {
	yaml_text := `version: v1beta1
kind: Spicepod
name: spice_app
datasets:
    - ref: eth_recent_blocks
`

	spicePod := SpicepodSpec{
		Version: "v1beta1",
		Kind:    "Spicepod",
		Name:    "spice_app",
		Datasets: []Component{
			{
				Reference: Reference{
					Ref: "eth_recent_blocks",
				},
			},
		},
	}

	bytes, err := yaml.Marshal(spicePod)
	if err != nil {
		t.Fatalf("Failed to marshal yaml: %v", err)
	}

	if string(bytes) != yaml_text {
		t.Fatalf("Expected %s, got %s", yaml_text, string(bytes))
	}
}

func TestSpicepod_MarshalReferenceAndDataset(t *testing.T) {
	yaml_text := `version: v1beta1
kind: Spicepod
name: spice_app
datasets:
    - ref: eth_recent_blocks
    - from: datasets/uniswap_v2_eth_usdc
`

	spicePod := SpicepodSpec{
		Version: "v1beta1",
		Kind:    "Spicepod",
		Name:    "spice_app",
		Datasets: []Component{
			{
				Reference: Reference{
					Ref: "eth_recent_blocks",
				},
			},
			{
				Dataset: DatasetSpec{
					From: "datasets/uniswap_v2_eth_usdc",
				},
			},
		},
	}

	bytes, err := yaml.Marshal(spicePod)
	if err != nil {
		t.Fatalf("Failed to marshal yaml: %v", err)
	}

	if string(bytes) != yaml_text {
		t.Fatalf("Expected %s, got %s", yaml_text, string(bytes))
	}
}
