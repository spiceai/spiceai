package api

type Pod struct {
	Name         string `json:"name,omitempty" csv:"name"`
	ManifestPath string `json:"manifest_path,omitempty" csv:"manifest_path"`
}
