package api

type Pod struct {
	Name         string   `json:"name,omitempty" csv:"name"`
	ManifestPath string   `json:"manifest_path,omitempty" csv:"manifest_path"`
	Episodes     int64    `json:"episodes,omitempty" csv:"episodes"`
	Identifiers  []string `json:"identifiers,omitempty" csv:"-"`
	Measurements []string `json:"measurements,omitempty" csv:"-"`
	Categories   []string `json:"categories,omitempty" csv:"-"`
}
