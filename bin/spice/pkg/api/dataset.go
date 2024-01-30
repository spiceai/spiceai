package api

import "time"

const (
	DATA_SOURCE_SPICE_AI  = "spice.ai"
	DATA_SOURCE_SPICE_OSS = "spice-oss"
	DATA_SOURCE_DREMIO    = "dremio"

	DATASET_TYPE_OVERWRITE = "overwrite"
	DATASET_TYPE_APPEND    = "append"
)

var (
	DATA_SOURCES = []string{
		DATA_SOURCE_SPICE_AI,
		DATA_SOURCE_SPICE_OSS,
		DATA_SOURCE_DREMIO,
	}
)

type Dataset struct {
	Name         string        `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Type         string        `json:"type,omitempty" csv:"type" yaml:"type,omitempty"`
	Source       string        `json:"source,omitempty" csv:"source" yaml:"source,omitempty"`
	Acceleration *Acceleration `json:"acceleration,omitempty" csv:"acceleration" yaml:"acceleration,omitempty"`
}

type Acceleration struct {
	Enabled bool          `json:"enabled,omitempty" csv:"enabled" yaml:"enabled,omitempty"`
	Refresh time.Duration `json:"refresh,omitempty" csv:"refresh" yaml:"refresh,omitempty"`
}

func DataSourceToHumanReadable(source string) string {
	switch source {
	case DATA_SOURCE_SPICE_AI:
		return "Spice AI Platform (https://spice.ai)"
	case DATA_SOURCE_SPICE_OSS:
		return "Another Spice.ai OSS instance"
	case DATA_SOURCE_DREMIO:
		return "Dremio (https://www.dremio.com)"
	default:
		return source
	}
}
