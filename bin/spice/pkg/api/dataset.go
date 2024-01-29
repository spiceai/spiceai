package api

import "time"

const (
	DATA_SOURCE_SPICE_AI   = "spice.ai"
	DATA_SOURCE_SPICE_OSS  = "spice-oss"
	DATA_SOURCE_DATABRICKS = "databricks"

	DATASET_TYPE_OVERWRITE = "overwrite"
	DATASET_TYPE_APPEND    = "append"
)

var (
	DATA_SOURCES = []string{
		DATA_SOURCE_SPICE_AI,
		DATA_SOURCE_SPICE_OSS,
		DATA_SOURCE_DATABRICKS,
	}
)

type Dataset struct {
	Name         string        `json:"name,omitempty" csv:"name"`
	Type         string        `json:"type,omitempty" csv:"type"`
	Source       string        `json:"source,omitempty" csv:"source"`
	Acceleration *Acceleration `json:"acceleration,omitempty" csv:"acceleration"`
}

type Acceleration struct {
	Enabled bool          `json:"enabled,omitempty" csv:"enabled"`
	Refresh time.Duration `json:"refresh,omitempty" csv:"refresh"`
}

func DataSourceToHumanReadable(source string) string {
	switch source {
	case DATA_SOURCE_SPICE_AI:
		return "Spice AI Platform (https://spice.ai)"
	case DATA_SOURCE_SPICE_OSS:
		return "Another Spice.ai OSS instance"
	case DATA_SOURCE_DATABRICKS:
		return "Databricks (https://databricks.com)"
	default:
		return source
	}
}
