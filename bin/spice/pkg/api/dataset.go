package api

import "time"

const (
	DATASOURCE_SPICE_AI   = "spice.ai"
	DATASOURCE_SPICE_OSS  = "spice-oss"
	DATASOURCE_DATABRICKS = "databricks"

	DATASET_TYPE_OVERWRITE = "overwrite"
	DATASET_TYPE_APPEND    = "append"
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
	case DATASOURCE_SPICE_AI:
		return "Spice AI Platform (https://spice.ai)"
	case DATASOURCE_SPICE_OSS:
		return "Another Spice.ai OSS instance"
	case DATASOURCE_DATABRICKS:
		return "Databricks (https://databricks.com)"
	default:
		return source
	}
}
