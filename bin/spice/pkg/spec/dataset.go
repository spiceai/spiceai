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

import "time"

const (
	REFRESH_MODE_FULL   = "full"
	REFRESH_MODE_APPEND = "append"

	DATA_SOURCE_SPICEAI    = "spice.ai"
	DATA_SOURCE_DREMIO     = "dremio"
	DATA_SOURCE_DATABRICKS = "databricks"
	DATA_SOURCE_S3         = "s3"
	DATA_SOURCE_FTP        = "ftp"
	DATA_SOURCE_SFTP       = "sftp"
)

type DatasetSpec struct {
	From         string            `json:"from,omitempty" csv:"from" yaml:"from,omitempty"`
	Name         string            `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Description  string            `json:"description,omitempty" csv:"description" yaml:"description,omitempty"`
	Params       map[string]string `json:"params,omitempty" csv:"params" yaml:"params,omitempty"`
	Acceleration *AccelerationSpec `json:"acceleration,omitempty" csv:"acceleration" yaml:"acceleration,omitempty"`
}

type AccelerationSpec struct {
	Enabled              bool              `json:"enabled,omitempty" csv:"enabled" yaml:"enabled,omitempty"`
	Mode                 string            `json:"mode,omitempty" csv:"mode" yaml:"mode,omitempty"`
	Engine               string            `json:"engine,omitempty" csv:"engine" yaml:"engine,omitempty"`
	RefreshMode          string            `json:"refresh_mode,omitempty" csv:"refresh_mode" yaml:"refresh_mode,omitempty"`
	RefreshCheckInterval time.Duration     `json:"refresh_check_interval,omitempty" csv:"refresh_check_interval" yaml:"refresh_check_interval,omitempty"`
	RefreshSql           string            `json:"refresh_sql,omitempty" csv:"refresh_sql" yaml:"refresh_sql,omitempty"`
	Retention            time.Duration     `json:"retention,omitempty" csv:"retention" yaml:"retention,omitempty"`
	Params               map[string]string `json:"params,omitempty" csv:"params" yaml:"params,omitempty"`
}
