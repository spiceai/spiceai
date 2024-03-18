package api

type Dataset struct {
	From                string `json:"from,omitempty" csv:"from" yaml:"from,omitempty"`
	Name                string `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	ReplicationEnabled  bool   `json:"replication_enabled,omitempty" csv:"replication_enabled" yaml:"replication_enabled,omitempty"`
	AccelerationEnabled bool   `json:"acceleration_enabled,omitempty" csv:"acceleration_enabled" yaml:"acceleration_enabled,omitempty"`
	DependsOn           string `json:"depends_on,omitempty" csv:"depends_on" yaml:"depends_on,omitempty"`
}
