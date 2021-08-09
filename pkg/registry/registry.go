package registry

import (
	"strings"

	"github.com/spiceai/spice/pkg/spec"
)

type SpiceRegistry interface {
	GetPod(podPath string) (string, error)
	GetDataSource(datasourcePath string) (*spec.DataSourceSpec, error)
}

func GetRegistry(path string) SpiceRegistry {
	if strings.HasPrefix(path, "/") || strings.HasPrefix(path, "file://") {
		return &LocalFileRegistry{}
	}

	return &GitHubRegistry{}
}
