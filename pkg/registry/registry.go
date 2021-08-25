package registry

import (
	"strings"
)

type SpiceRegistry interface {
	GetPod(podPath string) (string, error)
}

func GetRegistry(path string) SpiceRegistry {
	if strings.HasPrefix(path, "/") || strings.HasPrefix(path, "file://") {
		return &LocalFileRegistry{}
	}

	return &SpiceRackRegistry{}
}
