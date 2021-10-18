package registry

import (
	"os"
	"strings"
)

type SpiceRegistry interface {
	GetPod(podPath string) (string, error)
}

func GetRegistry(path string) SpiceRegistry {
	if strings.HasPrefix(path, "/") || strings.HasPrefix(path, "../") || strings.HasPrefix(path, "file://") {
		return &LocalFileRegistry{}
	}

	if _, err := os.Stat(path); err == nil {
		return &LocalFileRegistry{}
	}

	return &SpiceRackRegistry{}
}
